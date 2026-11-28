import asyncio
import logging
from contextlib import asynccontextmanager
from itertools import islice
from typing import AsyncIterator, Iterable, List, Literal, Optional

from vulquantafs.core.model import TGFSDirectory, TGFSFileRef, TGFSMetadata
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.errors import FileOrDirectoryDoesNotExist, TechnicalError

from .removal_journal import RemovalJournal
from .upload_journal import UploadJournal

logger = logging.getLogger(__name__)

# ``inline``: every push hands the tree straight to the stores and the
# caller waits for it, failures included. ``coalesced``: pushes that land
# within a window of each other are merged into one store write, so a
# burst of operations (a folder deleted one file at a time over SFTP,
# FileZilla hammering with parallel connections) pays one mirror sync
# instead of one per operation. A coalesced push failure is logged and
# only raised to callers that explicitly require persistence.
MetadataPushMode = Literal["inline", "coalesced"]

DEFAULT_PUSH_WINDOW_SECONDS = 0.3
REMOVE_FLUSH_INTERVAL = 2.0
REMOVE_RETRY_INTERVAL = 30.0
REMOVE_CLOSE_TIMEOUT = 5.0
UPLOAD_BATCH_WINDOW_SECONDS = 0.3
UPLOAD_BATCH_SIZE = 256
UPLOAD_MAX_RETRY_SECONDS = 60.0


class MetaDataApi:
    def __init__(
        self,
        metadata_repo: IMetaDataRepository,
        push_mode: MetadataPushMode = "inline",
        push_window: float = DEFAULT_PUSH_WINDOW_SECONDS,
        removal_journal: RemovalJournal | None = None,
        upload_journal: UploadJournal | None = None,
    ):
        self.__metadata_repo = metadata_repo
        self.__batch_depth = 0
        self.__pending = False
        self.__push_mode: MetadataPushMode = push_mode
        self.__push_window = max(push_window, 0.0)
        # The coalescer bookkeeping: ``needed`` counts the push requests the
        # coalescer took on, ``pushed`` the number the last completed push
        # covered. A caller waits until ``pushed`` reaches its own number,
        # so it is released by the first push that carries its change.
        self.__needed = 0
        self.__pushed = 0
        self.__wake = asyncio.Event()
        self.__condition = asyncio.Condition()
        self.__required: dict[int, asyncio.Future[None]] = {}
        self.__pusher: Optional[asyncio.Task] = None
        self.__write_lock = asyncio.Lock()
        self.__write_needed = 0
        self.__write_pushed = 0
        self.__write_worker: Optional[asyncio.Task] = None
        self.__removals = removal_journal
        self.__remove_checkpoint = 0
        self.__remove_worker: asyncio.Task[None] | None = None
        self.__remove_closing = False
        self.__uploads = upload_journal
        self.__pending_uploads: dict[int, TGFSFileRef] = {}
        self.__upload_full_sync = False
        self.__upload_lock = asyncio.Lock()
        self.__upload_worker: asyncio.Task[None] | None = None

    async def init(self) -> None:
        await self.__metadata_repo.init()
        if self.__removals is not None:
            self.__remove_checkpoint, fingerprints = self.__removals.load()
            if fingerprints:
                pending = [self.get_root_directory()]
                while pending:
                    directory = pending.pop()
                    pending.extend(directory.find_dirs())
                    for ref in tuple(directory.find_files()):
                        if self.__removals.fingerprint(ref) in fingerprints:
                            directory.unlink_file_ref(ref)
                self.__schedule_removal_flush()
        self.__restore_uploads()

    def __restore_uploads(self) -> None:
        if self.__uploads is None:
            return
        for entry in self.__uploads.load():
            directory = self.get_root_directory()
            for part in filter(None, entry.parent.split("/")):
                try:
                    directory = directory.find_dir(part)
                except FileOrDirectoryDoesNotExist:
                    directory = directory.restore_dir(part)
            found = directory.find_files([entry.name])
            if found and found[0].message_id == entry.message_id:
                ref = found[0]
            else:
                if found:
                    # Prefer a locally acknowledged upload over a stale
                    # failover copy. A full reconciliation removes the
                    # old descriptor path, not just adds the new one.
                    self.__upload_full_sync = True
                    directory.unlink_file_ref(found[0])
                ref = directory.attach_file_ref(entry.name, entry.message_id)
            ref.mirrors = entry.mirrors
            ref.store = entry.store
            self.__pending_uploads[entry.id] = ref
        if self.__pending_uploads:
            self.__schedule_upload_flush()

    def stage_upload(
        self,
        directory: TGFSDirectory,
        name: str,
        message_id: int,
        mirrors: dict[str, int],
        store: str | None,
    ) -> TGFSFileRef | None:
        """Durably acknowledge an already-stored upload, without remote I/O."""
        if self.__uploads is None or self.__remove_closing:
            return None
        node = directory
        while node.parent is not None:
            if not any(child is node for child in node.parent.find_dirs([node.name])):
                raise FileOrDirectoryDoesNotExist(directory.absolute_path)
            node = node.parent
        if node is not self.get_root_directory():
            raise FileOrDirectoryDoesNotExist(directory.absolute_path)
        ref = directory.attach_file_ref(name, message_id)
        ref.mirrors = dict(mirrors)
        ref.store = store
        try:
            id_ = self.__uploads.add(ref)
        except BaseException:
            directory.unlink_file_ref(ref)
            raise
        directory._touch_modified()
        self.__pending_uploads[id_] = ref
        self.__schedule_upload_flush()
        return ref

    def __schedule_upload_flush(self) -> None:
        if self.__upload_worker is None or self.__upload_worker.done():
            self.__upload_worker = asyncio.create_task(
                self.__drain_uploads(), name="metadata-upload-journal"
            )

    async def __drain_uploads(self) -> None:
        retry = 1.0
        while self.__pending_uploads and not self.__remove_closing:
            if len(self.__pending_uploads) < UPLOAD_BATCH_SIZE:
                await asyncio.sleep(UPLOAD_BATCH_WINDOW_SECONDS)
            try:
                await self.__flush_upload_batch()
                retry = 1.0
            except Exception as ex:
                logger.warning(
                    "Acknowledged uploads remain in the local journal; "
                    "retrying in %.0fs: %s",
                    retry,
                    ex,
                )
                await asyncio.sleep(retry)
                retry = min(retry * 2, UPLOAD_MAX_RETRY_SECONDS)

    async def __flush_upload_batch(
        self,
        *,
        ref: TGFSFileRef | None = None,
        directory: TGFSDirectory | None = None,
    ) -> bool:
        async with self.__upload_lock:
            if not self.__pending_uploads or self.__uploads is None:
                return False
            entries: Iterable[tuple[int, TGFSFileRef]] = self.__pending_uploads.items()
            if ref is not None or directory is not None:
                entries = (
                    (id_, pending)
                    for id_, pending in entries
                    if pending is ref
                    or (
                        directory is not None
                        and directory.is_ancestor_of(pending.location)
                    )
                )
            batch = dict(islice(entries, UPLOAD_BATCH_SIZE))
            if not batch:
                return False
            await self.__metadata_repo.push_upload_refs(
                list(batch.values()), full=self.__upload_full_sync
            )
            self.__upload_full_sync = False
            self.__uploads.clear(batch)
            for id_ in batch:
                del self.__pending_uploads[id_]
            self.request_push()
            return True

    async def flush_uploads(
        self,
        *,
        ref: TGFSFileRef | None = None,
        directory: TGFSDirectory | None = None,
    ) -> None:
        """Order a conflicting mutation after the journaled create."""
        # No matching references means nothing to wait for, including when
        # an unrelated batch is already holding the journal lock.
        while any(
            pending is ref
            or (directory is not None and directory.is_ancestor_of(pending.location))
            for pending in self.__pending_uploads.values()
        ):
            if not await self.__flush_upload_batch(ref=ref, directory=directory):
                return

    def defer_removal(self, ref: TGFSFileRef) -> bool:
        """Record the unlink durably before replying, without remote I/O."""
        if (
            self.__removals is None
            or self.__push_mode != "coalesced"
            or self.__remove_closing
        ):
            return False
        if not any(
            candidate is ref for candidate in ref.location.find_files([ref.name])
        ):
            raise FileOrDirectoryDoesNotExist(ref.name)
        self.__remove_checkpoint = self.__removals.add(ref)
        ref.location.unlink_file_ref(ref)
        self.__schedule_removal_flush()
        return True

    def __schedule_removal_flush(self) -> None:
        if self.__remove_worker is None or self.__remove_worker.done():
            self.__remove_worker = asyncio.create_task(
                self.__flush_removals_in_background(), name="metadata-removals"
            )

    async def __flush_removals_in_background(self) -> None:
        delay = REMOVE_FLUSH_INTERVAL
        while self.__remove_checkpoint and not self.__remove_closing:
            await asyncio.sleep(delay)
            try:
                await self.__flush_removal_batch()
                delay = REMOVE_FLUSH_INTERVAL
            except Exception as ex:
                logger.warning("Metadata removals remain in the local journal: %s", ex)
                delay = REMOVE_RETRY_INTERVAL

    async def __flush_removal_batch(self) -> None:
        async with self.__write_lock:
            checkpoint = self.__remove_checkpoint
            if not checkpoint or self.__removals is None:
                return
            await self.__metadata_repo.push_removals()
            self.__removals.clear_through(checkpoint)
            if self.__remove_checkpoint == checkpoint:
                self.__remove_checkpoint = 0

    async def flush_removals(self) -> None:
        # A directory move may copy its remote subtree verbatim. Commit
        # earlier unlinks first so it cannot resurrect them under a new path.
        while self.__remove_checkpoint:
            await self.__flush_removal_batch()

    async def __write(self) -> None:
        async with self.__write_lock:
            await self.__metadata_repo.push()

    def reset(self) -> None:
        self.__metadata_repo.metadata = TGFSMetadata(dir=TGFSDirectory.root_dir())

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[None]:
        """Collapse the pushes of a bulk operation into a single write.

        The pinned-message backend rewrites the whole metadata blob on
        every push, so copying a folder file by file would rewrite it once
        per file. Nesting is allowed; only the outermost block writes.
        The write also happens when the block fails, because whatever part
        of the operation did succeed is real and has to be recorded.
        """
        self.__batch_depth += 1
        try:
            yield
        finally:
            self.__batch_depth -= 1
            if self.__batch_depth == 0 and self.__pending:
                self.__pending = False
                # A batch spells out the operation that is about to end;
                # its write is immediate, not windowed, so the caller can
                # rely on the tree having landed when the block exits.
                await self.__write()

    async def store_replication_backup(self, data: bytes) -> bool:
        return await self.__metadata_repo.store_replication_backup(data)

    async def load_replication_backup(self) -> bytes | None:
        return await self.__metadata_repo.load_replication_backup()

    async def push(self, *, require_success: bool = False) -> None:
        if self.__batch_depth and not require_success:
            self.__pending = True
            return
        if self.__push_mode != "coalesced":
            await self.__write()
            return
        target = self.__needed + 1
        self.__needed = target
        required = None
        if require_success:
            required = asyncio.get_running_loop().create_future()
            self.__required[target] = required
        self.__ensure_pusher()
        self.__wake.set()
        if required is not None:
            try:
                await required
            finally:
                self.__required.pop(target, None)
            return
        # Wait for the first push that covers this request. Pushes after
        # the window is over carry the callers that registered too late for
        # the running one; no caller waits indefinitely, every push
        # releases the ones that precede it.
        async with self.__condition:
            while self.__pushed < target:
                await self.__condition.wait()

    def request_push(self) -> None:
        """Ask for a metadata write without waiting for it to land.

        For call sites whose reply must not pay for the push: the change
        is already real in the in-memory tree, and the write it needs is
        the same one every other caller of that window wants. Waiting
        here put an SFTP open behind mirror syncs and deferred-ref
        commits it did not depend on, stalling opens for whole seconds
        during bulk uploads.
        """
        if self.__batch_depth:
            self.__pending = True
            return
        if self.__push_mode != "coalesced":
            self.__schedule_write()
            return
        # The coalescer bookkeeping of ``push``, minus the wait: the
        # pusher writes on wake and the landing is nobody's reply.
        self.__needed += 1
        self.__ensure_pusher()
        self.__wake.set()

    def __schedule_write(self) -> None:
        # One worker at a time: a request that lands while a write is
        # running raises the counter, and the running worker loops back
        # for one more round instead of a second task racing the lock.
        self.__write_needed += 1
        if self.__write_worker is None or self.__write_worker.done():
            self.__write_worker = asyncio.get_running_loop().create_task(
                self.__write_logged()
            )

    async def __write_logged(self) -> None:
        while True:
            target = self.__write_needed
            try:
                await self.__write()
            except Exception as ex:
                logger.error(f"Pushing the metadata failed: {ex}")
            self.__write_pushed = max(self.__write_pushed, target)
            if self.__write_needed <= target:
                return
            await asyncio.sleep(0)

    def __ensure_pusher(self) -> None:
        if self.__pusher is None or self.__pusher.done():
            self.__pusher = asyncio.get_running_loop().create_task(self.__pusher_loop())

    async def __pusher_loop(self) -> None:
        try:
            while True:
                await self.__wake.wait()
                self.__wake.clear()
                if self.__push_window > 0:
                    await asyncio.sleep(self.__push_window)
                # The snapshot is the last request reachable when the
                # window closed; registers that arrive during the write
                # open the next window.
                snapshot = self.__needed
                await self.__finish_push(snapshot)
        except asyncio.CancelledError:
            raise

    async def __finish_push(self, snapshot: int) -> None:
        error = None
        try:
            await self.__write()
        except Exception as ex:
            error = ex
            logger.error("Pushing the metadata failed: %s", ex)
        self.__pushed = snapshot
        for target, future in self.__required.items():
            if target <= snapshot and not future.done():
                if error is None:
                    future.set_result(None)
                else:
                    future.set_exception(TechnicalError(str(error)))
        async with self.__condition:
            self.__condition.notify_all()

    def get_root_directory(self) -> TGFSDirectory:
        return self.__metadata_repo.root()

    # -- file stats (``listing.stat_in_metadata``) -------------------------

    @property
    def push_writes_stats(self) -> bool:
        return self.__metadata_repo.push_writes_stats

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        return await self.__metadata_repo.load_stats(directory)

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        return self.__metadata_repo.stats_stored(directory)

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        async with self.__write_lock:
            await self.__metadata_repo.save_stats(directories, keep)

    # -- metadata redundancy -------------------------------------------------

    async def heal(self) -> None:
        """Re-write the metadata stores that missed a write, then run the
        stores' configured maintenance (e.g. commit-history compaction)
        next to it.

        The heal worker calls this on a fixed interval, with one store or
        with several; a store that is up to date and has no maintenance
        configured costs a pass that does nothing. No global write lock
        here: the stores order themselves, and a slow re-sync of one
        store must not make every concurrent push wait behind it (a File
        Zilla gives up after 20 s of nothing).
        """
        await self.__metadata_repo.heal()
        try:
            await self.__metadata_repo.maintain()
        except Exception as ex:
            logger.warning(f"Metadata maintenance failed: {ex}")

    def stale_stores(self) -> List[str]:
        return self.__metadata_repo.stale_stores()

    async def close(self) -> None:
        self.__remove_closing = True
        upload_worker = self.__upload_worker
        self.__upload_worker = None
        if upload_worker is not None:
            upload_worker.cancel()
            await asyncio.gather(upload_worker, return_exceptions=True)
        if self.__uploads is not None:
            # Unconfirmed entries must survive shutdown and be replayed
            # even when the remote store is still unreachable.
            self.__uploads.close()
        pusher = self.__pusher
        self.__pusher = None
        if pusher is not None and not pusher.done():
            pusher.cancel()
            try:
                await pusher
            except asyncio.CancelledError:
                pass
        worker = self.__write_worker
        self.__write_worker = None
        if worker is not None and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        # A background write that a close caught up with is still real.
        if self.__write_needed > self.__write_pushed:
            self.__write_pushed = self.__write_needed
            try:
                await self.__write()
            except Exception as ex:
                logger.error(f"Pushing the metadata while closing failed: {ex}")
        # Flush a coalesced push whose window has not closed yet: the
        # operations that registered ahead of the close are real and have
        # to be recorded, whatever they are still worth.
        if self.__needed > self.__pushed:
            await self.__finish_push(self.__needed)
        async with self.__condition:
            self.__condition.notify_all()
        worker = self.__remove_worker
        if worker is not None and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        try:
            if self.__remove_checkpoint:
                try:
                    await asyncio.wait_for(
                        self.flush_removals(), timeout=REMOVE_CLOSE_TIMEOUT
                    )
                except Exception as ex:
                    logger.warning(
                        "Metadata removals will be replayed at next startup: %s", ex
                    )
        finally:
            if self.__removals is not None:
                self.__removals.close()
            await self.__metadata_repo.close()
