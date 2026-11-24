import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterable, List, Literal, Optional

from vulquantafs.core.model import TGFSDirectory, TGFSFileRef, TGFSMetadata
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.errors import FileOrDirectoryDoesNotExist, TechnicalError

from .removal_journal import RemovalJournal

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


class MetaDataApi:
    def __init__(
        self,
        metadata_repo: IMetaDataRepository,
        push_mode: MetadataPushMode = "inline",
        push_window: float = DEFAULT_PUSH_WINDOW_SECONDS,
        removal_journal: RemovalJournal | None = None,
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

    async def init(self) -> None:
        await self.__metadata_repo.init()
        if self.__removals is None:
            return
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
