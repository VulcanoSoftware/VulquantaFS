import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterable, List, Literal, Optional

from tgdcfs.core.model import TGFSDirectory, TGFSMetadata
from tgdcfs.core.repository.interface import IMetaDataRepository

logger = logging.getLogger(__name__)

# ``inline``: every push hands the tree straight to the stores and the
# caller waits for it, failures included. ``coalesced``: pushes that land
# within a window of each other are merged into one store write, so a
# burst of operations (a folder deleted one file at a time over SFTP,
# FileZilla hammering with parallel connections) pays one mirror sync
# instead of one per operation. A coalesced push failure is logged, not
# raised: like the replicated layer's mirror failures, the operation that
# lost its push keeps its local changes, and the next window's push or a
# heal carries the tree over.
MetadataPushMode = Literal["inline", "coalesced"]

DEFAULT_PUSH_WINDOW_SECONDS = 0.3


class MetaDataApi:
    def __init__(
        self,
        metadata_repo: IMetaDataRepository,
        push_mode: MetadataPushMode = "inline",
        push_window: float = DEFAULT_PUSH_WINDOW_SECONDS,
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
        self.__pusher: Optional[asyncio.Task] = None

    async def init(self) -> None:
        await self.__metadata_repo.init()

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
                await self.__metadata_repo.push()

    async def push(self) -> None:
        if self.__batch_depth:
            self.__pending = True
            return
        if self.__push_mode != "coalesced":
            await self.__metadata_repo.push()
            return
        target = self.__needed + 1
        self.__needed = target
        self.__ensure_pusher()
        self.__wake.set()
        # Wait for the first push that covers this request. Pushes after
        # the window is over carry the callers that registered too late for
        # the running one; no caller waits indefinitely, every push
        # releases the ones that precede it.
        async with self.__condition:
            while self.__pushed < target:
                await self.__condition.wait()

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
                try:
                    await self.__metadata_repo.push()
                except Exception as ex:
                    logger.error(f"Pushing the metadata failed: {ex}")
                self.__pushed = snapshot
                async with self.__condition:
                    self.__condition.notify_all()
        except asyncio.CancelledError:
            raise

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
        await self.__metadata_repo.save_stats(directories, keep)

    # -- metadata redundancy -------------------------------------------------

    async def heal(self) -> None:
        """Re-write the metadata stores that missed a write; a no-op while
        every store is up to date."""
        await self.__metadata_repo.heal()

    def stale_stores(self) -> List[str]:
        return self.__metadata_repo.stale_stores()

    async def close(self) -> None:
        pusher = self.__pusher
        self.__pusher = None
        if pusher is not None and not pusher.done():
            pusher.cancel()
            try:
                await pusher
            except asyncio.CancelledError:
                pass
        # Flush a coalesced push whose window has not closed yet: the
        # operations that registered ahead of the close are real and have
        # to be recorded, whatever they are still worth.
        if self.__needed > self.__pushed:
            try:
                await self.__metadata_repo.push()
            except Exception as ex:
                logger.error(f"Pushing the metadata while closing failed: {ex}")
            self.__pushed = self.__needed
        async with self.__condition:
            self.__condition.notify_all()
        await self.__metadata_repo.close()
