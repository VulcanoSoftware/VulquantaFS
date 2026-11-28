from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, List, Optional, Sequence

from vulquantafs.core.model import (
    TGFSDirectory,
    TGFSFileDesc,
    TGFSFileRef,
    TGFSFileVersion,
    TGFSMetadata,
)
from vulquantafs.errors import MetadataNotInitialized
from vulquantafs.reqres import FileContent, SentFileMessage, UploadableFileMessage


@dataclass
class FDRepositoryResp:
    message_id: int
    fd: TGFSFileDesc
    # Mirror store key -> message id of the FD copy in that store.
    # Empty when redundancy is off.
    mirrors: Dict[str, int] = field(default_factory=dict)
    # Key of the store ``message_id`` was written to.
    store: Optional[str] = None


class IFileContentRepository(metaclass=ABCMeta):
    @abstractmethod
    async def save(self, file_msg: UploadableFileMessage) -> List[SentFileMessage]:
        pass

    def storage_name(self, name: str) -> str:
        """Name used on remote documents, including during replication."""
        return name

    @abstractmethod
    async def get(
        self,
        fv: TGFSFileVersion,
        begin: int,
        end: int,
        name: str,
        *,
        admit: bool = True,
    ) -> FileContent:
        """Bytes ``[begin, end]`` of the version, both ends inclusive.

        ``admit=False`` asks for the range without letting it open a new
        read-cache entry. An internal probe reads a handful of bytes and
        abandons the stream; admitting it would reserve the whole version
        and leave an entry behind that never receives a block. An entry
        that already exists is still read from and filled.
        """
        pass

    @abstractmethod
    async def update(self, message_id: int, buffer: bytes, name: str) -> int:
        pass

    async def stage(
        self, file_msg: UploadableFileMessage, version_id: str
    ) -> Optional[int]:
        """Take the whole upload into the local cache without touching a
        store (``write_ack: cache``); returns the bytes on disk, or
        ``None`` when the cache cannot take it and the caller has to
        upload the way it always did. Repositories without a cache
        return ``None``.
        """
        return None

    async def forget(self, version_ids: Iterable[str]) -> None:
        """The versions were deleted: drop whatever the repository kept
        for them locally (cache entries). Nothing to do by default."""
        return

    async def content_length(self, fv: TGFSFileVersion) -> int:
        """Logical size of the file as seen by the caller.

        Defaults to the stored on-wire size. The encryption decorator overrides
        this to subtract its per-chunk overhead and the file header so WebDAV
        clients see the plaintext length.
        """
        return fv.size


class IFDRepository(metaclass=ABCMeta):
    @abstractmethod
    async def save(
        self, fd: TGFSFileDesc, fr: Optional[TGFSFileRef] = None
    ) -> FDRepositoryResp:
        pass

    @abstractmethod
    async def get(
        self,
        fr: TGFSFileRef,
        include_all_versions: bool = False,
        validate: bool = True,
    ) -> TGFSFileDesc:
        """The descriptor of ``fr``. ``validate=False`` skips the check that
        its content messages still exist and trusts the recorded sizes."""
        pass


class IMetaDataRepository(metaclass=ABCMeta):
    batch_mirror_writes: bool = False

    def __init__(self):
        self.metadata: Optional[TGFSMetadata] = None
        # Whether the loaded tree carries stats; worked out on first ask.
        self._tree_has_stats: Optional[bool] = None

    async def init(self):
        self.metadata = await self.get()

    def bind_shared_tree(self, metadata: Optional[TGFSMetadata]) -> None:
        """Use a shared tree without installing store-specific write hooks."""
        self.metadata = metadata

    def export_for_primary(self, metadata: TGFSMetadata) -> TGFSMetadata:
        """Return a tree whose mutations aren't tied to this mirror."""
        return metadata

    def write_in_cooldown(self) -> bool:
        return False

    @property
    def supports_upload_journal(self) -> bool:
        return False

    async def push_upload_refs(
        self, refs: Sequence[TGFSFileRef], *, full: bool = False
    ) -> None:
        await self.push_full()

    @abstractmethod
    async def push(self) -> None:
        pass

    async def push_full(self) -> None:
        """Write the whole current tree to this store, however much of it
        is already there.

        For a blob store this is what ``push`` already does. For a store
        that writes incrementally (``github_repo``) it re-syncs everything
        the incremental writes missed -- which is how a store that was
        unreachable for a while gets healed. The replicated layer uses it
        for mirrors and for healing; the default delegates to ``push``.
        """
        await self.push()

    async def push_removals(self) -> None:
        """Persist journaled removals, which bypass incremental writes."""
        await self.push_full()

    async def store_replication_backup(self, data: bytes) -> bool:
        """Keep one snapshot of a replication queue as a backup; the local
        file stays the source of truth. ``False`` when this store does
        not keep one."""
        return False

    async def load_replication_backup(self) -> Optional[bytes]:
        """The stored queue snapshot, if any; ``None`` when there is none."""
        return None

    @abstractmethod
    async def get(self) -> TGFSMetadata:
        pass

    async def head_revision(self) -> Optional[float]:
        """The revision the store would date its tree with, read cheaply.

        A git store answers with its branch head's commit date -- two
        small requests -- so the replicated layer can decide which store
        holds the freshest tree *before* reading any of them fully (a
        grown tree costs hundreds of listing requests per store). ``None``
        means the store cannot tell cheaply and must be read to compare;
        it raises when the store could not be asked at all.
        """
        return None

    async def heal(self) -> None:
        """Re-sync every store this repository knows to be behind.

        Only the replicated layer has several stores to reconcile; the
        default has nothing to do.
        """
        return

    async def maintain(self) -> None:
        """Run the store's periodic housekeeping (commit-history compaction,
        deferred write flushes) once.

        Configured per store (e.g. ``squash_history_max_commits``); a store
        with no maintenance configured does nothing. Called next to
        :meth:`heal` by the heal worker, so it also runs on a store
        without mirrors.
        """
        return

    def stale_stores(self) -> List[str]:
        """Keys of the stores that missed a write and await healing."""
        return []

    async def close(self) -> None:
        """Release backend resources; the default has nothing to release."""
        return

    def root(self) -> TGFSDirectory:
        if not self.metadata:
            raise MetadataNotInitialized
        return self.metadata.dir

    # -- file stats (``listing.stat_in_metadata``) -------------------------
    #
    # The defaults fit a backend that serializes the whole tree on every
    # push: the stats ride along on the file refs. A backend that keeps
    # them apart from the tree overrides all of this.

    # Whether ``push`` writes the stats of every file ref.
    push_writes_stats: bool = True

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        """Put the stored stats of ``directory``'s files on their refs.

        Returns False when they could not be read; the caller must then
        not write that directory's stats, or it would drop the ones it
        could not see.
        """
        return True

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        """Whether the store may hold stats for files in ``directory``."""
        if self._tree_has_stats is None:
            self._tree_has_stats = any(
                fr.stat is not None for fr in _iter_file_refs(self.root())
            )
        return self._tree_has_stats

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        """Write the stats of ``directories``; ``keep=False`` removes every
        stored stat instead (``stat_in_metadata`` turned off)."""
        if not keep:
            for fr in _iter_file_refs(self.root()):
                fr.stat = None
            self._tree_has_stats = False
        await self.push()


def _iter_file_refs(directory: TGFSDirectory) -> Iterator[TGFSFileRef]:
    yield from directory.find_files()
    for child in directory.find_dirs():
        yield from _iter_file_refs(child)
