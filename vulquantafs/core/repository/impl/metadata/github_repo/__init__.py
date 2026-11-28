import asyncio
import base64
import bisect
import datetime
import hashlib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, NamedTuple, Optional, Sequence, Set, cast

from github import Github, GithubException, InputGitTreeElement
from github.ContentFile import ContentFile
from github.GitCommit import GitCommit
from github.Repository import Repository
from urllib3.util import Retry

from vulquantafs.config import GithubRepoConfig, expand_path
from vulquantafs.core.model import FileStat, TGFSDirectory, TGFSFileRef, TGFSMetadata
from vulquantafs.core.repository.impl.metadata.breaker import (
    FailFastRepository,
    StoreCircuitBreaker,
    probe_github_branch,
)
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.crypto.path_names import (
    PathNameEncryptionError,
    decrypt_path_name,
    encrypt_path_name,
    is_encrypted_path_name,
)
from vulquantafs.errors import MetadataNotInitialized, NoStoredMetadata, TechnicalError

from .dir_timestamps import DirTimestampStore, cache_file_name
from .gh_directory import (
    DROP_COMMIT_ATTEMPTS,
    DROP_FLUSH_BATCH,
    DROP_FLUSH_INTERVAL,
    TREE_OVERLAY_ACTION_LIMIT,
    GithubConfig,
    GithubDirectory,
    _PendingRefUpdate,
    _put_file_refs,
    commit_tree_overlay,
    file_ref_segment,
    parse_ref_segment,
)
from .stats_file import STATS_COMMIT_PREFIX, STATS_FILE_NAME
from .tree_snapshot import TreeSnapshot, snapshot_file_name

logger = logging.getLogger(__name__)


class _TreeEntry(NamedTuple):
    """The part of a git tree entry the tree builder reads: a full storage
    path, ``"tree"`` or ``"blob"``, the object's sha and its file mode."""

    path: str
    type: str
    sha: str
    mode: str = "100644"


class _HeadCommit(NamedTuple):
    """The branch head's sha with the Unix epoch of its commit date."""

    sha: str
    epoch: float


# How often a stats commit is retried when the branch moved under it.
STATS_COMMIT_ATTEMPTS = 3
# How often the full-tree sync commit is retried when the branch moved.
SYNC_COMMIT_ATTEMPTS = 3
GITHUB_PAGE_SIZE = 100

# Root-path blob that carries the replication queue's remote snapshot.
# ``_is_managed_blob`` does not claim it (the segment parses as neither a
# reference nor ``.gitkeep``), so a full sync leaves it alone; every
# backup simply replaces it.
REPLICATION_BACKUP_PATH = "replication-queue-backup.b64"


def _blob_sha(content: str) -> str:
    """The git blob sha of ``content``, as GitHub will report it."""
    data = content.encode()
    # Git's object id, not a security measure.
    return hashlib.sha1(
        b"blob %d\0" % len(data) + data, usedforsecurity=False
    ).hexdigest()


class GithubRepoMetadataRepository(IMetaDataRepository):
    supports_upload_journal = True

    def __init__(
        self,
        config: GithubRepoConfig,
        name_key: Optional[bytes] = None,
        repo: Optional[Repository] = None,
    ):
        super().__init__()
        self._stats_loads: dict[int, asyncio.Future[bool]] = {}

        # Keep bounded connection retries, but don't sleep through GitHub
        # rate limits on the single metadata writer or the SFTP event loop.
        gh = Github(
            config.access_token,
            per_page=GITHUB_PAGE_SIZE,
            retry=Retry(total=1, status=0, respect_retry_after_header=False),
        )
        # Injectable for a store that speaks the PyGithub surface without
        # being GitHub (the GitLab tree mirror); the default client talks
        # to GitHub itself. The lazy flag keeps the resolution request out
        # of the constructor: the repository completes on its first API
        # call, which runs on a worker thread. An unreachable GitHub at
        # boot must surface as this store's read error -- so the
        # replicated layer fails over to a healthy mirror and the heal
        # worker catches this store up once it answers again -- never as a
        # minutes-long blocking call on the event loop before the layer
        # even exists. The fail-fast wrapper keeps it that way afterwards:
        # once a call ran into a dead connection, every further call of
        # this store is refused until a background probe finds the
        # service again, so one outage costs its connect timeouts once --
        # not once per listing, write, stats commit and heal.
        self._breaker: Optional[StoreCircuitBreaker] = None
        if repo is None:
            raw = gh.get_repo(config.repo, lazy=True)
            self._breaker = StoreCircuitBreaker(probe_github_branch(raw, config.commit))
            repo = cast(Repository, FailFastRepository(raw, self._breaker))

        self._timestamps = DirTimestampStore(
            repo=repo,
            branch=config.commit,
            path=expand_path(cache_file_name(config.repo, config.commit)),
        )
        self._snapshot = TreeSnapshot(
            expand_path(snapshot_file_name(config.repo, config.commit))
        )

        self._ghc = GithubConfig(
            gh=gh,
            repo_name=config.repo,
            repo=repo,
            commit=config.commit,
            name_key=name_key,
            timestamps=self._timestamps,
            tree_fetch_concurrency=config.tree_fetch_concurrency,
            squash_history_max_commits=config.squash_history_max_commits,
        )

    # -- the shared tree -----------------------------------------------------
    #
    # The replicated layer hands every store the same tree object, loaded
    # from whichever store could serve it. Mutations persist through the
    # ``GithubDirectory`` nodes (the incremental writes) and ``push_full``
    # diffs whatever tree it is given, so a tree that arrived as plain
    # ``TGFSDirectory`` nodes -- loaded from a pinned message or a document
    # store -- is converted in place: same object, store-backed nodes, with
    # the mirrors, store key, stat and dates of every file ref and folder
    # preserved.

    @property
    def metadata(self) -> Optional[TGFSMetadata]:
        return self._metadata

    @metadata.setter
    def metadata(self, value: Optional[TGFSMetadata]) -> None:
        self._metadata = None if value is None else self._as_github_tree(value)

    def bind_shared_tree(self, metadata: Optional[TGFSMetadata]) -> None:
        # Using the normal setter here turns a plain primary's nodes into
        # write-through nodes bound to the mirror, bypassing replication.
        self._metadata = metadata

    def export_for_primary(self, metadata: TGFSMetadata) -> TGFSMetadata:
        return TGFSMetadata.from_dict(metadata.to_dict())

    def _as_github_tree(self, metadata: TGFSMetadata) -> TGFSMetadata:
        if isinstance(metadata.dir, GithubDirectory):
            return metadata
        plain_root = metadata.dir
        root = GithubDirectory(
            self._ghc, name="root", parent=None, children=[], files=[]
        )
        root.created_at = plain_root.created_at
        root.modified_at = plain_root.modified_at
        self._convert_directory(plain_root, root)
        metadata.dir = root
        return metadata

    def _convert_directory(
        self, source: TGFSDirectory, target: GithubDirectory
    ) -> None:
        for child in source.find_dirs():
            # New nodes follow the configured name key, like freshly created
            # directories do; the plaintext names of a document store get
            # encrypted on the way out.
            node = GithubDirectory(
                self._ghc,
                name=child.name,
                parent=target,
                stored_encrypted=None,
                defer_timestamps=True,
            )
            target.children.append(node)
            node.created_at = child.created_at
            node.modified_at = child.modified_at
            self._convert_directory(child, node)
        for fr in source.find_files():
            # Built directly instead of via ``attach_file_ref``, which loses
            # the mirrors, store key and stat of a loaded ref.
            target.files.append(
                TGFSFileRef(
                    message_id=fr.message_id,
                    name=fr.name,
                    location=target,
                    mirrors=dict(fr.mirrors),
                    store=fr.store,
                    stat=fr.stat,
                    stat_epoch=fr.stat_epoch,
                )
            )

    async def push(self) -> None:
        if self.metadata is not None and not isinstance(
            self.metadata.dir, GithubDirectory
        ):
            # Booted on a fallback; the plain tree intentionally has no
            # write-through hooks to a possibly unreachable primary.
            await self.push_full()
            return
        await self._flush_ref_updates()
        # The incremental writes already landed while the tree was mutated;
        # what is left is the batched file-reference deletions.
        if not self._ghc.pending_drops:
            return
        now = time.monotonic()
        if (
            len(self._ghc.pending_drops) < DROP_FLUSH_BATCH
            and now - self._ghc.pending_drops_since < DROP_FLUSH_INTERVAL
        ):
            return
        async with self._ghc.lock:
            paths = sorted(self._ghc.pending_drops)
            self._ghc.pending_drops.clear()
            self._ghc.pending_drops_since = 0.0
            try:
                await self._ghc.write(self._commit_drops, paths)
            except GithubException as ex:
                if getattr(ex, "status", None) == 404:
                    # No branch yet: a fresh store holds no blobs.
                    return
                self._requeue_drops(paths)
            except Exception:
                self._requeue_drops(paths)

    def _requeue_drops(self, paths: list[str]) -> None:
        logger.exception(
            f"Committing the deletion of {len(paths)} file reference(s) "
            f"to {self._ghc.repo_name} failed; the deletes are queued for "
            "the next metadata push"
        )
        self._ghc.pending_drops.update(paths)
        if not self._ghc.pending_drops_since:
            self._ghc.pending_drops_since = time.monotonic()

    async def _flush_ref_updates(self) -> None:
        """Re-commit the blobs of refs whose stored state moved (see
        ``GithubDirectory.note_ref_update``): one overlay commit that
        creates the new segments and drops the superseded ones, so a
        restart can never read the outdated segment back."""
        if not self._ghc.pending_ref_updates:
            return
        async with self._ghc.lock:
            updates = self._ghc.pending_ref_updates
            self._ghc.pending_ref_updates = []
            # The tree is read on the event loop (it is mutated there); only
            # the commit runs on the writer thread.
            creates: Set[str] = set()
            drops: Set[str] = set()
            for update in updates:
                drops.add(update.committed_path)
                fr = update.file_ref
                if not any(ref is fr for ref in update.directory.find_files([fr.name])):
                    continue
                creates.add(update.directory._ref_path_of(fr))
            drops -= creates
            try:
                await self._ghc.write(
                    self._commit_ref_updates, sorted(creates), sorted(drops)
                )
            except GithubException as ex:
                if getattr(ex, "status", None) == 404:
                    # No branch yet: a fresh store holds no blobs; the first
                    # full sync creates them under their current segments.
                    return
                self._requeue_ref_updates(updates)
            except Exception:
                self._requeue_ref_updates(updates)

    def _requeue_ref_updates(self, updates: list[_PendingRefUpdate]) -> None:
        logger.exception(
            f"Committing {len(updates)} file reference update(s) to "
            f"{self._ghc.repo_name} failed; they are queued for the "
            "next metadata push"
        )
        self._ghc.pending_ref_updates.extend(updates)

    def _commit_ref_updates(self, creates: Sequence[str], drops: Sequence[str]) -> None:
        """Overlay the ref updates on the branch head in one commit.

        Blocking; raises when the commit does not land, and retried from
        the queued updates on the next metadata push.
        """
        overlay = [
            InputGitTreeElement(path, "100644", "blob", content="") for path in creates
        ]
        overlay += [
            InputGitTreeElement(path, "100644", "blob", sha=None) for path in drops
        ]
        if not overlay:
            return
        commit_tree_overlay(
            self._ghc.repo,
            self._ghc.commit,
            overlay,
            f"Update {len(creates)} file reference(s)",
        )
        logger.info(
            f"Updated {len(creates)} file reference(s) in {self._ghc.repo_name}"
        )

    def _commit_drops(self, paths: list[str]) -> None:
        """Overlay the pending deletions on the branch head in one commit.

        A tree element with neither content nor sha removes the path from
        the base tree, like the stats commit does; a path that was never
        there, or that a directory delete already removed, is a no-op.
        Blocking; raises when the commit does not land, and retried when
        the branch moved under it, like the stats commit.
        """
        repo = self._ghc.repo
        elements = [
            InputGitTreeElement(path=path, mode="100644", type="blob", sha=None)
            for path in paths
        ]
        for attempt in range(DROP_COMMIT_ATTEMPTS):
            ref = repo.get_git_ref(f"heads/{self._ghc.commit}")
            base_commit = repo.get_git_commit(ref.object.sha)
            tree = repo.create_git_tree(elements, base_tree=base_commit.tree)
            commit = repo.create_git_commit(
                f"Delete {len(paths)} file reference(s)", tree, [base_commit]
            )
            try:
                ref.edit(commit.sha)
                logger.info(
                    f"Deleted {len(paths)} file reference(s) from "
                    f"{self._ghc.repo_name}"
                )
                return
            except GithubException as ex:
                if attempt + 1 == DROP_COMMIT_ATTEMPTS:
                    raise
                logger.info(
                    f"Branch {self._ghc.commit} moved while committing the "
                    f"file reference deletions ({ex.status}), retrying"
                )

    # -- maintenance (``maintain``) -----------------------------------------
    #
    # Run next to ``heal`` by the heal worker: once per interval, with the
    # store configured for it or not. A git store written one blob at a
    # time accumulates exactly the history that keeps every deleted blob
    # alive; squashing it into one commit carrying the whole current
    # state is what makes the backend reclaim the old ones.

    async def maintain(self) -> None:
        if self._ghc.squash_history_max_commits <= 0:
            return
        # History inspection does not write. Do not queue it ahead of
        # deletes on the single writer thread.
        head = await asyncio.to_thread(self._compaction_candidate)
        if head is not None:
            async with self._ghc.lock:
                await self._ghc.write(self._compact_history, head)

    def _compaction_candidate(self) -> Optional[str]:
        """Check only the page that would contain the first excess commit."""
        repo = self._ghc.repo
        page, offset = divmod(self._ghc.squash_history_max_commits, GITHUB_PAGE_SIZE)
        try:
            head = repo.get_git_ref(f"heads/{self._ghc.commit}").object.sha
            commits = repo.get_commits(sha=head).get_page(page)
        except GithubException as ex:
            if ex.status in (404, 409):
                return None
            raise
        return head if len(commits) > offset else None

    def _compact_history(self, head: str) -> None:
        """Reuse the complete immutable tree of the checked branch head."""
        repo = self._ghc.repo
        ref_name = f"heads/{self._ghc.commit}"
        ref = repo.get_git_ref(ref_name)
        if ref.object.sha != head:
            return
        base_commit = repo.get_git_commit(head)
        commit = repo.create_git_commit(
            "Compact metadata history",
            base_commit.tree,
            [],
        )
        # Skip the rewrite if another writer already published a new head.
        # The ref API offers no compare-and-swap for this forced update.
        if repo.get_git_ref(ref_name).object.sha != head:
            return
        ref.edit(commit.sha, force=True)
        logger.info(
            f"Compacted the metadata history of {self._ghc.repo_name}@"
            f"{self._ghc.commit} into one commit"
        )

    # -- full-tree sync (``push_full``) -------------------------------------
    #
    # As the primary metadata store the repo is written incrementally while
    # the tree is mutated (``GithubDirectory``), so ``push`` has nothing to
    # do. As a redundant copy under the replicated metadata layer -- and as
    # the healing step after this store missed incremental writes -- the
    # whole in-memory tree is diffed against the repo and reconciled in one
    # commit.

    async def push_full(self) -> None:
        if not self.metadata:
            raise MetadataNotInitialized()
        # Under the write lock, like the incremental writes: a deferred file
        # reference create that is still retrying must not race the sync.
        # The tree is read on the event loop (it is mutated there); only the
        # GitHub round trips run on the writer thread.
        async with self._ghc.lock:
            desired_blobs = self._desired_blobs()
            await self._ghc.write(self._full_sync_writer(), desired_blobs)
            # The sync reconciled the whole tree; every pending blob delete
            # it covered is durable now -- and a tree a file reference was
            # dropped from wrote them all out.
            self._ghc.pending_drops.clear()
            self._ghc.pending_drops_since = 0.0

    def _full_sync_writer(self) -> Callable[[Set[str]], None]:
        return self._sync_tree_commit

    async def push_upload_refs(
        self, refs: Sequence[TGFSFileRef], *, full: bool = False
    ) -> None:
        root = self.root()
        if full or not isinstance(root, GithubDirectory):
            # A plain tree loaded from another replica has no write-through
            # paths. Replay conflicts also need to reconcile obsolete blobs.
            await self.push_full()
            return
        async with self._ghc.lock:
            # An overlay is safe to replay even if GitHub moved the branch
            # but its reply was lost. Only a confirmed ref edit clears the
            # outbox; do not use the legacy single-file 422 heuristic.
            await self._ghc.write(
                self._write_upload_refs,
                [
                    (
                        self._ref_blob_path(
                            ref.location, self._dir_storage_path(ref.location), ref
                        ),
                        ref.name,
                    )
                    for ref in refs
                ],
            )

    def _write_upload_refs(self, refs: Sequence[tuple[str, str]]) -> None:
        repo, branch = self._ghc.repo, self._ghc.commit
        try:
            _put_file_refs(repo, branch, refs)
        except GithubException as ex:
            if ex.status not in (404, 409):
                raise
            # The branch may not have been created yet (for example the
            # first root-level uploads to a new metadata repository).
            try:
                self._initial_sync_commit({path for path, _name in refs})
            except GithubException as initial:
                if initial.status != 409:
                    raise
                # GitHub's Git Data API cannot create a tree in a completely
                # empty repository. Bootstrap it through the Contents API.
                repo.create_file(
                    path=refs[0][0],
                    message="Create initial VulquantaFS file reference",
                    content="",
                    branch=branch,
                )
                if len(refs) > 1:
                    _put_file_refs(repo, branch, refs[1:])

    async def store_replication_backup(self, data: bytes) -> bool:
        """Commit one snapshot of the replication queue to the branch root.

        The payload is gzipped JSON travelling base64-encoded, since the
        tree API's elements carry text. Replaces the previous snapshot in
        place; without a branch yet there is nothing worth backing up."""
        overlay = [
            InputGitTreeElement(
                path=REPLICATION_BACKUP_PATH,
                mode="100644",
                type="blob",
                content=base64.b64encode(data).decode("ascii"),
            )
        ]
        try:
            async with self._ghc.lock:
                await self._ghc.write(
                    commit_tree_overlay,
                    self._ghc.repo,
                    self._ghc.commit,
                    overlay,
                    "Back up the replication queue",
                )
        except GithubException as ex:
            if getattr(ex, "status", None) == 404:
                return False
            raise
        return True

    async def load_replication_backup(self) -> Optional[bytes]:
        """The stored snapshot, or ``None`` when there is none."""
        repo, branch = self._ghc.repo, self._ghc.commit
        try:
            ref = repo.get_git_ref(f"heads/{branch}")
            commit = repo.get_git_commit(ref.object.sha)
            tree = repo.get_git_tree(commit.tree.sha, recursive=True)
            if tree.truncated:
                entries: Sequence[_TreeEntry] = self._walk_tree()
            else:
                entries = [
                    _TreeEntry(path=e.path, type=e.type, sha=e.sha, mode=e.mode)
                    for e in tree.tree
                ]
        except GithubException as ex:
            if getattr(ex, "status", None) == 404:
                return None
            raise
        entry = next(
            (
                e
                for e in entries
                if e.type == "blob" and e.path == REPLICATION_BACKUP_PATH
            ),
            None,
        )
        if entry is None:
            return None
        blob = repo.get_git_blob(entry.sha)
        # PyGithub hands out the blob's content base64-encoded; the raw
        # bytes are the base64 text the backup committed.
        return base64.b64decode(base64.b64decode(blob.content).decode("ascii"))

    def _desired_blobs(self) -> Set[str]:
        """The blob paths the in-memory tree implies."""
        if self.metadata is None:
            raise MetadataNotInitialized()
        metadata = self.metadata
        blob_paths: Set[str] = set()
        dir_paths: Set[str] = set()

        def walk(directory: TGFSDirectory) -> None:
            path = self._dir_storage_path(directory)
            dir_paths.add(path)
            for fr in directory.find_files():
                blob_paths.add(self._ref_blob_path(directory, path, fr))
            for child in directory.find_dirs():
                walk(child)

        walk(metadata.dir)
        # Git cannot represent an empty directory: every directory without
        # a blob beneath it gets a ``.gitkeep``, like ``create_dir`` makes.
        sorted_blobs = sorted(blob_paths)
        for path in dir_paths:
            if not path:
                continue
            marker = f"{path}/"
            i = bisect.bisect_left(sorted_blobs, marker)
            if i >= len(sorted_blobs) or not sorted_blobs[i].startswith(marker):
                blob_paths.add(f"{marker}.gitkeep")
        return blob_paths

    def _dir_storage_path(self, directory: TGFSDirectory) -> str:
        """On-repo path of ``directory``; per-directory encrypted state is
        preserved for a tree of ``GithubDirectory`` nodes, a plain tree
        (loaded from another metadata store) follows the configured key."""
        if isinstance(directory, GithubDirectory) and directory._ghc is self._ghc:
            return directory._github_path
        segments: list[str] = []
        node: Optional[TGFSDirectory] = directory
        while node is not None and node.parent is not None:
            segments.append(self._encode_segment(node.name))
            node = node.parent
        return GithubDirectory.join_path(*reversed(segments))

    def _encode_segment(self, name: str) -> str:
        key = self._ghc.name_key
        return encrypt_path_name(key, name) if key is not None else name

    def _ref_blob_path(
        self, directory: TGFSDirectory, path: str, fr: TGFSFileRef
    ) -> str:
        if isinstance(directory, GithubDirectory) and directory._ghc is self._ghc:
            segment = directory._file_ref_storage_name(
                fr.name, fr.message_id, fr.mirrors
            )
        else:
            segment = file_ref_segment(
                self._encode_segment(fr.name), fr.message_id, fr.mirrors
            )
        return GithubDirectory.join_path(path, segment)

    @staticmethod
    def _is_managed_blob(path: str) -> bool:
        """Whether the sync may delete this blob: file references (``name.id``
        -- the same shape the loader parses) and ``.gitkeep`` placeholders.
        Stats files have their own retention rule; foreign blobs are left alone."""
        segment = path.rsplit("/", 1)[-1]
        if segment == ".gitkeep":
            return True
        if segment == STATS_FILE_NAME:
            return False
        try:
            parse_ref_segment(segment)
        except ValueError:
            return False
        return True

    def _sync_tree_commit(self, desired_blobs: Set[str]) -> None:
        """Diff the repo against the desired blobs and commit the delta.

        Blocking; raises when the sync cannot be done safely. Retried when
        the branch moved under it, like the stats commit. A repo without
        the branch yet (a fresh mirror) gets an initial commit.
        """
        repo = self._ghc.repo
        for attempt in range(SYNC_COMMIT_ATTEMPTS):
            try:
                ref = repo.get_git_ref(f"heads/{self._ghc.commit}")
            except GithubException as ex:
                if getattr(ex, "status", None) != 404:
                    raise
                self._initial_sync_commit(desired_blobs)
                return
            base_commit = repo.get_git_commit(ref.object.sha)
            tree = repo.get_git_tree(base_commit.tree.sha, recursive=True)
            if tree.truncated:
                # A truncated listing would make the rewrite below drop
                # every blob it did not see. Repositories past GitHub's
                # truncation limit (100,000 entries or 7 MB) never grow
                # out of it, so refusing here kept a caught-up store
                # failing forever; the listing is rebuilt one directory
                # at a time instead, complete if slower. Only a walk
                # whose own directories truncate (an unreachable repo)
                # still refuses the sync.
                logger.warning(
                    f"Tree of {self._ghc.repo_name}@{self._ghc.commit} came "
                    "back truncated; reading it one directory at a time"
                )
                current = {
                    e.path: (e.sha, e.mode)
                    for e in self._walk_tree()
                    if e.type == "blob"
                }
            else:
                current = {
                    e.path: (e.sha, e.mode) for e in tree.tree if e.type == "blob"
                }
            creates = sorted(desired_blobs - set(current))
            desired_sorted = sorted(desired_blobs)

            def stale_stats(path: str) -> bool:
                parent, _, filename = path.rpartition("/")
                if not parent or filename != STATS_FILE_NAME:
                    return False
                prefix = parent + "/"
                index = bisect.bisect_left(desired_sorted, prefix)
                return index == len(desired_sorted) or not desired_sorted[
                    index
                ].startswith(prefix)

            deletes = sorted(
                path
                for path in current
                if path not in desired_blobs
                and (self._is_managed_blob(path) or stale_stats(path))
            )
            if not creates and not deletes:
                return
            overlay = [
                # The content creates the blob server side; an element that
                # also carries a sha (even null) is rejected with a 422.
                InputGitTreeElement(path=path, mode="100644", type="blob", content="")
                for path in creates
            ]
            overlay += [
                InputGitTreeElement(path=path, mode="100644", type="blob", sha=None)
                for path in deletes
            ]
            try:
                commit_tree_overlay(
                    repo,
                    self._ghc.commit,
                    overlay,
                    f"Sync VulquantaFS metadata tree ({len(creates)} created, "
                    f"{len(deletes)} deleted)",
                )
            except GithubException as ex:
                if attempt + 1 == SYNC_COMMIT_ATTEMPTS:
                    raise
                logger.info(
                    f"Branch {self._ghc.commit} moved while syncing the "
                    f"metadata tree ({ex.status}), retrying"
                )
                continue
            logger.info(
                f"Synced the metadata tree to {self._ghc.repo_name}@"
                f"{self._ghc.commit}: {len(creates)} created, "
                f"{len(deletes)} deleted"
            )
            return

    def _initial_sync_commit(self, desired_blobs: Set[str]) -> None:
        """First commit on a branch that does not exist yet.

        The desired blobs are committed as content-only creates in chunks
        of at most ``TREE_OVERLAY_ACTION_LIMIT`` elements chained by base
        tree: one request naming a whole metadata tree refused with an
        "input too large" 422 once the repository outgrew it.
        """
        repo = self._ghc.repo
        paths = sorted(desired_blobs)
        chunks = [
            paths[i : i + TREE_OVERLAY_ACTION_LIMIT]
            for i in range(0, len(paths), TREE_OVERLAY_ACTION_LIMIT)
        ] or [[]]
        parent: Optional[GitCommit] = None
        head = ""
        for number, chunk in enumerate(chunks, start=1):
            elements = [
                InputGitTreeElement(path=path, mode="100644", type="blob", content="")
                for path in chunk
            ]
            tree = (
                repo.create_git_tree(elements)
                if parent is None
                else repo.create_git_tree(elements, base_tree=parent.tree)
            )
            message = (
                f"Sync VulquantaFS metadata tree ({len(paths)} created)"
                if len(chunks) == 1
                else f"Sync VulquantaFS metadata tree ({len(paths)} created) "
                f"({number}/{len(chunks)})"
            )
            commit = repo.create_git_commit(
                message, tree, [] if parent is None else [parent]
            )
            head = commit.sha
            parent = commit
        repo.create_git_ref(f"refs/heads/{self._ghc.commit}", head)
        logger.info(
            f"Created {self._ghc.repo_name}@{self._ghc.commit} with the "
            f"synced metadata tree ({len(paths)} blobs)"
        )

    # -- file stats (``listing.stat_in_metadata``) -------------------------
    #
    # Every folder that has stats keeps them in its own ``.vulquantafsstats.json``
    # next to the file refs, keyed by descriptor message id, so a move or a
    # rename keeps them without a write. The tree load only notes the blob
    # sha; the content is read on the folder's first listing.

    push_writes_stats = False

    async def load_stats(self, directory: TGFSDirectory) -> bool:
        if not isinstance(directory, GithubDirectory) or directory.stats_loaded:
            return True
        if directory.stats_sha is None:
            directory.stats_loaded = True
            return True
        # Ten thousand concurrent listings of one folder read the blob once.
        key = id(directory)
        task = self._stats_loads.get(key)
        if task is None:
            task = asyncio.ensure_future(self._load_stats(directory))
            self._stats_loads[key] = task
            task.add_done_callback(lambda _: self._stats_loads.pop(key, None))
        # Shielded: a client that gives up does not cancel the read for the
        # listings that wait on it.
        return await asyncio.shield(task)

    async def _load_stats(self, directory: GithubDirectory) -> bool:
        sha = directory.stats_sha
        if sha is None:
            directory.stats_loaded = True
            return True
        try:
            blob = await asyncio.to_thread(self._ghc.repo.get_git_blob, sha)
            entries = json.loads(base64.b64decode(blob.content))
            if not isinstance(entries, dict):
                raise ValueError("not a JSON object")
        except Exception as ex:
            logger.warning(
                f"Could not read {directory.stats_path} from "
                f"{self._ghc.repo_name}: {ex}"
            )
            return False
        for fr in directory.find_files():
            # A ref that changed since the start has a stored entry from
            # before the change; it stays empty and is read again.
            if fr.stat is None and fr.stat_epoch == 0:
                fr.stat = FileStat.from_dict(entries.get(str(fr.message_id)))
        directory.stats_loaded = True
        return True

    def stats_stored(self, directory: TGFSDirectory) -> bool:
        return isinstance(directory, GithubDirectory) and (
            directory.stats_sha is not None
        )

    async def save_stats(
        self, directories: Iterable[TGFSDirectory], keep: bool
    ) -> None:
        """Write the stats files of ``directories`` in one commit.

        Raises when a folder's stored stats could not be read first or the
        commit failed; the caller keeps the folders and tries again.
        """
        changes: list[tuple[GithubDirectory, Optional[str]]] = []
        unreadable: list[str] = []
        # Directories compare by content, so they are told apart by identity.
        for directory in {id(d): d for d in directories}.values():
            if not isinstance(directory, GithubDirectory):
                continue
            if not directory.is_attached():
                # Deleted meanwhile; its stats file went with it.
                continue
            if keep and not await self.load_stats(directory):
                unreadable.append(directory.stats_path)
                continue
            content = self._stats_content(directory) if keep else None
            if content is None:
                if directory.stats_sha is not None:
                    changes.append((directory, None))
            elif _blob_sha(content) != directory.stats_sha:
                changes.append((directory, content))

        if changes:
            async with self._ghc.lock:
                # RMDIR may have completed while this write was queued.
                # A stats blob must never recreate a detached directory.
                attached = [(d, content) for d, content in changes if d.is_attached()]
                if attached:
                    writes = [(d.stats_path, content) for d, content in attached]
                    await self._ghc.write(self._commit_stats, writes)
                    for directory, content in attached:
                        directory.stats_sha = (
                            None if content is None else _blob_sha(content)
                        )
                        directory.stats_loaded = True

        if unreadable:
            raise TechnicalError(
                f"Could not read the stored stats of {', '.join(unreadable)}"
            )

    @staticmethod
    def _stats_content(directory: GithubDirectory) -> Optional[str]:
        entries = {
            str(fr.message_id): fr.stat.to_dict()
            for fr in directory.find_files()
            if fr.stat is not None
        }
        if not entries:
            return None
        return json.dumps(entries, sort_keys=True, separators=(",", ":"))

    def _commit_stats(self, writes: list[tuple[str, Optional[str]]]) -> None:
        """One commit on top of the branch head; retried when another
        commit landed in between (the ref update is not forced)."""
        repo = self._ghc.repo
        elements = [
            (
                InputGitTreeElement(path=path, mode="100644", type="blob", sha=None)
                if content is None
                else InputGitTreeElement(
                    path=path, mode="100644", type="blob", content=content
                )
            )
            for path, content in writes
        ]
        for attempt in range(STATS_COMMIT_ATTEMPTS):
            ref = repo.get_git_ref(f"heads/{self._ghc.commit}")
            base_commit = repo.get_git_commit(ref.object.sha)
            tree = repo.create_git_tree(elements, base_tree=base_commit.tree)
            commit = repo.create_git_commit(
                f"{STATS_COMMIT_PREFIX} of {len(writes)} folder(s)",
                tree,
                [base_commit],
            )
            try:
                ref.edit(commit.sha)
                logger.info(
                    f"Wrote the stats of {len(writes)} folder(s) "
                    f"to {self._ghc.repo_name}"
                )
                return
            except GithubException as ex:
                if attempt + 1 == STATS_COMMIT_ATTEMPTS:
                    raise
                logger.info(
                    f"Branch {self._ghc.commit} moved while writing file stats "
                    f"({ex.status}), retrying"
                )

    async def get(self) -> TGFSMetadata:
        # Off the event loop: PyGithub and the facade are synchronous.
        head = await asyncio.to_thread(self._head_commit)
        if head is not None:
            # The listing only changes when the branch moves, and a grown
            # repository costs hundreds of requests to list: when the head
            # of this boot is the head the last boot listed, the local
            # snapshot of that listing serves the tree in its place.
            tree = await asyncio.to_thread(
                self._snapshot.load,
                self._ghc.repo_name,
                self._ghc.commit,
                head.sha,
            )
            if tree is not None:
                await asyncio.to_thread(self._timestamps.load)
                metadata = TGFSMetadata.from_dict(tree)
                metadata.revision = head.epoch
                return metadata
        # Off the loop too: with a large repo the walk is a request per
        # directory. The structure is built fresh, so nothing is shared.
        root_dir = await asyncio.to_thread(self._load_directory_at, head)
        metadata = TGFSMetadata(dir=root_dir)
        # The replicated layer compares stores by this date: the branch head
        # is when this copy was written last. A store that proves dead later
        # during the read fails the load anyway; an empty repository has no
        # head and simply dates as unknown.
        metadata.revision = head.epoch if head else None
        if head is not None:
            await asyncio.to_thread(
                self._snapshot.save,
                self._ghc.repo_name,
                self._ghc.commit,
                head.sha,
                metadata.to_dict(),
            )
        return metadata

    def _head_commit(self) -> Optional[_HeadCommit]:
        """The branch head's sha and commit date, or None.

        Asked for through ``get_branch`` and ``get_git_commit``, the two
        calls that carry their request themselves: a dead connection
        then fails inside the guarded call, where the breaker sees it."""
        try:
            branch = self._ghc.repo.get_branch(self._ghc.commit)
            sha = branch.commit.sha
            commit = self._ghc.repo.get_git_commit(sha)
            date = commit.committer.date
        except Exception:
            return None
        if not isinstance(date, datetime.datetime):
            return None
        return _HeadCommit(sha, date.timestamp())

    def _head_commit_epoch(self) -> Optional[float]:
        """When the branch was written last, as a Unix epoch, or None."""
        head = self._head_commit()
        return None if head is None else head.epoch

    async def head_revision(self) -> Optional[float]:
        """The branch head's commit date; an empty store is reported
        with ``NoStoredMetadata`` and a head that cannot be dated with
        ``None``, so a load can tell unreachable, empty and undatable
        apart."""
        try:
            head = await asyncio.to_thread(self._probe_head)
        except GithubException as ex:
            if ex.status in (404, 409):
                raise NoStoredMetadata(
                    f"The branch {self._ghc.commit} of {self._ghc.repo_name} "
                    f"does not exist yet"
                ) from ex
            raise
        return None if head is None else head.epoch

    def _probe_head(self) -> Optional[_HeadCommit]:
        """The branch head, letting a failed read raise (a swallowed one
        would hide an outage from the load and from the breaker)."""
        branch = self._ghc.repo.get_branch(self._ghc.commit)
        sha = branch.commit.sha
        commit = self._ghc.repo.get_git_commit(sha)
        date = commit.committer.date
        if not isinstance(date, datetime.datetime):
            return None
        return _HeadCommit(sha, date.timestamp())

    def _load_directory_at(self, head: Optional[_HeadCommit]) -> GithubDirectory:
        return self._build_directory_structure()

    def _build_directory_structure(self, ref: Optional[str] = None) -> GithubDirectory:
        root = GithubDirectory(
            self._ghc, name="root", parent=None, children=[], files=[]
        )
        self._restore_root_timestamps(root)

        try:
            entries = self._read_tree(ref) if ref is not None else self._read_tree()
            if entries is not None:
                self._build_from_tree(entries, root)
            else:
                contents = self._ghc.repo.get_contents("", ref=self._ghc.commit)
                self._process_contents(contents, root)
        except GithubException as ex:
            if ref is None and ex.status in (404, 409):
                # An empty repository, or the branch does not exist yet:
                # there is no tree, which is how every installation starts.
                logger.warning(
                    f"{self._ghc.repo_name}@{self._ghc.commit} holds no tree "
                    f"yet ({ex.status}); starting with an empty directory tree"
                )
            else:
                raise TechnicalError(
                    f"Could not read the metadata tree from "
                    f"{self._ghc.repo_name}@{self._ghc.commit}: {ex}"
                ) from ex
        except Exception as ex:
            # An unreachable repo must surface as a failure (the replicated
            # layer fails over to another metadata store), never as a
            # silently empty tree that would go on to diverge from the repo.
            raise TechnicalError(
                f"Could not read the metadata tree from "
                f"{self._ghc.repo_name}@{self._ghc.commit}: {ex}"
            ) from ex

        return root

    def _read_tree(self, ref: Optional[str] = None) -> Optional[Sequence[_TreeEntry]]:
        """The whole repo tree in one request, or ``None`` if it is unusable.

        ``get_contents`` lists exactly one directory, so walking the tree with
        it costs one round trip per directory -- 306 of them here, all before
        a single file can be listed. The recursive tree API returns every blob
        and subtree at once instead, which makes the load independent of how
        many folders the repo holds.

        GitHub truncates the response for very large trees (over 100,000
        entries or 7 MB, which a few tens of thousands of encrypted file
        names reach). That would silently hide whole folders, so the tree is
        then read one directory at a time instead.
        """
        try:
            tree = self._ghc.repo.get_git_tree(ref or self._ghc.commit, recursive=True)
            if not tree.truncated:
                return [_TreeEntry(e.path, e.type, e.sha) for e in tree.tree]
            logger.warning(
                f"Tree of {self._ghc.repo_name}@{self._ghc.commit} came back "
                "truncated; reading it one directory at a time"
            )
        except Exception as ex:
            logger.warning(
                f"Could not read the tree of {self._ghc.repo_name}@{self._ghc.commit}, "
                f"falling back to a directory-by-directory walk: {ex}"
            )
            return None
        try:
            return self._walk_tree()
        except Exception as ex:
            logger.warning(
                f"Could not walk the tree of {self._ghc.repo_name}@{self._ghc.commit}, "
                f"falling back to the contents API: {ex}"
            )
            return None

    def _walk_tree(self) -> list[_TreeEntry]:
        """Every entry of the repo, one (non-recursive) tree per directory.

        One request per directory, like the contents API, but a tree lists
        all of a directory's entries where the contents API stops at 1000:
        a folder of 20k files came back with 1000 of them. Directories are
        read level by level, ``tree_fetch_concurrency`` requests at a time,
        so a big repo does not pay one round trip after another. Raises
        when a single directory's tree is truncated too.
        """
        started = time.monotonic()
        entries: list[_TreeEntry] = []
        read = 0
        pending: list[tuple[str, str]] = [("", self._ghc.commit)]
        with ThreadPoolExecutor(
            max_workers=self._ghc.tree_fetch_concurrency,
            thread_name_prefix="github-tree",
        ) as pool:
            while pending:
                batch, pending = (
                    pending[: self._ghc.tree_fetch_concurrency],
                    pending[self._ghc.tree_fetch_concurrency :],
                )
                trees = list(
                    pool.map(lambda item: self._ghc.repo.get_git_tree(item[1]), batch)
                )
                for (prefix, _), t in zip(batch, trees):
                    read += 1
                    if t.truncated:
                        raise TechnicalError(
                            f"The tree of '{prefix or '/'}' is truncated"
                        )
                    for e in t.tree:
                        path = f"{prefix}{e.path}"
                        entries.append(_TreeEntry(path, e.type, e.sha, e.mode))
                        if e.type == "tree":
                            pending.append((f"{path}/", e.sha))
        logger.info(
            f"Rebuilt the tree of {self._ghc.repo_name}@{self._ghc.commit} "
            f"directory by directory: {read} directories in "
            f"{time.monotonic() - started:.1f}s"
        )
        return entries

    def _attach_loaded_ref(
        self,
        directory: GithubDirectory,
        name: str,
        message_id: int,
        mirrors: dict[str, int],
    ) -> None:
        """Attach one ref rebuilt from the repo, superseding stale blobs.

        A ref whose descriptor moved can leave its previous segment behind
        until a full sync removes it; of two segments for one name the
        freshest wins -- the highest descriptor id, then the one that
        carries mirror copies.
        """
        existing = directory.find_files([name])
        if existing:
            current = existing[0]
            if (message_id, bool(mirrors)) <= (
                current.message_id,
                bool(current.mirrors),
            ):
                return
            directory.files.remove(current)
        fr = directory.attach_file_ref(name, message_id)
        fr.mirrors = mirrors

    def _build_from_tree(
        self, entries: Sequence[_TreeEntry], root: GithubDirectory
    ) -> None:
        """Materialise the in-memory tree from one flat list of tree entries.

        Entries carry full storage paths and arrive in no guaranteed order, so
        directories are resolved on demand and every missing parent is created
        along the way.
        """
        dirs: dict[str, GithubDirectory] = {"": root}

        for entry in entries:
            if entry.type == "tree":
                self._dir_at(entry.path, dirs)
            elif entry.type == "blob":
                parent_path, _, segment = entry.path.rpartition("/")
                if segment == ".gitkeep":
                    continue
                if segment == STATS_FILE_NAME:
                    # Read on the folder's first listing, not here.
                    self._dir_at(parent_path, dirs).stats_sha = entry.sha
                    continue
                try:
                    stored_name, message_id, mirrors = parse_ref_segment(segment)
                    file_name, _ = self._decode_name(stored_name)
                    # attach, not create: the directory's date is the git
                    # history's, not the moment this tree was rebuilt.
                    self._attach_loaded_ref(
                        self._dir_at(parent_path, dirs),
                        file_name,
                        message_id,
                        mirrors,
                    )
                except ValueError:
                    logger.warning(
                        f"Invalid name format for {segment}, expected a format like 'name.message_id'"
                    )

    def _dir_at(
        self, storage_path: str, dirs: dict[str, GithubDirectory]
    ) -> GithubDirectory:
        """The directory at ``storage_path``, created with its parents if new."""
        found = dirs.get(storage_path)
        if found is not None:
            return found

        parent_path, _, segment = storage_path.rpartition("/")
        dir_name, was_encrypted = self._decode_name(segment)
        child = self._create_child_dir(
            dir_name,
            self._dir_at(parent_path, dirs),
            stored_encrypted=was_encrypted,
            defer_timestamps=True,
        )
        # Dates come from the cache on first read. Warming them here only
        # queues the path; the workers pay for the lookup in the background
        # instead of holding the load for two calls per directory.
        if self._ghc.timestamps is not None:
            self._ghc.timestamps.request(storage_path)
        dirs[storage_path] = child
        return child

    def _restore_root_timestamps(self, root: GithubDirectory) -> None:
        """Give the root its real dates from the repo's own metadata.

        The root has no ``.gitkeep`` to date it from, so fall back to the
        backing repository's creation and last-push timestamps instead of
        the ``now()`` dataclass default. Best-effort: any failure leaves the
        default rather than breaking the load.
        """
        try:
            created = self._ghc.repo.created_at
            modified = self._ghc.repo.pushed_at or self._ghc.repo.updated_at
            if isinstance(created, datetime.datetime):
                root.created_at = created
            if isinstance(modified, datetime.datetime):
                root.modified_at = modified
        except Exception as ex:
            logger.debug(f"Could not read repo timestamps for root: {ex}")

    def _create_child_dir(
        self,
        name: str,
        parent_dir: GithubDirectory,
        stored_encrypted: bool = False,
        defer_timestamps: bool = False,
    ) -> GithubDirectory:
        child_dir = GithubDirectory(
            self._ghc,
            name,
            parent_dir,
            stored_encrypted=stored_encrypted,
            defer_timestamps=defer_timestamps,
        )
        parent_dir.children.append(child_dir)
        return child_dir

    def _decode_name(self, raw: str) -> tuple[str, bool]:
        """Map an on-repo path segment to its (plaintext, was_encrypted) form.

        Legacy plaintext segments pass through unchanged; encrypted ones are
        decrypted with the configured key. This is what lets encrypted and
        plaintext entries coexist in the same repo during/after migration.
        """
        if not is_encrypted_path_name(raw):
            return raw, False
        key = self._ghc.name_key
        if key is None:
            return raw, True  # cannot decrypt without the key
        try:
            return decrypt_path_name(key, raw), True
        except PathNameEncryptionError as ex:
            logger.warning(f"Failed to decrypt path name {raw!r}: {ex}")
            return raw, True

    def _process_contents(
        self, contents: list[ContentFile] | ContentFile, parent_dir: GithubDirectory
    ) -> None:
        if not isinstance(contents, list):
            contents = [contents]

        for content in contents:
            if content.type == "dir":
                # content.name is the on-repo (possibly encrypted) segment;
                # decrypt it for the in-memory model but keep using
                # content.path (the storage path) for the git-history lookup.
                dir_name, was_encrypted = self._decode_name(content.name)
                child_dir = self._create_child_dir(
                    dir_name,
                    parent_dir,
                    stored_encrypted=was_encrypted,
                    defer_timestamps=True,
                )
                # Dates come from the cache on first read. Warming them here
                # only queues the path; the workers pay for the lookup in the
                # background instead of holding the boot for two calls each.
                if self._ghc.timestamps is not None:
                    self._ghc.timestamps.request(content.path)
                try:
                    child_contents = self._ghc.repo.get_contents(
                        content.path, ref=self._ghc.commit
                    )
                    self._process_contents(child_contents, child_dir)
                except Exception as ex:
                    logger.warning(
                        f"Failed to construct directory {content.name}: {ex}"
                    )
            elif content.type == "file":
                try:
                    if content.name == ".gitkeep":
                        continue
                    if content.name == STATS_FILE_NAME:
                        parent_dir.stats_sha = content.sha
                        continue
                    stored_name, message_id, mirrors = parse_ref_segment(content.name)
                    file_name, _ = self._decode_name(stored_name)
                    self._attach_loaded_ref(parent_dir, file_name, message_id, mirrors)
                except ValueError:
                    logger.warning(
                        f"Invalid name format for {content.name}, expected a format like 'name.message_id'"
                    )
