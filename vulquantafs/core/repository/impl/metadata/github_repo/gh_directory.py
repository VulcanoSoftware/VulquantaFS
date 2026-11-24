import asyncio
import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence, Set, TypeVar

import requests
from github import Github, InputGitTreeElement
from github.Repository import Repository

from vulquantafs.core.model import TGFSDirectory, TGFSFileRef
from vulquantafs.crypto.path_names import encrypt_path_name
from vulquantafs.errors import (
    DirectoryIsNotEmpty,
    FileOrDirectoryDoesNotExist,
    TechnicalError,
)
from vulquantafs.utils.time import FIRST_DAY_OF_EPOCH

from ..breaker import StoreUnreachable
from .dir_timestamps import DirTimestampStore
from .stats_file import STATS_FILE_NAME

logger = logging.getLogger(__name__)

T = TypeVar("T")

# A dropped connection (GitHub or the network hiccups for a few seconds) is
# tried again briefly; a WebDAV client gives up after about 20 s.
PUT_ATTEMPTS = 3
PUT_RETRY_DELAY = 1.5

# A deferred file reference write (an SFTP open does not wait for it) can
# take much more punishment: ten rounds over roughly ten minutes before the
# reference is given up on.
DEFERRED_PUT_ATTEMPTS = 10
DEFERRED_PUT_RETRY_DELAY = 5

# The deferred creates are committed in batches: the first wait collects the
# co-arrivals (an SFTP client opens several files side by side), the rounds
# after it run back to back, while the bulk upload keeps the queue filled, so
# the drain keeps up with any client instead of falling behind by one commit
# per file -- the queue that once grew past ten pushed the opens over
# FileZilla's 20-second timeout.
DEFERRED_COMMIT_WINDOW = 1.0
# Completed uploads wait for their reference. Briefly collect concurrent CLOSE
# requests without adding the full background window to each small file.
AWAITED_COMMIT_WINDOW = 0.025
# ... but never longer than this. A GitHub read timeout plus the write
# thread's inline retries can hold the shared batch for half a minute and
# more; a client that does not hear its CLOSE reply within about 20 s drops
# the connection and aborts the upload. Past this bound the caller hears a
# failure instead while the commit keeps retrying in the background, so a
# retransmission appends a version to the same reference instead of finding
# a server that never answers.
AWAITED_COMMIT_MAX_WAIT = 15.0

# GitHub refuses a tree whose request carries a whole repository: the
# rewrites sent every blob as an element and large metadata repositories
# came back "Sorry, your request timed out.  It's likely that your input
# was too large to process" (422) -- or landed after tens of minutes. An
# overlay commit only names the delta on top of the base tree, and a
# single one never carries more than this many elements; a bigger delta
# arrives as several commits chained by base tree.
TREE_OVERLAY_ACTION_LIMIT = 1000


def commit_tree_overlay(
    repo: Repository,
    branch: str,
    overlay: Sequence[InputGitTreeElement],
    message: str,
) -> str:
    """Land ``overlay`` on top of the branch's current tree; move the branch once.

    A tree element only overlays the base tree it carries: an element with
    content or a sha adds or replaces its path, one with neither (``sha=None``)
    removes it. Only the delta travels, in commits of at most
    ``TREE_OVERLAY_ACTION_LIMIT`` elements chained by base tree -- so the
    request bodies stay small however large the repository, and a chunk
    that did not land leaves the branch as it was. Blocking; raises when
    a commit or the final ref update fails, which callers treat like any
    failed commit (retried, or the store goes stale).

    Returns the sha the branch was moved to.
    """
    ref = repo.get_git_ref(f"heads/{branch}")
    first_base = repo.get_git_commit(ref.object.sha)
    chunks = [
        list(overlay[i : i + TREE_OVERLAY_ACTION_LIMIT])
        for i in range(0, len(overlay), TREE_OVERLAY_ACTION_LIMIT)
    ] or [[]]
    commit = first_base
    for number, chunk in enumerate(chunks, start=1):
        tree = repo.create_git_tree(chunk, base_tree=commit.tree)
        chunk_message = (
            message if len(chunks) == 1 else f"{message} ({number}/{len(chunks)})"
        )
        # Preserve the original head as an ancestor. An orphan first chunk
        # makes the final ref update a non-fast-forward on GitHub.
        commit = repo.create_git_commit(chunk_message, tree, [commit])
    ref.edit(commit.sha)
    return commit.sha


# Deleting a file reference used to wait on two API calls on the writer
# thread (read the blob's sha, commit its deletion), so a 20k-file folder
# deleted over SFTP took hours. ``delete_file_ref_async`` only registers
# the blob paths instead, and the blobs leave in one tree-rewrite commit
# once the batch is full or the oldest registration has waited this long
# (``GithubRepoMetadataRepository.push``).
DROP_FLUSH_BATCH = 100
DROP_FLUSH_INTERVAL = 5.0
DROP_COMMIT_ATTEMPTS = 3


def _is_transient(ex: Exception) -> bool:
    status = getattr(ex, "status", None)
    if status is not None:
        return status >= 500
    # A refusal of the fail-fast wrapper is the outage it shortcuts
    # happening; the deferred writes keep retrying until the probe finds
    # the service again.
    return isinstance(
        ex, (requests.exceptions.RequestException, OSError, StoreUnreachable)
    )


def _is_sha_missing(ex: Exception) -> bool:
    status = getattr(ex, "status", None)
    if status != 422:
        return False
    data = getattr(ex, "data", None)
    message = data.get("message", "") if isinstance(data, dict) else ""
    return '"sha" wasn\'t supplied' in str(message)


@dataclass
class _PendingRef:
    """A deferred file reference create whose blob still has to land.

    The in-memory tree already lists the reference; only the repo blob
    waits. ``directory`` is where its in-tree membership is re-checked at
    commit time, so a delete or rename that landed meanwhile wins.
    """

    directory: "GithubDirectory"
    name: str
    file_message_id: int
    file_ref: TGFSFileRef
    attempts: int = 0
    committed: Optional[asyncio.Future[None]] = None

    def refresh_message_id(self) -> bool:
        """Adopt the ref's current descriptor id; ``True`` when it moved.

        A retransmission whose CLOSE was answered while this create was
        still retrying appends a version and moves the ref to a fresh
        descriptor message. The blob must name that id, not the stale one,
        or the next start reads the older, replaced message back. A move
        also means the file is real in the tree beyond this create, so it
        earns a full new round of attempts instead of the drop.
        """
        if self.file_ref.message_id == self.file_message_id:
            return False
        self.file_message_id = self.file_ref.message_id
        self.attempts = 0
        return True


@dataclass
class GithubConfig:
    gh: Github
    repo_name: str
    repo: Repository
    commit: str
    # Deterministic AES-SIV key for path-name encryption. ``None`` keeps the
    # legacy behaviour of storing plaintext directory/file names in the repo.
    name_key: Optional[bytes] = None
    # Where the directory dates come from. ``None`` leaves every loaded
    # directory on the ``now()`` default, as it was before the cache existed.
    timestamps: Optional[DirTimestampStore] = None
    # How many directories the truncated-tree fallback reads in parallel
    # (one request per directory; see ``_walk_tree``).
    tree_fetch_concurrency: int = 8
    # Squash the branch's commit history once it grows past this many
    # commits (0 keeps it); read by ``maintain`` from the store config.
    squash_history_max_commits: int = 0
    # File-reference blob paths removed from the in-memory tree but not
    # yet from the repo, plus the monotonic time the oldest was
    # registered. ``delete_file_ref_async`` registers, ``push`` flushes
    # batches into one commit; touch them on the event loop only.
    pending_drops: Set[str] = field(
        default_factory=set,
        compare=False,
        repr=False,
    )
    pending_drops_since: float = 0.0
    # File-reference creates scheduled for the batched background write
    # (``create_file_ref_deferred``). Touched on the event loop only; the
    # drain empties it one ``lock``-covered commit at a time.
    pending_refs: list[_PendingRef] = field(
        default_factory=list,
        compare=False,
        repr=False,
    )
    # The background drain committing ``pending_refs``; ``None`` while
    # there is nothing to commit.
    pending_refs_flusher: Optional[asyncio.Task[None]] = field(
        default=None,
        compare=False,
        repr=False,
    )
    # GitLab commits file actions rather than native Git tree overlays.
    delete_subtree: Optional[Callable[[str, bool], int]] = field(
        default=None, compare=False, repr=False
    )
    removing_directory: Optional["GithubDirectory"] = field(
        default=None, compare=False, repr=False
    )
    # PyGithub is synchronous: a slow write on the event loop freezes
    # every other request. The writes run on this
    # one thread instead. One thread, because every write is a commit on the
    # same branch: two at once only race each other into 409/422 conflicts.
    writer: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="github-writes"
        ),
        compare=False,
        repr=False,
    )
    # Serialises the file-reference blob writes of every directory that
    # shares this repo. A delete or rename that lands while a deferred
    # create is still retrying must not be undone by that retry -- the
    # deleted or renamed file would reappear after a restart. The lock is
    # FIFO-fair, so the write that asked first also runs first, and the
    # in-tree check a deferred task does under the lock always sees the
    # deletes that waited before it. The writer thread keeps the blocking
    # blob calls themselves from interleaving.
    lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        compare=False,
        repr=False,
    )

    async def write(self, fn: Callable[..., T], *args) -> T:
        """Run the blocking GitHub write ``fn`` off the event loop, after
        the writes queued before it."""
        return await asyncio.get_running_loop().run_in_executor(self.writer, fn, *args)

    def schedule_ref(self, entry: _PendingRef) -> None:
        """Queue a create for the shared commit worker."""
        self.pending_refs.append(entry)
        if self.pending_refs_flusher is None or self.pending_refs_flusher.done():
            window = (
                DEFERRED_COMMIT_WINDOW
                if entry.committed is None
                else AWAITED_COMMIT_WINDOW
            )
            self.pending_refs_flusher = asyncio.get_running_loop().create_task(
                self._drain_pending_refs(window), name="github-file-refs"
            )

    async def _drain_pending_refs(self, window: float) -> None:
        # One commit per batch, not per reference: a bulk upload schedules
        # a create for every file it opens, and one-at-a-time commits fell
        # behind the client, until the queued batched flushes at the back
        # of the same lock held the metadata push of the open itself --
        # for longer than the client waits.
        await asyncio.sleep(window)
        while self.pending_refs:
            if await self._commit_pending_refs():
                await asyncio.sleep(DEFERRED_PUT_RETRY_DELAY)

    async def _commit_pending_refs(self) -> bool:
        """Commit the queued creates. ``True`` when some of them need
        another round."""
        batch = self.pending_refs
        self.pending_refs = []
        async with self.lock:
            alive = []
            for entry in batch:
                if any(
                    ref is entry.file_ref
                    for ref in entry.directory.find_files([entry.name])
                ):
                    alive.append(entry)
                elif entry.committed is not None and not entry.committed.done():
                    entry.committed.set_exception(
                        FileOrDirectoryDoesNotExist(entry.name)
                    )
            if not alive:
                return False
            for entry in alive:
                entry.refresh_message_id()
            if len(alive) == 1:
                entry = alive[0]
                try:
                    await self.write(
                        entry.directory._put_file_ref,
                        entry.name,
                        entry.file_message_id,
                    )
                    self._confirm(entry)
                    return False
                except Exception as ex:
                    if self._retry(entry, ex):
                        self.pending_refs.append(entry)
                        return True
                    return False
            try:
                await self.write(
                    _put_file_refs,
                    self.repo,
                    self.commit,
                    [
                        (
                            entry.directory._ref_storage_path(
                                entry.name, entry.file_message_id
                            ),
                            entry.directory._message_name(entry.name),
                        )
                        for entry in alive
                    ],
                )
                logger.info(
                    f"Created {len(alive)} file reference(s) in {self.repo_name}"
                )
                for entry in alive:
                    self._confirm(entry)
                return False
            except Exception as ex:
                logger.warning(
                    f"Committing {len(alive)} file reference(s) to "
                    f"{self.repo_name} failed ({ex}); writing them one by one"
                )
            failures: list[tuple[_PendingRef, Exception]] = []
            for entry in alive:
                try:
                    await self.write(
                        entry.directory._put_file_ref,
                        entry.name,
                        entry.file_message_id,
                    )
                    self._confirm(entry)
                except Exception as ex:
                    failures.append((entry, ex))
            retry = [entry for entry, ex in failures if self._retry(entry, ex)]
            self.pending_refs.extend(retry)
            return bool(retry)

    @staticmethod
    def _confirm(entry: _PendingRef) -> None:
        if entry.committed is not None and not entry.committed.done():
            entry.committed.set_result(None)

    def _retry(self, entry: _PendingRef, ex: Exception) -> bool:
        entry.attempts += 1
        # A caller still waiting on CLOSE gets the outcome of this round,
        # not a reply for a reference that might later disappear.
        if entry.committed is not None and not entry.committed.done():
            logger.error(
                f"Failed to create file reference {entry.name} in "
                f"{self.repo_name}: {ex}"
            )
            entry.directory.files.remove(entry.file_ref)
            if entry.committed is not None and not entry.committed.done():
                entry.committed.set_exception(ex)
            return False
        # Nobody waits on the reference any more (the CLOSE reply gave up
        # on its deadline, or there never was a waiter): the blob keeps
        # retrying in the background, so a retransmission finds the file
        # in the tree instead of a hole.
        if entry.refresh_message_id():
            logger.info(
                f"File reference {entry.name} moved to a newer message "
                f"while its blob waited; committing {entry.file_message_id}"
            )
            return True
        if entry.attempts >= DEFERRED_PUT_ATTEMPTS or not _is_transient(ex):
            logger.error(
                f"Failed to create file reference {entry.name} in "
                f"{self.repo_name}: {ex}"
            )
            entry.directory.files.remove(entry.file_ref)
            if entry.committed is not None and not entry.committed.done():
                entry.committed.set_exception(ex)
            return False
        logger.warning(
            f"Creating file reference {entry.name} failed ({ex}); trying again"
        )
        return True


def _put_file_refs(
    repo: Repository, branch: str, refs: Sequence[tuple[str, str]]
) -> None:
    """Create a batch of empty file-reference blobs in one commit. Blocking;
    raises when GitHub refuses."""
    overlay = [
        InputGitTreeElement(path, "100644", "blob", content="") for path, _ in refs
    ]
    commit_tree_overlay(repo, branch, overlay, f"Create {len(refs)} file reference(s)")


class GithubDirectory(TGFSDirectory):
    file_refs_persist_on_create = True

    def __init__(
        self,
        ghc: GithubConfig,
        name: str,
        parent: Optional[TGFSDirectory],
        children: Optional[list[TGFSDirectory]] = None,
        files: Optional[list[TGFSFileRef]] = None,
        stored_encrypted: Optional[bool] = None,
        defer_timestamps: bool = False,
    ):
        # These have to exist before the dataclass initialiser runs: it assigns
        # created_at/modified_at, which goes through the setters below.
        self._created_at = self._modified_at = FIRST_DAY_OF_EPOCH
        self._created_pending = defer_timestamps
        self._modified_pending = defer_timestamps
        self._ts_initialised = False

        super().__init__(name, parent, children or [], files or [])
        self._ghc = ghc
        self._removed = False
        # Whether THIS directory is stored under an encrypted name in the
        # repo. New directories follow the configured key; directories loaded
        # from the repo carry whatever form they were found in, so encrypted
        # and legacy-plaintext folders can be read and written side by side.
        self._stored_encrypted = (
            stored_encrypted
            if stored_encrypted is not None
            else ghc.name_key is not None
        )
        self._ts_initialised = True
        # Blob sha of this folder's stats file in the repo, ``None`` when
        # there is none; whether its content has been put on the file refs.
        self.stats_sha: Optional[str] = None
        self.stats_loaded = False
        # Background blob writes started by ``create_file_ref_deferred``;
        # only kept here so they are not garbage collected mid-flight.
        self._deferred_refs: set[asyncio.Task] = set()

    # --- dates ---------------------------------------------------------------
    #
    # A directory loaded from the repo does not know its dates yet: reading them
    # costs two GitHub calls, and doing that for every directory while the tree
    # is built is what made the boot grow with the folder count. So the load
    # marks them pending and they are filled in from the cache on first read --
    # never by blocking the reader, because a PROPFIND of a folder with a
    # hundred children would otherwise pay for a hundred round trips at once.

    @property
    def created_at(self) -> datetime.datetime:
        self._resolve_timestamps()
        return self._created_at

    @created_at.setter
    def created_at(self, value: datetime.datetime) -> None:
        self._created_at = value
        if self._ts_initialised:
            # An explicit write knows better than the git history.
            self._created_pending = False

    @property
    def modified_at(self) -> datetime.datetime:
        self._resolve_timestamps()
        return self._modified_at

    @modified_at.setter
    def modified_at(self, value: datetime.datetime) -> None:
        self._modified_at = value
        if self._ts_initialised:
            self._modified_pending = False

    def _resolve_timestamps(self) -> None:
        if not (self._created_pending or self._modified_pending):
            return

        store = getattr(getattr(self, "_ghc", None), "timestamps", None)
        if store is None:
            return

        path = self._github_path
        if not path:
            # The root is dated from the repo's own metadata, which is free.
            self._created_pending = self._modified_pending = False
            return

        found = store.get(path)
        if found is None:
            # Queued for the background workers; report the default until then.
            return

        created, modified = found
        if self._created_pending:
            self._created_at = created
            self._created_pending = False
        if self._modified_pending:
            self._modified_at = modified
            self._modified_pending = False

    def is_attached(self) -> bool:
        """Whether this folder is still part of the tree (not deleted)."""
        node: TGFSDirectory = self
        while node.parent is not None:
            if not any(child is node for child in node.parent.children):
                return False
            node = node.parent
        return True

    @property
    def stats_path(self) -> str:
        """Repo path of this folder's stats file."""
        return self.join_path(self._github_path, STATS_FILE_NAME)

    @staticmethod
    def join_path(*args: str) -> str:
        """Join paths in a way that is compatible with GitHub"""
        return "/".join(part.strip("/") for part in args if part)

    @property
    def _storage_name(self) -> str:
        """The on-repo name of this directory (encrypted when applicable)."""
        key = self._ghc.name_key
        if key is not None and self._stored_encrypted:
            return encrypt_path_name(key, self.name)
        return self.name

    def _file_ref_storage_name(self, name: str, message_id: int) -> str:
        """On-repo filename ``<name>.<id>``, with ``<name>`` encrypted if keyed."""
        key = self._ghc.name_key
        if key is not None:
            return f"{encrypt_path_name(key, name)}.{message_id}"
        return f"{name}.{message_id}"

    def _message_name(self, name: str) -> str:
        """``name`` as it may appear in a commit message: encrypted when
        keyed, like the path it belongs to."""
        key = self._ghc.name_key
        return encrypt_path_name(key, name) if key is not None else name

    @property
    def _github_path(self) -> str:
        """Get the GitHub repository path for this directory"""
        if self.parent is None:
            return ""

        if isinstance(self.parent, GithubDirectory):
            parent_path = self.parent._github_path
        else:
            parent_path = ""

        return self.join_path(parent_path, self._storage_name)

    def create_dir_skip_github_ops(self, name: str) -> "GithubDirectory":
        res = GithubDirectory(self._ghc, name, self)
        self.children.append(res)
        return res

    def create_dir(self, name: str) -> "GithubDirectory":
        self._require_not_removing()
        child = super().create_dir(name)

        # Create directory in GitHub by creating a placeholder file. The new
        # directory follows the configured key, so its on-repo segment is the
        # encrypted name when path-name encryption is enabled.
        key = self._ghc.name_key
        child_segment = encrypt_path_name(key, name) if key is not None else name
        dir_path = self.join_path(self._github_path, child_segment, ".gitkeep")
        try:
            self._ghc.repo.create_file(
                path=dir_path,
                # The on-repo segment, never the plaintext name: commit
                # messages are as public as the paths.
                message=f"Create directory {child_segment}",
                content="",
                branch=self._ghc.commit,
            )
            logger.info(f"Created directory {name} in GitHub repository at {dir_path}")
        except Exception as ex:
            logger.error(f"Failed to create directory {name} in GitHub: {ex}")
            self.children.remove(child)
            raise

        # Convert the child to GithubDirectory
        github_child = GithubDirectory(
            ghc=self._ghc,
            name=child.name,
            parent=self,
            children=child.children,
            files=child.files,
        )

        # Replace the child in the parent's children list
        child_index = self.children.index(child)
        self.children[child_index] = github_child

        return github_child

    def delete(self) -> None:
        self._require_no_removing_descendant()
        if self.parent:
            # Remove all files and subdirectories from GitHub
            self._delete_github_directory()
        super().delete()

    async def delete_empty_async(self) -> None:
        # Finish the in-memory change even if the SFTP connection closes
        # while the thread is committing the removal.
        operation = asyncio.create_task(self._delete_empty_on_writer())
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            operation.add_done_callback(self._log_cancelled_removal)
            raise

    @staticmethod
    def _log_cancelled_removal(operation: asyncio.Task[None]) -> None:
        if not operation.cancelled() and (error := operation.exception()) is not None:
            logger.warning(
                "Directory removal failed after the client disconnected: %s", error
            )

    async def _delete_empty_on_writer(self) -> None:
        async with self._ghc.lock:
            if not self.is_attached():
                raise FileOrDirectoryDoesNotExist(self.absolute_path)
            if self.find_dirs() or self.find_files():
                raise DirectoryIsNotEmpty(self.absolute_path)
            if self.parent is None:
                super().delete()
                return
            parent = self.parent
            keep_parent = (
                parent.parent is not None
                and not parent.find_files()
                and len(parent.find_dirs()) == 1
            )
            self._ghc.removing_directory = self
            try:
                path = self._github_path
                await self._ghc.write(
                    self._commit_empty_directory_removal, path, keep_parent
                )
                prefix = path + "/"
                self._ghc.pending_drops.difference_update(
                    {p for p in self._ghc.pending_drops if p.startswith(prefix)}
                )
                if not self._ghc.pending_drops:
                    self._ghc.pending_drops_since = 0.0
                super().delete()
                self._removed = True
            finally:
                self._ghc.removing_directory = None

    def _require_not_removing(self) -> None:
        if self._removed:
            raise FileOrDirectoryDoesNotExist(self.absolute_path)
        if self._ghc.removing_directory is self:
            raise TechnicalError("The directory is being removed; retry the operation")

    def _require_no_removing_descendant(self) -> None:
        active = self._ghc.removing_directory
        if active is not None and (self is active or self.is_ancestor_of(active)):
            raise TechnicalError("A directory is being removed; retry the operation")

    def attach_file_ref(self, name: str, fd_message_id: int) -> TGFSFileRef:
        self._require_not_removing()
        return super().attach_file_ref(name, fd_message_id)

    def _commit_empty_directory_removal(self, path: str, keep_parent: bool) -> None:
        delete_subtree = self._ghc.delete_subtree
        if delete_subtree is not None:
            delete_subtree(path, keep_parent)
            return
        repo = self._ghc.repo
        ref = repo.get_git_ref(f"heads/{self._ghc.commit}")
        base = repo.get_git_commit(ref.object.sha)
        # Walk only the ancestors in this immutable Git tree. One subtree
        # deletion removes stale file refs and stats without listing them.
        current = base.tree.sha
        parts = path.split("/")
        for name in parts:
            tree = repo.get_git_tree(current)
            if tree.truncated:
                raise TechnicalError("Cannot safely remove from a truncated Git tree")
            entry = next((e for e in tree.tree if e.path == name), None)
            if entry is None:
                return
            if entry.type != "tree":
                raise TechnicalError(f"Expected a Git directory at {path}")
            current = entry.sha
        elements = [InputGitTreeElement(path, "040000", "tree", sha=None)]
        if keep_parent and not any(e.path == ".gitkeep" for e in tree.tree):
            marker = self.join_path(*parts[:-1], ".gitkeep")
            elements.append(InputGitTreeElement(marker, "100644", "blob", content=""))
        new_tree = repo.create_git_tree(elements, base_tree=base.tree)
        commit = repo.create_git_commit(f"Delete directory {path}", new_tree, [base])
        # No force: a writer that changed the head since the scoped read
        # wins, rather than having its changes erased by this rmdir.
        ref.edit(commit.sha)

    def move_to(
        self, new_parent: TGFSDirectory, new_name: Optional[str] = None
    ) -> None:
        self._require_no_removing_descendant()
        if isinstance(new_parent, GithubDirectory):
            new_parent._require_not_removing()
        old_parent, old_name = self.parent, self.name
        old_prefix = self._github_path
        super().move_to(new_parent, new_name)
        new_prefix = self._github_path
        if not old_prefix or not new_prefix or old_prefix == new_prefix:
            return
        try:
            self._move_github_directory(old_prefix, new_prefix)
        except Exception:
            # The repo *is* the metadata here, so an in-memory tree that the
            # repo does not back would only survive until the next reload.
            # Put the directory back where it was instead.
            if old_parent is not None and self.parent is not None:
                self.parent.children.remove(self)
                self.name = old_name
                self.parent = old_parent
                old_parent.children.append(self)
            raise

    def _move_github_directory(self, old_prefix: str, new_prefix: str) -> None:
        """Re-path this directory's subtree in a commit that names only the
        moved entries.

        The Contents API has no rename, and deleting plus re-creating every
        blob would lose the git history the timestamps are restored from, so
        the affected blobs are re-homed: an overlay commit that removes each
        old path (``sha=None``) and re-adds it, same blob sha, under the new
        prefix. The blobs the move does not touch stay where they are (the
        commit overlays the branch's current tree) -- the whole-tree rewrite
        a single request used to carry refused with "your input was too
        large" once the repository outgrew it.
        """
        try:
            base_commit = self._ghc.repo.get_git_commit(
                self._ghc.repo.get_git_ref(f"heads/{self._ghc.commit}").object.sha
            )
            tree = self._ghc.repo.get_git_tree(base_commit.tree.sha, recursive=True)

            overlay: list[InputGitTreeElement] = []
            moved = 0
            for entry in tree.tree:
                if entry.type != "blob":
                    continue
                path = entry.path
                if path == old_prefix or path.startswith(old_prefix + "/"):
                    overlay.append(
                        InputGitTreeElement(
                            path=path, mode="100644", type="blob", sha=None
                        )
                    )
                    overlay.append(
                        InputGitTreeElement(
                            path=new_prefix + path[len(old_prefix) :],
                            mode=entry.mode,
                            type="blob",
                            sha=entry.sha,
                        )
                    )
                    moved += 1

            if moved == 0:
                return

            commit_tree_overlay(
                self._ghc.repo,
                self._ghc.commit,
                overlay,
                f"Move directory {old_prefix} to {new_prefix}",
            )
            logger.info(
                f"Moved {moved} object(s) from {old_prefix} to {new_prefix} "
                f"in {self._ghc.repo_name}"
            )
        except Exception as ex:
            logger.error(
                f"Failed to move directory {old_prefix} to {new_prefix} "
                f"in {self._ghc.repo_name}: {ex}"
            )
            raise

    def create_file_ref(self, name: str, file_message_id: int) -> TGFSFileRef:
        file_ref = super().create_file_ref(name, file_message_id)
        try:
            self._put_file_ref(name, file_message_id)
        except Exception as ex:
            logger.error(
                f"Failed to create file reference {name} in "
                f"{self._ghc.repo_name}: {ex}"
            )
            self.files.remove(file_ref)
            raise
        return file_ref

    async def create_file_ref_async(
        self, name: str, file_message_id: int
    ) -> TGFSFileRef:
        file_ref = super().create_file_ref(name, file_message_id)
        committed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._ghc.schedule_ref(
            _PendingRef(self, name, file_message_id, file_ref, committed=committed)
        )
        # The shared worker completes the write even if the client goes
        # away during the commit. By this point the content already exists.
        done, _ = await asyncio.wait({committed}, timeout=AWAITED_COMMIT_MAX_WAIT)
        if not done:
            # GitHub is too slow to answer the commit. Answer the client
            # anyway: holding the reply past what it waits for gets the
            # connection killed and the upload aborted mid-bulk. The
            # reference stays in the tree and the worker keeps retrying,
            # so a retransmission appends a version to the same file.
            committed.cancel()
            raise TechnicalError(
                f"Timed out after {AWAITED_COMMIT_MAX_WAIT:g}s waiting for "
                f"the file reference of {name} to be committed to "
                f"{self._ghc.repo_name}; the content is stored and the "
                f"commit retries in the background"
            )
        committed.result()
        return file_ref

    async def create_file_ref_deferred(
        self, name: str, file_message_id: int
    ) -> TGFSFileRef:
        """Create the reference now, write the blob in the background.

        The in-memory part runs right away so the name shows up in a
        listing and a following upload finds it; only the GitHub blob
        write is deferred, so a GitHub outage cannot hold an SFTP open --
        and with it the client's transfer -- hostage. A write that keeps
        failing is treated like a failing ``create_file_ref_async``: the
        reference leaves the tree and the error is logged.

        The blob writes are batched: the creates scheduled while one write
        runs land together in a tree commit, like the batched deletions
        and the mirror sync do. One commit per file would queue whole
        folders of creates behind the same lock during a bulk upload,
        until the metadata push of the open itself waited too long.
        """
        file_ref = super().create_file_ref(name, file_message_id)
        entry = _PendingRef(
            directory=self,
            name=name,
            file_message_id=file_message_id,
            file_ref=file_ref,
        )
        self._ghc.schedule_ref(entry)
        if self._ghc.pending_refs_flusher is not None:
            self._deferred_refs.add(self._ghc.pending_refs_flusher)
        return file_ref

    def _ref_storage_path(self, name: str, file_message_id: int) -> str:
        return self.join_path(
            self._github_path, self._file_ref_storage_name(name, file_message_id)
        )

    def _put_file_ref(self, name: str, file_message_id: int) -> None:
        """Create the file reference blob in the repo (filename encrypted
        when keyed). Blocking; raises when GitHub refuses."""
        file_path = self._ref_storage_path(name, file_message_id)
        for attempt in range(PUT_ATTEMPTS):
            try:
                self._ghc.repo.create_file(
                    path=file_path,
                    message=f"Create file reference for {self._message_name(name)}",
                    content="",
                    branch=self._ghc.commit,
                )
                logger.info(
                    f"Created file reference {name} in {self._ghc.repo_name} "
                    f"at {file_path}"
                )
                return
            except Exception as ex:
                if _is_sha_missing(ex) or (
                    attempt > 0 and getattr(ex, "status", None) == 422
                ):
                    # GitHub only sends "sha wasn't supplied" when the blob
                    # is already there: an earlier attempt got through
                    # although its answer was lost. After an earlier
                    # attempt, any 422 means the same -- the first attempt
                    # was transient (otherwise it would have raised), so
                    # this one is not a fresh validation error.
                    logger.info(
                        f"Created file reference {name} in {self._ghc.repo_name} "
                        f"at {file_path} (an earlier attempt landed)"
                    )
                    return
                if attempt == PUT_ATTEMPTS - 1 or not _is_transient(ex):
                    raise
                logger.warning(
                    f"Creating file reference {name} failed ({ex}); trying again"
                )
                time.sleep(PUT_RETRY_DELAY * (attempt + 1))

    def delete_file_ref(self, fr: TGFSFileRef) -> None:
        self._drop_file_ref(fr)
        super().delete_file_ref(fr)

    async def delete_file_ref_async(self, fr: TGFSFileRef) -> None:
        # Under the lock, so a deferred create that is still retrying
        # cannot re-create this blob after the drop (its in-tree check
        # only helps if the removal happened before that check).
        async with self._ghc.lock:
            paths = self._drop_candidate_paths(fr)
            if paths:
                self._ghc.pending_drops.update(paths)
                if not self._ghc.pending_drops_since:
                    self._ghc.pending_drops_since = time.monotonic()
            super().delete_file_ref(fr)

    def _drop_candidate_paths(self, fr: TGFSFileRef) -> Set[str]:
        """The repo paths where ``fr``'s blob may live: the same
        candidates ``_drop_file_ref`` deletes, encrypted first."""
        candidates = []
        if self._ghc.name_key is not None:
            candidates.append(self._file_ref_storage_name(fr.name, fr.message_id))
        candidates.append(f"{fr.name}.{fr.message_id}")
        return {
            self.join_path(self._github_path, segment)
            for segment in dict.fromkeys(candidates)
        }

    def _drop_file_ref(self, fr: TGFSFileRef) -> None:
        """Remove the file reference blob from the repo. Blocking; a blob
        that cannot be found is logged, not raised."""
        # The ref may be stored under its encrypted or its legacy-plaintext
        # name (mixed repos), so try the encrypted form first, then plaintext.
        candidates = []
        if self._ghc.name_key is not None:
            candidates.append(self._file_ref_storage_name(fr.name, fr.message_id))
        candidates.append(f"{fr.name}.{fr.message_id}")

        for segment in dict.fromkeys(candidates):  # de-dup, keep order
            file_path = self.join_path(self._github_path, segment)
            try:
                file_content = self._ghc.repo.get_contents(
                    file_path, ref=self._ghc.commit
                )
                if isinstance(file_content, list):
                    file_content = file_content[0]
                # The name as this segment stores it: encrypted, or the
                # legacy plaintext the path already shows.
                stored_name = segment.rsplit(".", 1)[0]
                self._ghc.repo.delete_file(
                    path=file_path,
                    message=f"Delete file reference for {stored_name}",
                    sha=file_content.sha,
                    branch=self._ghc.commit,
                )
                logger.info(
                    f"Deleted file reference {fr.name} from {self._ghc.repo_name}"
                )
                break
            except Exception as ex:
                logger.debug(
                    f"Could not delete {file_path} from {self._ghc.repo_name}: {ex}"
                )
        else:
            logger.error(
                f"Failed to delete file reference {fr.name} "
                f"from {self._ghc.repo_name}: not found"
            )

    def _delete_github_directory(self) -> None:
        """Remove this directory and everything beneath it in a commit
        that names only the removed paths.

        ``delete_file`` can only remove a single file — it returns HTTP 422
        on a subtree — so a per-entry delete cannot recurse into
        subdirectories (their nested ``.gitkeep`` and files would survive).
        The subtree's paths are removed by an overlay commit whose element
        carries ``sha=None``, dropping arbitrarily-nested content while
        everything outside the prefix stays untouched; one that took the
        whole tree as its elements refused with "your input was too
        large" once the repository outgrew a single request.
        """
        prefix = self._github_path
        if not prefix:
            return  # guard: never wipe the whole repo via the root
        try:
            base_commit = self._ghc.repo.get_git_commit(
                self._ghc.repo.get_git_ref(f"heads/{self._ghc.commit}").object.sha
            )
            tree = self._ghc.repo.get_git_tree(base_commit.tree.sha, recursive=True)

            removed = [
                entry.path
                for entry in tree.tree
                if entry.type == "blob"
                and (entry.path == prefix or entry.path.startswith(prefix + "/"))
            ]
            if not removed:
                return

            overlay = [
                InputGitTreeElement(path=path, mode="100644", type="blob", sha=None)
                for path in removed
            ]
            commit_tree_overlay(
                self._ghc.repo,
                self._ghc.commit,
                overlay,
                f"Delete directory {prefix}",
            )
            logger.info(
                f"Deleted {len(removed)} object(s) under {prefix} "
                f"from {self._ghc.repo_name}"
            )
        except Exception as ex:
            logger.error(
                f"Failed to delete directory {prefix} "
                f"from {self._ghc.repo_name}: {ex}"
            )
