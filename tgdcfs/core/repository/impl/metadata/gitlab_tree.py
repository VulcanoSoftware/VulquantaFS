"""A PyGithub-shaped window on a GitLab repository.

The tree-mirror metadata store (``GithubRepoMetadataRepository``) speaks
PyGithub: branches, commits, recursive trees, tree rewrites. GitLab's
REST API shapes the same git concepts differently -- a commit is posted
as a batch of file actions instead of tree plus commit plus ref update,
listings paginate where GitHub truncates, and a tree object id is not a
valid ``ref``. This facade translates: it implements exactly the subset
of PyGithub's ``Repository`` the metadata store uses, backed by GitLab
REST v4, and raises PyGithub's ``GithubException`` so every retry,
fallback and error check of the store keeps working unchanged.

All methods are blocking and run on the store's writer thread (or a
``to_thread`` read), like their PyGithub counterparts.
"""

import asyncio
import base64
import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, cast
from urllib.parse import quote

import requests
from github import GithubException, InputGitTreeElement
from github.Repository import Repository

from tgdcfs.config import GithubRepoConfig, GitlabRepoConfig
from tgdcfs.core.model import TGFSMetadata
from tgdcfs.core.repository.impl.metadata.breaker import (
    FailFastRepository,
    StoreCircuitBreaker,
    StoreUnreachable,
)
from tgdcfs.core.repository.impl.metadata.github_repo import (
    GithubRepoMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.github_repo.dir_timestamps import (
    STATS_COMMIT_SCAN_LIMIT,
)
from tgdcfs.crypto.metadata_blob import decrypt_metadata_blob
from tgdcfs.errors import NoStoredMetadata, TechnicalError

logger = logging.getLogger(__name__)


def _blob_sha(data: bytes) -> str:
    # Git's object id, so a sha the cache folds back in agrees with what a
    # tree listing reports for the same content.
    return hashlib.sha1(
        b"blob %d\0" % len(data) + data, usedforsecurity=False
    ).hexdigest()


# One listing page; the facade always follows the whole pagination, so a
# load never silently misses the tail of a big repository the way a
# truncated GitHub tree would.
PER_PAGE = 100
REQUEST_TIMEOUT_SECONDS = 60.0


# GitLab answers a request its storage backend (Gitaly) could not complete
# with a 4xx/5xx whose body carries the gRPC termination text. Such a call
# kept the connection waiting for its whole request timeout and did no
# work: it is an outage of the storage, not a refused request, and the
# callers pay the same wait again on every listing, write and heal until
# something notices. Raising :class:`StoreUnreachable` for it lets the
# circuit breaker cut that off.
GITALY_DEADLINE_MARKERS = ("Stream removed", "Deadline Exceeded")

# The most actions one GitLab commit may carry. A GitLab commit is one
# storage operation for every action it holds, so the full-tree sync of a
# large file system has to arrive as several ordinary commits, or GitLab
# kills the operation on its own deadline.
GITLAB_COMMIT_ACTION_LIMIT = 500

# GitLab answers 429 when the access token runs into its usage quota. A
# full-tree sync fires many requests in a burst and one 429 used to fail
# the whole store write, so the request is retried: for the wait the API
# prescribes (``Retry-After``), a doubling backoff when it prescribes
# none, and then the failure stands for the stale/heal machinery.
RATE_LIMIT_RETRIES = 5
RATE_LIMIT_MAX_SLEEP_SECONDS = 30.0

# How deep a history squash pages for its anchor (the oldest commit it
# can force the branch onto): 1000 pages of PER_PAGE. Reaching the cap
# still shortens the history; the next compaction reads on from there.
MAX_HISTORY_PAGES = 1000


def _is_gitaly_deadline(data: dict) -> bool:
    text = str(data)
    return any(marker in text for marker in GITALY_DEADLINE_MARKERS)


class _TreeEntry(NamedTuple):
    path: str
    type: str
    sha: str
    mode: str


class _Tree:
    def __init__(self, entries: Sequence[_TreeEntry]):
        self.truncated = False
        self.tree = list(entries)


class _GitRefObject:
    def __init__(self, sha: str):
        self.sha = sha


class _GitRef:
    def __init__(self, facade: "GitlabRepository", branch: str, sha: str):
        self._facade = facade
        self._branch = branch
        self.object = _GitRefObject(sha)

    def edit(self, sha: str, force: bool = False) -> None:
        self._facade._verify_branch_head(self._branch, sha)


class _GitTreeObject:
    def __init__(self, sha: str):
        self.sha = sha


class _Commit:
    def __init__(self, sha: str, committed_date: Optional[str] = None):
        self.sha = sha
        # GitLab exposes no tree ids; the tree API accepts the commit sha
        # as its ref, so the commit sha stands in for the tree sha.
        self.tree = _GitTreeObject(sha)
        self.committer = type("_Committer", (), {"date": _parse_date(committed_date)})()


class _CommitMeta:
    def __init__(self, data: dict):
        self.message = data.get("message", "")
        self.committer = type(
            "_Committer", (), {"date": _parse_date(data.get("committed_date"))}
        )()


class _CommitInfo:
    def __init__(self, data: dict):
        self.sha = data.get("id", "")
        self.commit = _CommitMeta(data)


class _Blob:
    def __init__(self, content: str):
        self.content = content


class _Branch:
    def __init__(self, sha: str):
        self.commit = _GitRefObject(sha)


class _DiffFile:
    def __init__(self, filename: str, previous_filename: Optional[str]):
        self.filename = filename
        self.previous_filename = previous_filename


class _Comparison:
    def __init__(self, files: List[_DiffFile]):
        self.files = files


class _ContentFile:
    def __init__(self, type: str, name: str, path: str, sha: str):
        self.type = type
        self.name = name
        self.path = path
        self.sha = sha


class _PendingTree:
    """The actions ``create_git_tree`` derived; applied by the next
    ``create_git_commit`` on the same (single writer) thread."""

    def __init__(self, actions: List[dict]):
        self.actions = actions
        self.sha = ""


def _parse_date(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _error_data(response: requests.Response) -> dict:
    try:
        data = response.json()
        return data if isinstance(data, dict) else {"message": str(data)}
    except Exception:
        return {"message": response.text[:500]}


class GitlabRepository:
    """The subset of PyGithub's ``Repository`` the tree-mirror metadata
    store uses, served from the GitLab REST API."""

    def __init__(self, config: GitlabRepoConfig):
        self._base_url = config.base_url.rstrip("/")
        self._project = quote(config.repo, safe="")
        self._branch = config.branch
        self._token = config.access_token
        self._blob_fetch_concurrency = config.blob_fetch_concurrency
        self._local = threading.local()
        # The moment (clock monotonic) until no request of this project may
        # be sent, because a 429 prescribed a wait. Every caller -- the
        # retries of its own request and any parallel worker of a listing
        # pool -- holds off until then: at one shared limiter, requests that
        # kept firing their separate wait budgets simultaneously were what
        # turned one rate limit into a self-sustaining storm of them.
        self._paused_until = 0.0
        # Tree object ids seen in listings, mapped to their path: GitLab's
        # tree API takes a commit-ish ref plus a path, never a tree id,
        # while the store walks trees by the shas a parent listing showed.
        self._tree_paths: Dict[str, str] = {}
        # The branch's blobs, walked while the head was the sha cached
        # beside them: the diff of a commit (``create_git_tree``) reads
        # the whole branch to distinguish its elements from everything
        # already there, and without the cache every overlay commit --
        # each file drop, each stats commit, every sync chunk -- paid
        # hundreds of paginated listing requests for it, rate-limited
        # waits on top. Writes that land through this repository fold
        # their actions back in; any sign of a write from elsewhere
        # invalidates the cache (one re-read) silently.
        self._cached_head: Optional[str] = None
        self._cached_blobs: Optional[Dict[str, _TreeEntry]] = None
        # Set when ``get_git_ref`` answered 404: the next commit creates
        # the branch, which needs a start point (see ``_post_commit``).
        self._branch_missing = False

    def _invalidate_tree_cache(self) -> None:
        self._cached_head = None
        self._cached_blobs = None

    def _branch_entries(self) -> Dict[str, _TreeEntry]:
        """The branch's tree entries, keyed by path, served from the cache
        while the head still is the one the cache was walked at.

        A cache that does not match the head re-reads once -- paginated,
        like every listing -- and re-caches; callers translate a 404 (the
        branch is not there yet) to their own blank-page handling.
        """
        head = self._branch_head()
        if self._cached_head == head and self._cached_blobs is not None:
            return self._cached_blobs
        items = self._paginated(
            "/repository/tree", {"ref": self._branch, "recursive": "true"}
        )
        entries = [
            _TreeEntry(
                path=item["path"],
                type=item["type"],
                sha=item["id"],
                mode=item.get("mode", "100644"),
            )
            for item in items
        ]
        for entry in entries:
            if entry.type == "tree":
                self._tree_paths[entry.sha] = entry.path
        self._cached_blobs = {entry.path: entry for entry in entries}
        self._cached_head = head
        return self._cached_blobs

    def _cache_actions(self, actions: List[dict], head: str) -> None:
        """Fold a commit that just landed through ``_post_commit`` into the
        branch cache, and record the head it left the branch at."""
        if self._cached_blobs is None or self._branch_missing:
            return
        for action in actions:
            path = cast(str, action.get("file_path"))
            if action.get("action") == "delete":
                self._cached_blobs.pop(path, None)
                continue
            content = cast(str, action.get("content", ""))
            if action.get("encoding") == "text":
                data = content.encode("utf-8")
            else:
                data = base64.b64decode(content)
            self._cached_blobs[path] = _TreeEntry(
                path=path, type="blob", sha=_blob_sha(data), mode="100644"
            )
        self._cached_head = head

    def _cache_file_write(self, path: str, data: bytes, deleted: bool) -> None:
        """Fold a single-path files-API write into the branch cache; the
        files API answers nothing about the head it left behind, so the
        new one is read once."""
        if self._cached_blobs is None or self._branch_missing:
            return
        if deleted:
            self._cached_blobs.pop(path, None)
        else:
            self._cached_blobs[path] = _TreeEntry(
                path=path, type="blob", sha=_blob_sha(data), mode="100644"
            )
        try:
            self._cached_head = self._branch_head()
        except GithubException:
            self._invalidate_tree_cache()

    # -- transport -----------------------------------------------------------

    @property
    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers["PRIVATE-TOKEN"] = self._token
            self._local.session = session
        return session

    def _rate_limit_wait(self, response: requests.Response, attempt: int) -> float:
        """Seconds to wait before the next try against a 429: the wait
        GitLab prescribes (``Retry-After``) when it says one, a doubling
        backoff otherwise, never beyond ``RATE_LIMIT_MAX_SLEEP_SECONDS``."""
        value = response.headers.get("Retry-After", "")
        try:
            wait = float(value)
        except (TypeError, ValueError):
            wait = float(2**attempt)
        return min(max(wait, 1.0), RATE_LIMIT_MAX_SLEEP_SECONDS)

    def _hold_off(self) -> None:
        """Sleep until the pause a 429 prescribed has passed, once."""
        remaining = self._paused_until - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    def _publish_pause(self, wait: float) -> None:
        """Make every later request of this project wait out ``wait``
        seconds before it sends, keeping the longest pause asked for."""
        until = time.monotonic() + wait
        if until > self._paused_until:
            self._paused_until = until

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        retry_rate_limit: bool = True,
    ) -> requests.Response:
        url = f"{self._base_url}/api/v4/projects/{self._project}{path}"

        def send() -> requests.Response:
            return self._session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

        attempts = RATE_LIMIT_RETRIES if retry_rate_limit else 1
        self._hold_off()
        response = send()
        for attempt in range(attempts - 1):
            if response.status_code != 429:
                break
            wait = self._rate_limit_wait(response, attempt)
            logger.warning(
                f"GitLab rate limit exceeded on {method} {path} "
                f"(attempt {attempt + 1} of {attempts}); "
                f"retrying in {wait:.0f}s"
            )
            # Every other call of this project holds off too: a wave of
            # requests that each burned its own wait budget against the
            # same limiter kept the limit from ever clearing.
            self._publish_pause(wait)
            time.sleep(wait)
            response = send()
        if response.status_code >= 400:
            data = _error_data(response)
            if _is_gitaly_deadline(data):
                raise StoreUnreachable(
                    f"GitLab's storage did not complete the request "
                    f"({data.get('message', data)})"
                )
            raise GithubException(response.status_code, data, dict(response.headers))
        return response

    def _json(self, response: requests.Response) -> Any:
        if not response.content:
            return {}
        return response.json()

    def probe(self) -> None:
        """A cheap read that rounds the same storage backend the writes
        have to traverse: one page of the branch's commit history. A
        missing branch answers 404, which means GitLab itself spoke --
        whatever the probe raises beyond a transport- or deadline-level
        failure counts as the service being up."""
        self._request(
            "GET",
            "/repository/commits",
            params={"ref_name": self._branch, "per_page": 1},
        )

    def _paginated(self, path: str, params: dict) -> List[dict]:
        response = self._request(
            "GET", path, params=dict(params, page=1, per_page=PER_PAGE)
        )
        first = self._json(response)
        items: List[dict] = first if isinstance(first, list) else []
        next_page = response.headers.get("X-Next-Page", "").strip()
        if not next_page or len(items) < PER_PAGE:
            return items
        total_pages = response.headers.get("X-Total-Pages", "").strip()
        if (
            total_pages.isdigit()
            and 1 < int(total_pages) <= MAX_HISTORY_PAGES
            and self._blob_fetch_concurrency > 1
        ):
            # The rest of the listing is fetched concurrently: the
            # recursive listing of a grown metadata repository paginates
            # into hundreds of GETs, and at one page per round trip it
            # lined the whole boot behind a quarter hour of them. GitLab
            # reports the total page count, so every page can be asked
            # for at once; each request still waits out a 429 like any
            # other, and a rate limiter makes the pool crawl instead of
            # fail.
            pages = range(2, int(total_pages) + 1)
            workers = min(self._blob_fetch_concurrency, len(pages))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                fetched = pool.map(
                    lambda page: self._json(
                        self._request(
                            "GET",
                            path,
                            params=dict(params, page=page, per_page=PER_PAGE),
                        )
                    ),
                    pages,
                )
                items.extend(
                    item
                    for batch in fetched
                    if isinstance(batch, list)
                    for item in batch
                )
            return items
        page = int(next_page)
        while True:
            response = self._request(
                "GET", path, params=dict(params, page=page, per_page=PER_PAGE)
            )
            batch = self._json(response)
            if not isinstance(batch, list):
                return items if items else []
            items.extend(batch)
            next_page = response.headers.get("X-Next-Page", "").strip()
            if not next_page or len(batch) < PER_PAGE:
                return items
            page = int(next_page)

    # -- project and branch state ---------------------------------------------

    @property
    def full_name(self) -> str:
        return self._project

    def _project_data(self) -> dict:
        return self._json(self._request("GET", ""))

    @property
    def created_at(self) -> Optional[datetime]:
        return _parse_date(self._project_data().get("created_at"))

    @property
    def pushed_at(self) -> Optional[datetime]:
        # GitLab has no pushed_at; last activity is the closest signal.
        return _parse_date(self._project_data().get("last_activity_at"))

    @property
    def updated_at(self) -> Optional[datetime]:
        return _parse_date(self._project_data().get("last_activity_at"))

    def _branch_head(self, branch: Optional[str] = None) -> str:
        name = branch or self._branch
        data = self._json(
            self._request("GET", f"/repository/branches/{quote(name, safe='')}")
        )
        return data["commit"]["id"]

    def get_branch(self, branch: str) -> _Branch:
        return _Branch(self._branch_head(branch))

    def get_git_ref(self, ref: str) -> _GitRef:
        branch = ref
        if branch.startswith("heads/"):
            branch = branch[len("heads/") :]
        try:
            sha = self._branch_head(branch)
        except GithubException as ex:
            if ex.status == 404 and branch == self._branch:
                self._branch_missing = True
            raise
        self._branch_missing = False
        return _GitRef(self, branch, sha)

    def _verify_branch_head(self, branch: str, sha: str) -> None:
        # The commit that was supposed to move the ref already landed (the
        # commits API commits straight onto the branch); a head that does
        # not match means another writer raced in between, which the
        # callers treat exactly like GitHub's non-fast-forward refusal.
        head = self._branch_head(branch)
        if head != sha:
            if branch == self._branch:
                # The write that moved the head from under this ref
                # invalidated the cache's claim on it.
                self._invalidate_tree_cache()
            raise GithubException(422, {"message": "Update is not a fast forward"})

    def create_git_ref(self, ref: str, sha: str) -> None:
        if not sha:
            # An empty sync on a completely empty project: there is no
            # branch to create yet, and the first real write initializes
            # the repository (see ``_bootstrap_branch``).
            return
        branch = ref
        if branch.startswith("refs/heads/"):
            branch = branch[len("refs/heads/") :]
        try:
            self._verify_branch_head(branch, sha)
            return
        except GithubException as ex:
            if ex.status != 422:
                raise
        self._request(
            "POST", "/repository/branches", json_body={"branch": branch, "ref": sha}
        )

    # -- reads -----------------------------------------------------------------

    def get_git_commit(self, sha: str) -> _Commit:
        data = self._json(
            self._request("GET", f"/repository/commits/{quote(sha, safe='')}")
        )
        return _Commit(sha, data.get("committed_date"))

    def get_git_tree(self, sha: str, recursive: bool = False) -> _Tree:
        if sha == self._branch or (
            self._cached_blobs is not None and sha == self._cached_head
        ):
            if sha == self._branch:
                # A listing of the branch; the cache is revalidated
                # against the head with a single request, since a write
                # from outside this process would make it stale.
                try:
                    cached = self._branch_entries()
                except GithubException as ex:
                    if ex.status != 404:
                        raise
                    # The branch does not exist yet, which the next
                    # write has to bootstrap.
                    self._branch_missing = True
                    raise
            else:
                cached = cast(Dict[str, _TreeEntry], self._cached_blobs)
            if not recursive:
                return _Tree(
                    [entry for entry in cached.values() if "/" not in entry.path]
                )
            return _Tree(list(cached.values()))
        # A tree id the facade itself listed maps back to its path; a
        # commit sha or branch name goes through as the ref directly.
        params: Dict[str, Any] = {"ref": sha}
        path = self._tree_paths.get(sha)
        if path:
            params["ref"] = self._branch
            params["path"] = path
        if recursive:
            params["recursive"] = "true"
        try:
            items = self._paginated("/repository/tree", params)
        except GithubException as ex:
            if ex.status == 404 and sha == self._branch:
                # A listing of the branch itself: the branch does not
                # exist yet, which the next write has to bootstrap.
                self._branch_missing = True
            raise
        entries = [
            _TreeEntry(
                path=item["path"],
                type=item["type"],
                sha=item["id"],
                mode=item.get("mode", "100644"),
            )
            for item in items
        ]
        for entry in entries:
            if entry.type == "tree":
                self._tree_paths[entry.sha] = entry.path
        return _Tree(entries)

    def get_git_blob(self, sha: str) -> _Blob:
        data = self._json(
            self._request("GET", f"/repository/blobs/{quote(sha, safe='')}")
        )
        return _Blob(data.get("content", ""))

    def get_contents(self, path: str, ref: Optional[str] = None) -> Any:
        ref = ref or self._branch
        path = path.strip("/")
        if path:
            try:
                data = self._json(
                    self._request(
                        "GET",
                        f"/repository/files/{quote(path, safe='')}",
                        params={"ref": ref},
                    )
                )
                return _ContentFile(
                    type="file",
                    name=path.rsplit("/", 1)[-1],
                    path=path,
                    sha=data.get("blob_id", ""),
                )
            except GithubException as ex:
                if ex.status != 404:
                    raise
        entries = [
            _ContentFile(
                type="dir" if item["type"] == "tree" else "file",
                name=item["path"].rsplit("/", 1)[-1],
                path=item["path"],
                sha=item["id"],
            )
            for item in self._paginated(
                "/repository/tree", {"ref": ref, **({"path": path} if path else {})}
            )
        ]
        if path and not entries:
            # A git tree has no empty directories: nothing under the path
            # means the path does not exist, like GitHub's 404.
            raise GithubException(404, {"message": "not found"})
        return entries

    def get_file_text(self, path: str, ref: Optional[str] = None) -> Optional[str]:
        """The text content of one file, or ``None`` when it does not exist.

        Used by the tree store to read (and so migrate) the single-document
        layout an older ``gitlab_repo`` configuration left behind."""
        path = path.strip("/")
        try:
            data = self._json(
                self._request(
                    "GET",
                    f"/repository/files/{quote(path, safe='')}",
                    params={"ref": ref or self._branch},
                )
            )
        except GithubException as ex:
            if ex.status == 404:
                return None
            raise
        return base64.b64decode(data.get("content", "")).decode()

    def get_commits(
        self, sha: Optional[str] = None, path: Optional[str] = None
    ) -> List[_CommitInfo]:
        params: Dict[str, Any] = {"ref_name": sha or self._branch}
        if path:
            params["path"] = path
        # The consumer scans at most STATS_COMMIT_SCAN_LIMIT entries for
        # the last non-stats commit; never pull more history than that.
        items = self._paginated("/repository/commits", params)
        return [_CommitInfo(item) for item in items[: STATS_COMMIT_SCAN_LIMIT + 1]]

    def compare(self, base: str, head: str) -> _Comparison:
        data = self._json(
            self._request(
                "GET", "/repository/compare", params={"from": base, "to": head}
            )
        )
        files = [
            _DiffFile(
                filename=diff.get("new_path", ""),
                previous_filename=(
                    diff.get("old_path")
                    if diff.get("renamed_file") and diff.get("old_path")
                    else None
                ),
            )
            for diff in data.get("diffs", [])
        ]
        return _Comparison(files)

    # -- writes ------------------------------------------------------------------

    def create_file(
        self,
        path: str,
        message: str,
        content: str,
        branch: str,
        **_kwargs: Any,
    ) -> None:
        body = {
            "branch": branch,
            "content": content,
            "commit_message": message,
            "encoding": "text",
        }
        if self._branch_missing:
            # The branch does not exist yet; the write bootstraps it the
            # same way a commit does (see ``_post_commit``).
            actions = [
                {
                    "action": "create",
                    "file_path": path,
                    "content": content,
                    "encoding": "text",
                }
            ]
            self._bootstrap_branch(
                {"branch": branch, "commit_message": message, "actions": actions}
            )
            if not actions:
                # An empty project: writing the file initialized it.
                return
        try:
            self._request(
                "POST", f"/repository/files/{quote(path, safe='')}", json_body=body
            )
        except GithubException as ex:
            if ex.status == 400 and "already exist" in str(ex.data).lower():
                # GitHub refuses a create of an existing blob with the 422
                # below; the store reads it as "an earlier attempt landed".
                raise GithubException(
                    422, {"message": '"sha" wasn\'t supplied'}
                ) from ex
            raise
        self._cache_file_write(path, content.encode("utf-8"), deleted=False)

    def delete_file(
        self, path: str, message: str, sha: str, branch: str, **_kwargs: Any
    ) -> None:
        self._request(
            "DELETE",
            f"/repository/files/{quote(path, safe='')}",
            json_body={
                "branch": branch,
                "commit_message": message,
            },
        )
        self._cache_file_write(path, b"", deleted=True)

    def create_git_tree(
        self,
        elements: Sequence[InputGitTreeElement],
        base_tree: Any = None,
    ) -> _PendingTree:
        """Diff the desired tree against the branch's current blobs and
        translate it into GitLab file actions.

        With ``base_tree`` the elements only overlay the branch (the stats
        commits); without it the elements are the whole desired tree and
        everything not listed is deleted (the sync, move and delete
        rewrites).
        """
        desired: Dict[str, tuple] = {}
        for element in elements:
            identity = element._identity
            desired[identity["path"]] = (
                identity.get("sha"),
                identity.get("content"),
            )

        try:
            entries = self._branch_entries()
        except GithubException as ex:
            if ex.status != 404:
                raise
            # The branch does not exist yet (a fresh repository); the
            # desired tree is the whole state, nothing to diff against.
            self._branch_missing = True
            entries = {}
        current = {
            path: entry.sha for path, entry in entries.items() if entry.type == "blob"
        }

        # GitLab commits by content: every blob the branch does not hold
        # yet travels with its action, so the diff has to read those blobs
        # first. A catch-up sync after an outage names thousands of them;
        # one round trip per blob turns that into a chain of them, so they
        # are fetched concurrently and the actions still come out in the
        # deterministic sorted order below.
        needed_shas = {
            sha
            for path, (sha, content) in desired.items()
            if content is None
            and sha is not None
            and (path not in current or current[path] != sha)
        }
        contents = self._fetch_blobs(needed_shas)

        actions: List[dict] = []
        for path, (sha, content) in sorted(desired.items()):
            if content is None and sha is None:
                # "sha": null removes the entry, like GitHub's tree API.
                if path in current:
                    actions.append({"action": "delete", "file_path": path})
            elif content is not None:
                actions.append(
                    {
                        "action": "update" if path in current else "create",
                        "file_path": path,
                        "content": content,
                        "encoding": "text",
                    }
                )
            elif path not in current:
                # A blob the branch does not hold yet: a fresh mirror, or
                # a path a tree rewrite moved here. GitLab commits by
                # content, so the blob's content travels with the action.
                actions.append(
                    {
                        "action": "create",
                        "file_path": path,
                        "content": contents[sha],
                        "encoding": "base64",
                    }
                )
            elif current[path] != sha:
                actions.append(
                    {
                        "action": "update",
                        "file_path": path,
                        "content": contents[sha],
                        "encoding": "base64",
                    }
                )
        if base_tree is None:
            actions.extend(
                {"action": "delete", "file_path": path}
                for path in sorted(current)
                if path not in desired
            )
        return _PendingTree(actions)

    def _fetch_blobs(self, shas: set) -> Dict[str, str]:
        """The base64 content of the named blobs, fetched concurrently.

        The sessions are thread-local (see ``_session``), so one connection
        per worker fetches in parallel; a fetch failure surfaces from the
        result iteration, the same exception a sequential fetch would have
        raised, only earlier in wall time.
        """
        if not shas:
            return {}
        ordered = sorted(shas)
        if len(ordered) == 1 or self._blob_fetch_concurrency <= 1:
            return {sha: self.get_git_blob(sha).content for sha in ordered}
        workers = min(self._blob_fetch_concurrency, len(ordered))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return {
                sha: content
                for sha, content in zip(
                    ordered,
                    pool.map(lambda sha: self.get_git_blob(sha).content, ordered),
                )
            }

    def create_git_commit(
        self, message: str, tree: _PendingTree, parents: Sequence[Any]
    ) -> _Commit:
        actions = tree.actions
        if not actions:
            return _Commit(self._noop_commit())
        chunks = [
            actions[i : i + GITLAB_COMMIT_ACTION_LIMIT]
            for i in range(0, len(actions), GITLAB_COMMIT_ACTION_LIMIT)
        ]
        # A single commit is one storage operation for all of its actions:
        # GitLab's backend gives the batches of a full-tree sync up early
        # ("4:Stream removed (Deadline Exceeded)"), long before the
        # request itself times out. Land the tree as ordinary, capped
        # commits instead. The diff never names a path twice, so every
        # chunk applies cleanly on the one before it, and a chunk that
        # does not land leaves the store stale for the next full sync,
        # like any failed push.
        total = len(chunks)
        sha = ""
        for number, chunk in enumerate(chunks, start=1):
            chunk_message = f"{message} ({number}/{total})" if total > 1 else message
            sha = self._post_commit(chunk_message, chunk)
            self._cache_actions(chunk, sha)
        tree.sha = sha
        return _Commit(sha)

    def commit_history(self, ref: str, max_items: int) -> List[str]:
        """The shas of the first ``max_items`` commits of ``ref``'s
        history, newest first.

        Paged, so a metadata branch that grows one commit per push can
        still be swept to its root -- the listing a stats commit uses is
        capped far below that.
        """
        shas: List[str] = []
        page = 1
        while len(shas) < max_items:
            response = self._request(
                "GET",
                "/repository/commits",
                params={"ref_name": ref, "page": page, "per_page": PER_PAGE},
            )
            batch = self._json(response)
            if not isinstance(batch, list):
                break
            shas.extend(str(item["id"]) for item in batch)
            next_page = response.headers.get("X-Next-Page", "").strip()
            if not next_page or len(batch) < PER_PAGE or page >= MAX_HISTORY_PAGES:
                break
            page = int(next_page)
        return shas[:max_items]

    def compact_history(self, message: str, max_commits: int) -> int:
        """Bound the branch's history to ``max_commits`` commits.

        GitLab's commits API cannot create a parentless commit, so the
        branch is force-committed onto its oldest ancestor instead, with
        actions that replay its whole current state; the repointing
        orphans everything older, which GitLab's housekeeping collects.
        Returns how many commits the history carried when it was over
        the bound, 0 when the history was within it. The anchor is the
        oldest commit the page cap lets the read reach: usually the
        history's root, otherwise the next compaction shortens from
        there.
        """
        if not max_commits:
            return 0
        try:
            beyond_bound = self.commit_history(self._branch, max_commits + 1)
            if len(beyond_bound) <= max_commits:
                return 0
            # The anchor has to sit below every commit that stays, so the
            # full history pages; the branch of an incremental metadata
            # store holds one commit per write, and only its root ends
            # the chain.
            history = self.commit_history(self._branch, MAX_HISTORY_PAGES * PER_PAGE)
        except GithubException as ex:
            if ex.status == 404:
                self._branch_missing = True
                return 0
            raise
        if len(history) == MAX_HISTORY_PAGES * PER_PAGE:
            logger.warning(
                f"History of {self.full_name}@{self._branch} exceeds the "
                "compaction read cap; bounding it incrementally"
            )
        if len(history) < 2:
            return 0
        anchor = history[-1]
        if anchor == history[0]:
            return 0
        current_tree = self.get_git_tree(self._branch, recursive=True)
        if current_tree.truncated:
            logger.warning(
                f"Tree of {self.full_name}@{self._branch} came back "
                "truncated; the metadata history cannot be squashed safely"
            )
            return 0
        current = {e.path: e.sha for e in current_tree.tree if e.type == "blob"}
        anchor_tree = self.get_git_tree(anchor, recursive=True)
        past = {e.path: e.sha for e in anchor_tree.tree if e.type == "blob"}
        # GitLab commits by content: every blob the anchor's tree does not
        # hold travels with its action, so they are fetched alongside the
        # diff, concurrently like the sync does it.
        needed = {sha for path, sha in current.items() if past.get(path) != sha}
        contents = self._fetch_blobs(needed)
        actions: List[dict] = [
            {"action": "delete", "file_path": path}
            for path in sorted(set(past) - set(current))
        ]
        actions += [
            {
                "action": "update" if path in past else "create",
                "file_path": path,
                "content": contents[sha],
                "encoding": "base64",
            }
            for path, sha in sorted(current.items())
            if past.get(path) != sha
        ]
        if not actions:
            chunks: List[List[dict]] = [[]]
        else:
            chunks = [
                actions[i : i + GITLAB_COMMIT_ACTION_LIMIT]
                for i in range(0, len(actions), GITLAB_COMMIT_ACTION_LIMIT)
            ]
        total = len(chunks)
        sha = ""
        for number, chunk in enumerate(chunks, start=1):
            sha = self._post_commit(
                message if total == 1 else f"{message} ({number}/{total})",
                chunk,
                force=number == 1,
                start_sha=anchor if number == 1 else None,
            )
            self._cache_actions(chunk, sha)
        return len(history)

    def _noop_commit(self) -> str:
        """The head an empty commit leaves the branch at: the current one,
        with a missing branch pointed at the project's default branch so
        the next write has a home. A completely empty project cannot hold
        a branch at all -- there is nothing to sync onto it yet."""
        try:
            return self._branch_head()
        except GithubException as ex:
            if ex.status != 404:
                raise
        default = self._project_data().get("default_branch")
        if not default:
            return ""
        try:
            head = self._branch_head(default)
        except GithubException:
            return ""
        try:
            self._request(
                "POST",
                "/repository/branches",
                json_body={"branch": self._branch, "ref": head},
            )
        except GithubException as ex:
            if ex.status != 400:
                raise
            # The branch came into existence in the meantime.
            head = self._branch_head()
        self._branch_missing = False
        # The cache, if any, described a branch that was not there.
        self._invalidate_tree_cache()
        return head

    def _post_commit(
        self,
        message: str,
        actions: List[dict],
        force: bool = False,
        start_sha: Optional[str] = None,
    ) -> str:
        body: Dict[str, Any] = {
            "branch": self._branch,
            "commit_message": message,
            "actions": actions,
        }
        if force:
            body["force"] = True
            body["start_sha"] = start_sha
        if self._branch_missing:
            # The branch does not exist yet; the commits API only creates
            # it when told where to start from.
            self._bootstrap_branch(body)
            if not body["actions"]:
                # A one-action commit on an empty project: writing the
                # first file already created it.
                self._branch_missing = False
                self._invalidate_tree_cache()
                return self._branch_head()
        try:
            data = self._json(
                self._request("POST", "/repository/commits", json_body=body)
            )
        except GithubException as ex:
            if ex.status != 400:
                raise
            text = str(ex.data).lower()
            if "already exist" in text and any(
                action["action"] == "create" for action in actions
            ):
                # Another writer created one of the paths first; our
                # content is the desired end state either way.
                for action in actions:
                    if action["action"] == "create":
                        action["action"] = "update"
                data = self._json(
                    self._request("POST", "/repository/commits", json_body=body)
                )
            else:
                raise
        self._branch_missing = False
        return data["id"]

    def _bootstrap_branch(self, body: Dict[str, Any]) -> None:
        """Give the commit a branch to land on: from the project's default
        branch, or -- on a completely empty project -- by writing the
        first file, which initializes the repository."""
        default_branch = self._project_data().get("default_branch")
        if default_branch:
            try:
                head = self._branch_head(default_branch)
            except GithubException:
                head = None
            if head:
                try:
                    self._request(
                        "POST",
                        "/repository/branches",
                        json_body={"branch": self._branch, "ref": head},
                    )
                    return
                except GithubException as ex:
                    if ex.status != 400:
                        raise
                # A 400 here means the branch came into existence in the
                # meantime; the commit below lands on it.
        for action in list(body["actions"]):
            if action["action"] not in ("create", "update"):
                continue
            self._request(
                "POST",
                f"/repository/files/{quote(action['file_path'], safe='')}",
                json_body={
                    "branch": self._branch,
                    "content": action["content"],
                    "encoding": action.get("encoding", "text"),
                    "commit_message": body["commit_message"],
                },
            )
            body["actions"].remove(action)
            return


class GitlabTreeRepoMetadataRepository(GithubRepoMetadataRepository):
    """The GitLab repository as a mirrored metadata tree, like ``github_repo``.

    Speaks the PyGithub surface through :class:`GitlabRepository`, so the
    whole tree engine -- incremental writes, the diff sync, the stats
    files, the directory dates -- runs unchanged on GitLab.

    A repository that still holds the older single-document layout (the
    ``gitlab_repo`` store of releases before the layout existed) is read
    through that document until the first push has mirrored the tree; the
    document is then removed, so the migration runs once.
    """

    def __init__(
        self,
        config: GitlabRepoConfig,
        name_key: Optional[bytes] = None,
        blob_key: Optional[bytes] = None,
    ):
        raw = GitlabRepository(config)
        # Like its GitHub sibling, behind a fail-fast wrapper: GitLab that
        # cannot do the work (a hung storage backend, a dead connection)
        # costs the full request timeout per call, and the replicated layer
        # still writes the full tree here on every push -- which used to
        # hold every file operation for that long, over and over, while
        # GitLab was unwell. A deadline answer or a dead connection trips
        # the breaker; a background probe alone waits until GitLab answers
        # again, and the heal worker catches this store up afterwards.
        self._gitlab_breaker = StoreCircuitBreaker(raw.probe)
        self._gitlab = cast(Any, FailFastRepository(raw, self._gitlab_breaker))
        self._blob_key = blob_key
        self._document_path = config.file_path
        # Set once the document is confirmed gone (or never existed), so
        # the cleanup costs one request per process at most.
        self._document_removed = False
        gh_config = GithubRepoConfig(
            repo=config.repo,
            commit=config.branch,
            access_token=config.access_token,
            squash_history_max_commits=config.squash_history_max_commits,
        )
        # The facade implements the subset of PyGithub's Repository the
        # engine uses; the cast records that duck-typed contract.
        super().__init__(
            gh_config,
            name_key=name_key,
            repo=cast(Repository, self._gitlab),
        )

    async def close(self) -> None:
        self._gitlab_breaker.stop()
        await super().close()

    async def head_revision(self) -> Optional[float]:
        """The branch head's commit date, probed without any retrying.

        The replicated layer probes this before it reads anything, and a
        probe that retried its way through a rate limit re-created the
        minutes of 429 waiting the probe exists to avoid: one attempt
        per request either answers, or the store counts as unreachable
        and the layer skips it."""
        try:
            return await asyncio.to_thread(self._head_epoch_once)
        except GithubException as ex:
            if ex.status in (404, 409):
                raise NoStoredMetadata(
                    f"The branch {self._ghc.commit} of {self._ghc.repo_name} "
                    f"does not exist yet"
                ) from ex
            raise

    def _head_epoch_once(self) -> float:
        branch = self._ghc.commit
        facade = self._gitlab
        head = facade._json(
            facade._request(
                "GET",
                f"/repository/branches/{quote(branch, safe='')}",
                retry_rate_limit=False,
            )
        )
        sha = head["commit"]["id"]
        commit = facade._json(
            facade._request(
                "GET",
                f"/repository/commits/{quote(sha, safe='')}",
                retry_rate_limit=False,
            )
        )
        date = _parse_date(commit.get("committed_date"))
        if date is None:
            raise TechnicalError(
                f"The branch head of {self._ghc.repo_name}@{branch} "
                f"carries no commit date"
            )
        return date.timestamp()

    async def get(self) -> TGFSMetadata:
        metadata = await super().get()
        if self._tree_is_empty(metadata):
            document = await self._read_document()
            if document is not None:
                try:
                    text = (
                        decrypt_metadata_blob(self._blob_key, document).decode()
                        if self._blob_key is not None
                        else document
                    )
                    loaded = TGFSMetadata.from_dict(json.loads(text))
                except Exception as ex:
                    raise TechnicalError(
                        f"Could not parse the metadata document of "
                        f"{self._ghc.repo_name}@{self._ghc.commit}:"
                        f"{self._document_path}: {ex}"
                    ) from ex
                logger.info(
                    f"{self._ghc.repo_name}@{self._ghc.commit} holds no "
                    f"metadata tree yet; serving the single-document layout "
                    f"from {self._document_path} until the first push has "
                    f"mirrored the tree"
                )
                # Through the setter, so the document's plain tree becomes
                # this store's own node type and every later mutation keeps
                # writing through incrementally.
                self.metadata = loaded
                return self.metadata
        return metadata

    async def push_full(self) -> None:
        await super().push_full()
        await self._remove_document()

    def _compact_history(self) -> None:
        """The GitLab form of the history squash: the facade force-commits
        the branch's current state onto its oldest ancestor, since the
        commits API cannot create a parentless commit."""
        limit = self._ghc.squash_history_max_commits
        squashed = cast(Any, self._ghc.repo).compact_history(
            "Compact metadata history", limit
        )
        if squashed:
            logger.info(
                f"Compacted the metadata history of {self._ghc.repo_name}@"
                f"{self._ghc.commit}: squashed {squashed} commits"
            )

    @staticmethod
    def _tree_is_empty(metadata: TGFSMetadata) -> bool:
        return not metadata.dir.find_files() and not metadata.dir.find_dirs()

    async def _read_document(self) -> Optional[str]:
        try:
            return await asyncio.to_thread(
                self._gitlab.get_file_text, self._document_path
            )
        except Exception as ex:
            # A document that cannot be read must not shadow a tree that
            # simply is empty; the next push re-syncs the repo anyway.
            logger.warning(
                f"Could not read the metadata document {self._document_path} "
                f"from {self._ghc.repo_name}@{self._ghc.commit}: {ex}"
            )
            return None

    async def _remove_document(self) -> None:
        if self._document_removed:
            return
        self._document_removed = True
        try:
            exists = (
                await asyncio.to_thread(self._gitlab.get_file_text, self._document_path)
            ) is not None
            if not exists:
                return
            await asyncio.to_thread(
                self._gitlab.delete_file,
                self._document_path,
                "Remove the TGDCFS single-document metadata (migrated to the "
                "mirrored tree layout)",
                "",
                self._ghc.commit,
            )
            logger.info(
                f"Removed the migrated metadata document {self._document_path} "
                f"from {self._ghc.repo_name}@{self._ghc.commit}"
            )
        except Exception as ex:
            # The tree is synced; a document that refuses to go away is
            # retried on the next push_full of this process.
            self._document_removed = False
            logger.warning(
                f"Could not remove the migrated metadata document "
                f"{self._document_path} from {self._ghc.repo_name}@"
                f"{self._ghc.commit}: {ex}"
            )
