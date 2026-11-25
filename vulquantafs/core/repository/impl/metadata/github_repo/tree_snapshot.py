"""A local copy of the last tree a metadata store served.

Every boot re-lists the whole metadata tree from the git store: the tree
only changes when the branch moves, so nothing in it is new -- but a grown
repository paginates into hundreds of GETs, and behind a slow or
rate-limited backend (GitLab answering 429 on every wave of them) that
alone used to hold the boot for many minutes. The head commit identifies
the tree's content exactly, so the listing result is persisted beside the
other caches and re-served whenever the next boot sees the same head: the
boot then costs one ref read plus one commit read per store, and reads the
listing only when the branch actually moved (or no snapshot exists yet,
as on the first boot).
"""

import json
import logging
import os
import tempfile
import threading
from typing import Optional

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 1


def snapshot_file_name(repo: str, branch: str) -> str:
    """A file name per (repo, branch) that is safe on every filesystem."""
    slug = "".join(c if c.isalnum() or c in "-_." else "-" for c in f"{repo}-{branch}")
    return f"tree-snapshot-{slug}.json"


class TreeSnapshot:
    """The last listed tree of one (repo, branch), keyed by its head sha.

    Read and written best-effort: a missing, unreadable or stale snapshot
    only costs the full listing the boot would have paid without it.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()

    def load(self, repo_name: str, branch: str, head: str) -> Optional[dict]:
        """The tree dict captured at ``head``, or None when it does not
        match this repo, branch or head."""
        try:
            with open(self._path, encoding="utf-8") as fp:
                data = json.load(fp)
        except FileNotFoundError:
            return None
        except Exception as ex:
            logger.warning(f"Ignoring unreadable tree snapshot {self._path}: {ex}")
            return None

        if not isinstance(data, dict) or data.get("version") != SNAPSHOT_VERSION:
            return None
        if (
            data.get("repo") != repo_name
            or data.get("branch") != branch
            or data.get("head") != head
        ):
            return None
        tree = data.get("tree")
        if not isinstance(tree, dict):
            return None
        logger.info(
            f"Served the metadata tree of {repo_name}@{branch} from the "
            f"local snapshot; the head {head[:8]} is unchanged"
        )
        return tree

    def save(self, repo_name: str, branch: str, head: str, tree: dict) -> None:
        """Persist the tree listing for the next boot; best-effort."""
        payload = {
            "version": SNAPSHOT_VERSION,
            "repo": repo_name,
            "branch": branch,
            "head": head,
            "tree": tree,
        }
        with self._lock:
            try:
                directory = os.path.dirname(self._path) or "."
                os.makedirs(directory, exist_ok=True)
                # Write beside the target and rename, so a crash mid-write
                # leaves the previous snapshot intact instead of a
                # truncated file.
                fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tree-snapshot-")
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as fp:
                        json.dump(payload, fp)
                    os.replace(tmp, self._path)
                except Exception:
                    os.unlink(tmp)
                    raise
            except Exception as ex:
                logger.warning(f"Could not write the tree snapshot {self._path}: {ex}")
