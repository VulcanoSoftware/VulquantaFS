"""Shared fixtures of the metadata store tests."""

import pytest

from vulquantafs.core.repository.impl.metadata.github_repo.tree_snapshot import (
    TreeSnapshot,
)


@pytest.fixture(autouse=True)
def tree_snapshots_in_tmp(tmp_path, monkeypatch):
    """Keep the tree snapshot out of the checkout, and out of the way of
    follow-up tests.

    The test data dir is the repository directory (``VULQUANTAFS_DATA_DIR=.``),
    so a snapshot written during one test would be found -- by head sha --
    by the next one before its fake backend had answered anything. Every
    store here gets one inside this test's scratch dir instead; the store
    tests that exercise the snapshot flow specifically point it at the
    same dir, so a second store within one test shares the cache, like
    two boots of one installation would.
    """
    snapshot_path = tmp_path / "tree-snapshot.json"
    monkeypatch.setattr(
        "vulquantafs.core.repository.impl.metadata.github_repo.TreeSnapshot",
        lambda path: TreeSnapshot(str(snapshot_path)),
    )
