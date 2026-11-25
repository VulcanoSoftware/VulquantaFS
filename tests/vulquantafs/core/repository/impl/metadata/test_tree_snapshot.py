"""The local snapshot of the last tree listing."""

import pytest

from vulquantafs.core.repository.impl.metadata.github_repo.tree_snapshot import (
    TreeSnapshot,
)


@pytest.fixture
def snapshot(tmp_path):
    return TreeSnapshot(str(tmp_path / "tree-snapshot.json"))


TREE = {"type": "TGFSMetadata", "dir": {"name": "root", "files": []}}


def test_a_saved_tree_is_served_for_the_same_head(snapshot):
    snapshot.save("group/project", "main", "abc123", TREE)

    assert snapshot.load("group/project", "main", "abc123") == TREE


def test_a_head_that_moved_is_not_served(snapshot):
    snapshot.save("group/project", "main", "abc123", TREE)

    assert snapshot.load("group/project", "main", "def456") is None


def test_another_repo_or_branch_is_not_served(snapshot):
    snapshot.save("group/project", "main", "abc123", TREE)

    assert snapshot.load("group/other", "main", "abc123") is None
    assert snapshot.load("group/project", "dev", "abc123") is None


import json
import pathlib


def test_a_missing_or_corrupt_file_is_not_served(tmp_path):
    missing = TreeSnapshot(str(tmp_path / "absent.json"))

    assert missing.load("group/project", "main", "abc123") is None

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{broken")
    snapshot = TreeSnapshot(str(corrupt))

    assert snapshot.load("group/project", "main", "abc123") is None


def test_a_file_of_an_unrecognised_version_is_not_served(snapshot):
    snapshot.save("group/project", "main", "abc123", TREE)

    payload = json.loads(pathlib.Path(snapshot._path).read_text())
    payload["version"] = 999
    pathlib.Path(snapshot._path).write_text(json.dumps(payload))

    assert snapshot.load("group/project", "main", "abc123") is None


def test_a_saved_tree_overwrites_an_older_one(snapshot):
    snapshot.save("group/project", "main", "abc123", TREE)
    snapshot.save("group/project", "main", "def456", {"later": True})

    assert snapshot.load("group/project", "main", "abc123") is None
    assert snapshot.load("group/project", "main", "def456") == {"later": True}
