"""The ``metadata`` setter of the git tree stores: a tree that arrived as
plain ``TGFSDirectory`` nodes (loaded from a pinned message or a document
store) is converted in place into store-backed nodes, so mutations keep
writing through incrementally and every store of the replicated layer
keeps sharing one tree object.
"""

from typing import cast
from unittest.mock import Mock, patch

import pytest
from github import Github

from tgdcfs.config import GithubRepoConfig
from tgdcfs.core.model import TGFSDirectory, TGFSMetadata
from tgdcfs.core.repository.impl.metadata.github_repo import (
    GithubRepoMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.github_repo.gh_directory import (
    GithubDirectory,
)
from tgdcfs.core.repository.impl.metadata.replicated import (
    MetadataStore,
    ReplicatedMetadataRepository,
)
from tgdcfs.core.repository.interface import IMetaDataRepository
from tgdcfs.errors import NoStoredMetadata


def config() -> GithubRepoConfig:
    return GithubRepoConfig(
        access_token="test_token", repo="owner/test-repo", commit="main"
    )


def stubbed_timestamps():
    store = Mock()
    store.get.return_value = None
    return store


def repository(ghc=None) -> GithubRepoMetadataRepository:
    with patch(
        "tgdcfs.core.repository.impl.metadata.github_repo.Github",
        spec=Github,
    ):
        repo = GithubRepoMetadataRepository(config())
    repo._ghc = ghc or Mock(name_key=None, timestamps=stubbed_timestamps())
    return repo


class MirrorStub(IMetaDataRepository):
    """Records the tree it was handed; nothing else."""

    def __init__(self):
        super().__init__()
        self.received = None

    @property
    def metadata(self):
        return self.received

    @metadata.setter
    def metadata(self, value):
        self.received = value

    async def get(self):
        if self.received is None:
            raise NoStoredMetadata("stub")
        return self.received

    async def push(self):
        pass


def plain_tree() -> TGFSMetadata:
    root = TGFSDirectory.root_dir()
    root.created_at = root.created_at.replace(year=2020)
    docs = root.create_dir("docs")
    docs.created_at = docs.created_at.replace(year=2021)
    ref = docs.create_file_ref("readme", 42)
    ref.mirrors = {"dc:9876": 7}
    ref.store = "tg:-1001"
    return TGFSMetadata(root)


class TestMetadataSetter:
    def test_a_plain_tree_is_wrapped_in_place(self):
        repo = repository()
        tree = plain_tree()

        repo.metadata = tree

        assert repo.metadata is tree
        assert isinstance(tree.dir, GithubDirectory)
        docs = tree.dir.find_dir("docs")
        assert isinstance(docs, GithubDirectory)
        assert docs.parent is tree.dir

    def test_file_refs_keep_mirrors_store(self):
        repo = repository()
        tree = plain_tree()

        repo.metadata = tree

        fr = tree.dir.find_dir("docs").find_file("readme")
        assert fr.message_id == 42
        assert fr.mirrors == {"dc:9876": 7}
        assert fr.store == "tg:-1001"
        assert fr.location is tree.dir.find_dir("docs")

    def test_directory_dates_survive_the_conversion(self):
        repo = repository()
        tree = plain_tree()

        repo.metadata = tree

        assert tree.dir.created_at.year == 2020
        assert tree.dir.find_dir("docs").created_at.year == 2021

    def test_a_store_backed_tree_passes_through_unchanged(self):
        repo = repository()
        tree = plain_tree()
        repo.metadata = tree
        wrapped = repo.metadata

        repo.metadata = wrapped

        assert repo.metadata is wrapped
        assert repo.metadata.dir is wrapped.dir

    def test_names_follow_the_configured_key(self):
        from tgdcfs.crypto.path_names import encrypt_path_name

        key = b"k" * 32
        ghc = Mock(name_key=key, timestamps=stubbed_timestamps())
        repo = repository(ghc)
        tree = plain_tree()

        repo.metadata = tree

        docs = cast(GithubDirectory, tree.dir.find_dir("docs"))
        assert docs._storage_name == encrypt_path_name(key, "docs")

    async def test_mutations_write_through_after_the_conversion(self):
        ghc = Mock(name_key=None, timestamps=stubbed_timestamps())
        repo = repository(ghc)
        tree = plain_tree()
        repo.metadata = tree

        tree.dir.create_dir("new")

        ghc.repo.create_file.assert_called_once()
        assert ghc.repo.create_file.call_args.kwargs["path"] == "new/.gitkeep"

    async def test_the_replicated_layer_keeps_one_shared_tree(self):
        repo = repository()
        mirror = MirrorStub()
        tree = plain_tree()

        layer = ReplicatedMetadataRepository(
            [
                MetadataStore(key="primary", repo=repo, primary=True),
                MetadataStore(key="mirror", repo=mirror),
            ]
        )
        layer.metadata = tree

        assert mirror.metadata is tree
        assert isinstance(mirror.metadata.dir, GithubDirectory)
