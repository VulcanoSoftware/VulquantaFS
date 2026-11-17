"""The metadata store wiring: which repository class a configured
metadata store gets, per type and layout."""

from typing import cast
from unittest.mock import Mock, patch

import pytest

from tgdcfs.config import MetadataConfig, MetadataConfigDict, MetadataType
from tgdcfs.core.client import Client
from tgdcfs.core.repository.impl.metadata.github_document import (
    GithubDocumentMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.gitlab_repo import (
    GitlabRepoMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.gitlab_tree import (
    GitlabTreeRepoMetadataRepository,
)


def github_cfg(**overrides):
    data = {"repo": "o/r", "commit": "main", "access_token": "t"}
    data.update(overrides)
    return data


def gitlab_cfg(**overrides):
    data = {"repo": "g/p", "branch": "main", "access_token": "t"}
    data.update(overrides)
    return data


def metadata_config(**overrides) -> MetadataConfig:
    data = {"type": "github_repo", "github_repo": github_cfg()}
    data.update(overrides)
    return MetadataConfig.from_dict(cast(MetadataConfigDict, data))


def build(cfg):
    return Client._build_repo_metadata_repo(cfg, b"k" * 32, b"b" * 32)


class TestWiring:
    def test_github_tree_is_the_default(self):
        with patch(
            "tgdcfs.core.repository.impl.metadata.github_repo.GithubRepoMetadataRepository"
        ) as cls:
            cls.return_value = sentinel = Mock()
            repo = build(metadata_config())

        assert repo is sentinel
        cfg, kwargs = cls.call_args.args[0], cls.call_args.kwargs
        assert cfg.repo == "o/r"
        assert kwargs == {"name_key": b"k" * 32}

    def test_github_document_layout(self):
        with patch(
            "tgdcfs.core.repository.impl.metadata.github_document.GithubDocumentMetadataRepository"
        ) as cls:
            cls.return_value = sentinel = Mock()
            repo = build(metadata_config(github_repo=github_cfg(layout="document")))

        assert repo is sentinel
        assert cls.call_args.kwargs == {"blob_key": b"b" * 32}

    def test_gitlab_tree_is_the_default(self):
        with patch(
            "tgdcfs.core.repository.impl.metadata.gitlab_tree.GitlabTreeRepoMetadataRepository"
        ) as cls:
            cls.return_value = sentinel = Mock()
            repo = build(metadata_config(type="gitlab_repo", gitlab_repo=gitlab_cfg()))

        assert repo is sentinel
        assert cls.call_args.kwargs == {
            "name_key": b"k" * 32,
            "blob_key": b"b" * 32,
        }

    def test_gitlab_document_layout(self):
        with patch(
            "tgdcfs.core.repository.impl.metadata.gitlab_repo.GitlabRepoMetadataRepository"
        ) as cls:
            cls.return_value = sentinel = Mock()
            repo = build(
                metadata_config(
                    type="gitlab_repo",
                    gitlab_repo=gitlab_cfg(layout="document"),
                )
            )

        assert repo is sentinel
        assert cls.call_args.kwargs == {"blob_key": b"b" * 32}

    def test_a_github_store_without_a_repo_config_is_rejected(self):
        cfg = MetadataConfig(name="m", type=MetadataType.GITHUB_REPO, github_repo=None)
        with pytest.raises(ValueError, match="github_repo"):
            build(cfg)

    def test_a_gitlab_store_without_a_repo_config_is_rejected(self):
        cfg = MetadataConfig(
            name="m", type=MetadataType.GITLAB_REPO, github_repo=None, gitlab_repo=None
        )
        with pytest.raises(ValueError, match="gitlab_repo"):
            build(cfg)

    def test_the_real_gitlab_tree_store_wraps_the_engine(self):
        # No mocks: constructing the store must not touch the network.
        repo = build(metadata_config(type="gitlab_repo", gitlab_repo=gitlab_cfg()))
        assert isinstance(repo, GitlabTreeRepoMetadataRepository)
        assert repo._ghc.repo_name == "g/p"
        assert repo._ghc.commit == "main"

    def test_the_real_document_stores_construct_offline(self):
        with patch("tgdcfs.core.repository.impl.metadata.github_document.Github"):
            gh_doc = build(metadata_config(github_repo=github_cfg(layout="document")))
        assert isinstance(gh_doc, GithubDocumentMetadataRepository)

        gl_doc = build(
            metadata_config(
                type="gitlab_repo", gitlab_repo=gitlab_cfg(layout="document")
            )
        )
        assert isinstance(gl_doc, GitlabRepoMetadataRepository)
