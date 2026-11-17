"""The GitHub document metadata store (``layout: document``): the
Contents-API round trips, 404-as-empty, the update/create fallback and
at-rest encryption of the document."""

import base64
import json
from unittest.mock import Mock, patch

import pytest
from github import Github, GithubException

from tgdcfs.config import GithubRepoConfig
from tgdcfs.core.model import TGFSDirectory, TGFSMetadata
from tgdcfs.core.repository.impl.metadata.github_document import (
    GithubDocumentMetadataRepository,
)
from tgdcfs.crypto.metadata_blob import (
    derive_metadata_blob_key,
    encrypt_metadata_blob,
)
from tgdcfs.errors import MetadataNotInitialized, NoStoredMetadata, TechnicalError


def config(**overrides) -> GithubRepoConfig:
    data = {"repo": "owner/test-repo", "commit": "main", "access_token": "token"}
    data.update(overrides)
    return GithubRepoConfig.from_dict(data)


def sample_tree() -> TGFSMetadata:
    root = TGFSDirectory.root_dir()
    docs = root.create_dir("docs")
    docs.create_file_ref("readme", 42)
    return TGFSMetadata(root)


def document(metadata: TGFSMetadata) -> str:
    return json.dumps(metadata.to_dict())


def content_file(text: str, sha: str = "docsha") -> Mock:
    content = Mock()
    content.content = base64.b64encode(text.encode()).decode()
    content.sha = sha
    return content


def repository(mock_repo: Mock, cfg=None, blob_key=None):
    with patch(
        "tgdcfs.core.repository.impl.metadata.github_document.Github", spec=Github
    ) as gh_cls:
        gh_cls.return_value.get_repo.return_value = mock_repo
        repo = GithubDocumentMetadataRepository(cfg or config(), blob_key=blob_key)
    return repo


class TestGet:
    async def test_a_missing_document_is_no_stored_metadata(self):
        mock_repo = Mock()
        mock_repo.get_contents.side_effect = GithubException(
            404, {"message": "Not Found"}, None
        )
        repo = repository(mock_repo)

        with pytest.raises(NoStoredMetadata, match="owner/test-repo@main"):
            await repo.get()

    async def test_the_document_is_parsed(self):
        mock_repo = Mock()
        mock_repo.get_contents.return_value = content_file(document(sample_tree()))
        repo = repository(mock_repo)

        metadata = await repo.get()

        docs = metadata.dir.find_dir("docs")
        assert docs.find_file("readme").message_id == 42

    async def test_the_document_is_decrypted(self):
        key = derive_metadata_blob_key(b"k" * 32)
        mock_repo = Mock()
        mock_repo.get_contents.return_value = content_file(
            encrypt_metadata_blob(key, document(sample_tree()).encode())
        )
        repo = repository(mock_repo, blob_key=key)

        metadata = await repo.get()

        assert metadata.dir.find_dir("docs").find_file("readme").message_id == 42

    async def test_an_unreadable_repo_is_a_technical_error(self):
        mock_repo = Mock()
        mock_repo.get_contents.side_effect = GithubException(
            401, {"message": "Bad credentials"}, None
        )
        repo = repository(mock_repo)

        with pytest.raises(TechnicalError, match="Could not read"):
            await repo.get()


class TestPush:
    def pushed_repo(self, exists=None):
        mock_repo = Mock()
        if exists is not None:
            mock_repo.get_contents.return_value = content_file(
                document(sample_tree()), sha=exists
            )
        return mock_repo

    async def test_a_push_without_a_tree_raises(self):
        repo = repository(self.pushed_repo())

        with pytest.raises(MetadataNotInitialized):
            await repo.push()

    async def test_the_first_push_creates_the_document(self):
        mock_repo = self.pushed_repo()
        mock_repo.get_contents.side_effect = GithubException(
            404, {"message": "Not Found"}, None
        )
        repo = repository(mock_repo)
        repo._exists = False
        repo.metadata = sample_tree()

        await repo.push()

        mock_repo.create_file.assert_called_once()
        kwargs = mock_repo.create_file.call_args.kwargs
        assert kwargs["path"] == "metadata.json"
        assert kwargs["branch"] == "main"
        assert "sha" not in kwargs
        body = json.loads(kwargs["content"])
        assert body["dir"]["name"] == "root"

    async def test_an_existing_document_is_updated_with_its_sha(self):
        mock_repo = self.pushed_repo(exists="docsha")
        repo = repository(mock_repo)
        repo.metadata = sample_tree()

        await repo.push()

        kwargs = mock_repo.create_file.call_args.kwargs
        assert kwargs["sha"] == "docsha"

    async def test_a_vanished_document_falls_back_to_create(self):
        mock_repo = Mock()
        mock_repo.get_contents.side_effect = GithubException(
            404, {"message": "Not Found"}, None
        )
        repo = repository(mock_repo)
        repo._exists = True
        repo.metadata = sample_tree()

        await repo.push()

        mock_repo.create_file.assert_called_once()
        assert "sha" not in mock_repo.create_file.call_args.kwargs

    async def test_a_document_that_appeared_mid_push_is_overwritten(self):
        # The create answers "already exists" (PyGithub's missing-sha 422);
        # the store falls back to an update against the fresh document.
        mock_repo = Mock()
        mock_repo.get_contents.return_value = content_file("{}", sha="othersha")

        def create(**kwargs):
            if "sha" not in kwargs:
                raise GithubException(422, {"message": '"sha" wasn\'t supplied'}, None)
            return Mock()

        mock_repo.create_file.side_effect = create
        repo = repository(mock_repo)
        repo._exists = False
        repo.metadata = sample_tree()

        await repo.push()

        assert mock_repo.create_file.call_count == 2
        assert mock_repo.create_file.call_args.kwargs["sha"] == "othersha"

    async def test_the_pushed_document_is_encrypted(self):
        key = derive_metadata_blob_key(b"k" * 32)
        tree = sample_tree()
        mock_repo = self.pushed_repo(exists="docsha")
        repo = repository(mock_repo, blob_key=key)
        repo.metadata = tree

        await repo.push()

        text = mock_repo.create_file.call_args.kwargs["content"]
        assert decrypts_to(key, text) == document(tree)


def decrypts_to(key: bytes, text: str) -> str:
    from tgdcfs.crypto.metadata_blob import decrypt_metadata_blob

    return decrypt_metadata_blob(key, text).decode()
