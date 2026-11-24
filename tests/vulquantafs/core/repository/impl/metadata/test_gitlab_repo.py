"""The GitLab blob metadata store: REST calls, 404-as-empty, the
update-then-create fallback and at-rest encryption of the document."""

import base64
import json

import aiohttp
import pytest

from vulquantafs.config import GitlabRepoConfig
from vulquantafs.core.model import TGFSDirectory, TGFSMetadata
from vulquantafs.core.repository.impl.metadata.gitlab_repo import (
    GitlabRepoMetadataRepository,
)
from vulquantafs.crypto.metadata_blob import derive_metadata_blob_key
from vulquantafs.errors import MetadataNotInitialized, NoStoredMetadata, TechnicalError


class FakeResponse:
    def __init__(self, status: int, body: str = ""):
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *args) -> bool:
        return False


class FakeGitlabSession:
    """Answers every request through ``handler(method, url, json)`` and
    records what was sent."""

    def __init__(self, handler):
        self.handler = handler
        self.requests = []
        self.closed = False

    def request(self, method: str, url: str, json=None):
        self.requests.append((method, url, json))
        status, body = self.handler(method, url, json)
        return FakeResponse(status, body)

    async def close(self):
        self.closed = True


def gitlab_config(**overrides) -> GitlabRepoConfig:
    data = {
        "repo": "group/project",
        "branch": "main",
        "access_token": "glpat-token",
    }
    data.update(overrides)
    return GitlabRepoConfig.from_dict(data)


def sample_metadata() -> TGFSMetadata:
    root = TGFSDirectory.root_dir()
    root.create_dir("docs")
    return TGFSMetadata(root)


def document_body(metadata: TGFSMetadata) -> str:
    return json.dumps(
        {"content": base64.b64encode(json.dumps(metadata.to_dict()).encode()).decode()}
    )


def repository(session: FakeGitlabSession, config=None, blob_key=None):
    repo = GitlabRepoMetadataRepository(config or gitlab_config(), blob_key=blob_key)
    repo._session = session  # type: ignore[assignment]
    return repo


class TestGet:
    async def test_a_missing_document_is_no_stored_metadata(self):
        session = FakeGitlabSession(lambda m, u, j: (404, ""))
        repo = repository(session)

        with pytest.raises(NoStoredMetadata, match="group/project@main:metadata.json"):
            await repo.get()

    async def test_the_document_is_parsed(self):
        metadata = sample_metadata()
        session = FakeGitlabSession(lambda m, u, j: (200, document_body(metadata)))
        repo = repository(session)

        result = await repo.get()

        assert isinstance(result, TGFSMetadata)
        assert [d.name for d in result.dir.find_dirs()] == ["docs"]

    async def test_the_url_targets_the_project_file_and_branch(self):
        session = FakeGitlabSession(lambda m, u, j: (404, ""))
        repo = repository(session, gitlab_config(file_path="backups/meta.json"))

        with pytest.raises(NoStoredMetadata):
            await repo.get()

        method, url, _ = session.requests[0]
        assert method == "GET"
        assert url == (
            "https://gitlab.com/api/v4/projects/group%2Fproject"
            "/repository/files/backups%2Fmeta.json?ref=main"
        )

    async def test_a_self_hosted_base_url_is_used(self):
        session = FakeGitlabSession(lambda m, u, j: (404, ""))
        repo = repository(
            session, gitlab_config(base_url="https://gitlab.example.com/")
        )

        with pytest.raises(NoStoredMetadata):
            await repo.get()

        assert session.requests[0][1].startswith(
            "https://gitlab.example.com/api/v4/projects/"
        )

    async def test_an_api_error_raises_technical(self):
        session = FakeGitlabSession(lambda m, u, j: (500, '{"message": "boom"}'))
        repo = repository(session)

        with pytest.raises(TechnicalError, match="500"):
            await repo.get()

    async def test_a_transport_error_raises_technical(self):
        def handler(m, u, j):
            raise aiohttp.ClientError("connection reset")

        session = FakeGitlabSession(handler)
        repo = repository(session)

        with pytest.raises(TechnicalError, match="connection reset"):
            await repo.get()

    async def test_garbage_content_raises_technical(self):
        session = FakeGitlabSession(
            lambda m, u, j: (
                200,
                json.dumps({"content": base64.b64encode(b"{").decode()}),
            )
        )
        repo = repository(session)

        with pytest.raises(TechnicalError, match="Could not parse"):
            await repo.get()


class TestPush:
    async def test_push_without_a_tree_raises(self):
        session = FakeGitlabSession(lambda m, u, j: (200, "{}"))
        repo = repository(session)

        with pytest.raises(MetadataNotInitialized):
            await repo.push()

    async def test_the_first_push_creates_the_document(self):
        def handler(m, u, j):
            return (404, "") if m == "PUT" else (201, "{}")

        session = FakeGitlabSession(handler)
        repo = repository(session)
        repo.metadata = sample_metadata()
        expected = repo.metadata.to_dict()

        await repo.push()

        methods = [r[0] for r in session.requests]
        # The store does not know yet whether the file exists: it tries an
        # update first and falls back to create.
        assert methods == ["PUT", "POST"]
        put_payload = session.requests[0][2]
        assert put_payload["branch"] == "main"
        assert json.loads(put_payload["content"]) == expected

    async def test_a_known_existing_document_is_updated_in_place(self):
        metadata = sample_metadata()
        session = FakeGitlabSession(lambda m, u, j: (200, document_body(metadata)))
        repo = repository(session)
        repo.metadata = await repo.get()
        session.requests.clear()

        repo.metadata.dir.create_dir("more")
        await repo.push()

        assert [r[0] for r in session.requests] == ["PUT"]

    async def test_an_update_that_404s_falls_back_to_create(self):
        calls = []

        def handler(m, u, j):
            calls.append(m)
            if m == "PUT":
                return 404, ""
            return 201, "{}"

        session = FakeGitlabSession(handler)
        repo = repository(session)
        repo.metadata = sample_metadata()

        await repo.push()

        assert calls == ["PUT", "POST"]

    async def test_a_create_that_404s_raises_technical(self):
        session = FakeGitlabSession(lambda m, u, j: (404, ""))
        repo = repository(session)
        repo.metadata = sample_metadata()

        with pytest.raises(TechnicalError, match="Could not create"):
            await repo.push()

    async def test_push_full_writes_the_document_too(self):
        def handler(m, u, j):
            return (404, "") if m == "PUT" else (201, "{}")

        session = FakeGitlabSession(handler)
        repo = repository(session)
        repo.metadata = sample_metadata()

        await repo.push_full()

        assert [r[0] for r in session.requests] == ["PUT", "POST"]

    async def test_the_document_survives_a_roundtrip(self):
        stored: dict = {}

        def handler(m, u, j):
            if m == "GET":
                if "content" not in stored:
                    return 404, ""
                return 200, json.dumps(
                    {"content": base64.b64encode(stored["content"]).decode()}
                )
            stored["content"] = j["content"].encode()
            return 200, "{}"

        session = FakeGitlabSession(handler)
        repo = repository(session)
        repo.metadata = sample_metadata()
        await repo.push()

        result = await repo.get()

        assert [d.name for d in result.dir.find_dirs()] == ["docs"]


class TestEncryption:
    async def test_the_document_is_stored_encrypted_and_read_back(self):
        key = derive_metadata_blob_key(b"master-secret".ljust(32, b"\0"))
        stored: dict = {}

        def handler(m, u, j):
            if m == "GET":
                if "content" not in stored:
                    return 404, ""
                return 200, json.dumps(
                    {"content": base64.b64encode(stored["content"]).decode()}
                )
            stored["content"] = j["content"].encode()
            return 200, "{}"

        session = FakeGitlabSession(handler)
        repo = repository(session, blob_key=key)
        repo.metadata = sample_metadata()
        await repo.push()

        # On the wire there is no plain JSON to be found.
        assert b"docs" not in stored["content"]
        assert stored["content"].startswith(b"VULQUANTAFSMETAV1_")

        result = await repo.get()

        assert [d.name for d in result.dir.find_dirs()] == ["docs"]


class TestSession:
    async def test_close_closes_the_session(self):
        session = FakeGitlabSession(lambda m, u, j: (200, "{}"))
        repo = repository(session)
        repo.metadata = sample_metadata()
        await repo.push()

        await repo.close()

        assert session.closed
        await repo.close()
