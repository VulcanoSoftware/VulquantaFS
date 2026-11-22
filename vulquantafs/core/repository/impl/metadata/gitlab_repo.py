"""The metadata tree as one JSON document in a GitLab repository.

``gitlab_repo`` is the GitLab sibling of the pinned-message store: the
whole tree is serialized into a single file (``metadata.json`` by
default) and rewritten on every push. Unlike ``github_repo`` -- which
mirrors the tree into the repository's own directory structure and
writes incrementally -- this is a blob store, which makes it a
drop-in redundant copy next to any other metadata store in the
replicated metadata layer.

Talks to the GitLab REST API over ``aiohttp`` (already a dependency);
no GitLab client library is added. Works with gitlab.com and
self-hosted instances (``base_url``).
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Optional
from urllib.parse import quote

import aiohttp

from vulquantafs.config import GitlabRepoConfig
from vulquantafs.core.model import TGFSMetadata
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.crypto.metadata_blob import (
    decrypt_metadata_blob,
    encrypt_metadata_blob,
)
from vulquantafs.errors import MetadataNotInitialized, NoStoredMetadata, TechnicalError

logger = logging.getLogger(__name__)

# One push, one HTTP round trip; a slow GitLab must not hold a WebDAV
# write hostage for minutes.
REQUEST_TIMEOUT_SECONDS = 60.0


class GitlabRepoMetadataRepository(IMetaDataRepository):
    def __init__(
        self,
        config: GitlabRepoConfig,
        blob_key: Optional[bytes] = None,
        legacy_blob_key: Optional[bytes] = None,
    ):
        super().__init__()
        self._config = config
        # AES-256-GCM key for the blob when at-rest encryption is enabled;
        # ``None`` stores the document as plain JSON. The legacy key opens
        # blobs left behind by tgdcfs before the rebrand.
        self._blob_key = blob_key
        self._legacy_blob_key = legacy_blob_key
        self._session: Optional[aiohttp.ClientSession] = None
        # Whether the document exists in the repo; ``None`` until the first
        # successful read or write answered the question.
        self._exists: Optional[bool] = None

    @property
    def _project_id(self) -> str:
        return quote(self._config.repo, safe="")

    @property
    def _file_url(self) -> str:
        return (
            f"{self._config.base_url}/api/v4/projects/{self._project_id}"
            f"/repository/files/{quote(self._config.file_path, safe='')}"
        )

    def _headers(self) -> dict:
        return {"PRIVATE-TOKEN": self._config.access_token}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
                headers=self._headers(),
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _request(
        self, method: str, url: str, *, json_body: Optional[dict] = None
    ) -> Optional[dict]:
        """One API call; ``None`` for a 404, the parsed body otherwise."""
        session = await self._ensure_session()
        try:
            async with session.request(method, url, json=json_body) as resp:
                if resp.status == 404:
                    return None
                body = await resp.text()
                if resp.status >= 400:
                    raise TechnicalError(
                        f"GitLab API {method} {url.split('/api/v4/')[-1]} "
                        f"returned {resp.status}: {body[:500]}"
                    )
                return json.loads(body) if body else {}
        except aiohttp.ClientError as ex:
            raise TechnicalError(f"GitLab API request failed: {ex}") from ex
        except json.JSONDecodeError as ex:
            raise TechnicalError(f"GitLab API returned invalid JSON: {ex}") from ex

    async def get(self) -> TGFSMetadata:
        params = f"?ref={quote(self._config.branch, safe='')}"
        data = await self._request("GET", self._file_url + params)
        if data is None:
            self._exists = False
            raise NoStoredMetadata(
                f"{self._config.repo}@{self._config.branch}:{self._config.file_path}"
            )
        self._exists = True
        try:
            text = base64.b64decode(data["content"]).decode()
            if self._blob_key is not None:
                text = decrypt_metadata_blob(
                    self._blob_key, text, legacy_key=self._legacy_blob_key
                ).decode()
            return TGFSMetadata.from_dict(json.loads(text))
        except TechnicalError:
            raise
        except Exception as ex:
            raise TechnicalError(
                f"Could not parse the metadata document of "
                f"{self._config.repo}@{self._config.branch}: {ex}"
            ) from ex

    async def push(self) -> None:
        if not self.metadata:
            raise MetadataNotInitialized()

        data = json.dumps(self.metadata.to_dict()).encode()
        text = (
            encrypt_metadata_blob(self._blob_key, data)
            if self._blob_key is not None
            else data.decode()
        )
        payload_base = {
            "branch": self._config.branch,
            "content": text,
            "commit_message": "Update VulquantaFS metadata",
            "encoding": "text",
        }
        # Update when the document exists; create on the first push into a
        # fresh repo. Both directions fall back once, so a race with
        # another writer or a stale ``_exists`` heals itself.
        if self._exists is False:
            await self._create(payload_base)
        else:
            updated = await self._update(payload_base)
            if not updated:
                await self._create(payload_base)

    async def _update(self, payload: dict) -> bool:
        body = await self._request("PUT", self._file_url, json_body=payload)
        if body is None:
            self._exists = False
            return False
        self._exists = True
        return True

    async def _create(self, payload: dict) -> None:
        body = await self._request("POST", self._file_url, json_body=payload)
        if body is None:
            # A 404 on create is GitLab saying the branch or project does
            # not exist; that is not recoverable here.
            raise TechnicalError(
                f"Could not create {self._config.file_path} in "
                f"{self._config.repo}@{self._config.branch}: not found"
            )
        self._exists = True

    async def push_full(self) -> None:
        # A blob store has no partial state to reconcile.
        await self.push()
