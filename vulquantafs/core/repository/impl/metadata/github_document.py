"""The metadata tree as one JSON document in a GitHub repository.

The ``layout: document`` variant of ``github_repo``: the whole tree is
serialized into a single file (``metadata.json`` by default) and
rewritten on every push, the same shape the pinned-message and
``gitlab_repo`` document stores use. Unlike the default tree layout --
which mirrors the tree into the repository's own directory structure and
writes incrementally -- this is a blob store, which makes it a drop-in
redundant copy next to any other metadata store in the replicated
metadata layer.

Talks to the GitHub Contents API through PyGithub (already a
dependency), off the event loop like every other blocking GitHub call.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from functools import partial
from typing import Any, Optional, cast

from github import Github, GithubException, GithubRetry
from github.ContentFile import ContentFile

from vulquantafs.config import GithubRepoConfig
from vulquantafs.core.model import TGFSMetadata
from vulquantafs.core.repository.impl.metadata.breaker import (
    FailFastRepository,
    StoreCircuitBreaker,
    probe_github_branch,
)
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.crypto.metadata_blob import (
    decrypt_metadata_blob,
    encrypt_metadata_blob,
)
from vulquantafs.errors import MetadataNotInitialized, NoStoredMetadata, TechnicalError

logger = logging.getLogger(__name__)


def _is_sha_missing(ex: Exception) -> bool:
    """PyGithub's refusal to overwrite an existing file without its sha."""
    status = getattr(ex, "status", None)
    if status != 422:
        return False
    data = getattr(ex, "data", None)
    message = data.get("message", "") if isinstance(data, dict) else ""
    return '"sha" wasn\'t supplied' in str(message)


class GithubDocumentMetadataRepository(IMetaDataRepository):
    def __init__(
        self,
        config: GithubRepoConfig,
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
        # Lazy: the repository completes on the first API call (which runs
        # in ``asyncio.to_thread``), so construction stays off the network
        # and an unreachable GitHub surfaces as this store's read error --
        # the replicated layer fails over to the next store and the heal
        # worker catches this one up later. The bounded retry keeps that
        # first failure at one attempt plus its timeout, and the fail-fast
        # wrapper refuses this store's calls while the service stays
        # unreachable instead of paying the connection timeout per call,
        # mirroring the tree store.
        raw = Github(config.access_token, retry=GithubRetry(total=1)).get_repo(
            config.repo, lazy=True
        )
        self._breaker = StoreCircuitBreaker(probe_github_branch(raw, config.commit))
        self._repo: Any = FailFastRepository(raw, self._breaker)
        # Whether the document exists in the repo; ``None`` until the first
        # successful read or write answered the question.
        self._exists: Optional[bool] = None

    async def get(self) -> TGFSMetadata:
        try:
            content = cast(
                ContentFile,
                await asyncio.to_thread(
                    self._repo.get_contents,
                    self._config.file_path,
                    ref=self._config.commit,
                ),
            )
        except GithubException as ex:
            if ex.status == 404:
                self._exists = False
                raise NoStoredMetadata(
                    f"{self._config.repo}@{self._config.commit}:"
                    f"{self._config.file_path}"
                ) from ex
            raise TechnicalError(
                f"Could not read the metadata document from "
                f"{self._config.repo}@{self._config.commit}:"
                f"{self._config.file_path}: {ex}"
            ) from ex
        except Exception as ex:
            raise TechnicalError(
                f"Could not read the metadata document from "
                f"{self._config.repo}@{self._config.commit}:"
                f"{self._config.file_path}: {ex}"
            ) from ex
        self._exists = True
        try:
            text = base64.b64decode(content.content).decode()
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
                f"{self._config.repo}@{self._config.commit}:"
                f"{self._config.file_path}: {ex}"
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
        # Update when the document exists; create on the first push into a
        # fresh repo. Both directions fall back once, so a race with
        # another writer or a stale ``_exists`` heals itself.
        if self._exists is False:
            await self._create(text)
        else:
            updated = await self._update(text)
            if not updated:
                await self._create(text)

    async def _update(self, text: str) -> bool:
        """Overwrite the document; ``False`` when it turned out not to exist."""
        try:
            current = cast(
                ContentFile,
                await asyncio.to_thread(
                    self._repo.get_contents,
                    self._config.file_path,
                    ref=self._config.commit,
                ),
            )
        except GithubException as ex:
            if ex.status == 404:
                self._exists = False
                return False
            raise TechnicalError(
                f"Could not read {self._config.file_path} in "
                f"{self._config.repo}@{self._config.commit}: {ex}"
            ) from ex
        try:
            await asyncio.to_thread(
                partial(
                    self._repo.create_file,
                    path=self._config.file_path,
                    message="Update VulquantaFS metadata",
                    content=text,
                    branch=self._config.commit,
                    # Not in the signature; travels through **kwargs.
                    sha=current.sha,  # type: ignore[call-arg]
                )
            )
        except GithubException as ex:
            if _is_sha_missing(ex):
                # Another writer created the document in between; its
                # content is about to be overwritten by this push anyway,
                # and the next push reads the sha afresh.
                return True
            raise TechnicalError(
                f"Could not write {self._config.file_path} in "
                f"{self._config.repo}@{self._config.commit}: {ex}"
            ) from ex
        except Exception as ex:
            raise TechnicalError(
                f"Could not write {self._config.file_path} in "
                f"{self._config.repo}@{self._config.commit}: {ex}"
            ) from ex
        self._exists = True
        return True

    async def _create(self, text: str) -> None:
        try:
            await asyncio.to_thread(
                self._repo.create_file,
                path=self._config.file_path,
                message="Create VulquantaFS metadata",
                content=text,
                branch=self._config.commit,
            )
        except GithubException as ex:
            if _is_sha_missing(ex):
                # The document came into existence in the meantime; put
                # this push's content on top of it.
                if not await self._update(text):
                    raise TechnicalError(
                        f"Could not create {self._config.file_path} in "
                        f"{self._config.repo}@{self._config.commit}: "
                        f"it appeared and vanished again"
                    ) from ex
                return
            raise TechnicalError(
                f"Could not create {self._config.file_path} in "
                f"{self._config.repo}@{self._config.commit}: {ex}"
            ) from ex
        except Exception as ex:
            raise TechnicalError(
                f"Could not create {self._config.file_path} in "
                f"{self._config.repo}@{self._config.commit}: {ex}"
            ) from ex
        self._exists = True

    async def push_full(self) -> None:
        # A blob store has no partial state to reconcile.
        await self.push()
