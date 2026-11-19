"""The metadata store constructors must come up without touching the
network.

The GitHub stores used to resolve their repository with a blocking call
in ``__init__`` -- on the event loop, while the config loaded, with
PyGithub retrying a dead connection for minutes. An api.github.com
outage froze the whole server before the replicated metadata layer even
existed, and a perfectly healthy GitLab mirror never got the chance to
serve. The repositories now resolve lazily on the first API call, which
runs on a worker thread; these tests pin that down and run the failover
itself.
"""

from unittest.mock import Mock, patch

import time

import pytest
import requests
from github import GithubException

from tgdcfs.config import GithubRepoConfig
from tgdcfs.core.repository.impl.metadata.github_document import (
    GithubDocumentMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.github_repo import (
    GithubRepoMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.gitlab_tree import (
    GitlabTreeRepoMetadataRepository,
)
from tgdcfs.core.repository.impl.metadata.replicated import (
    MetadataStore,
    ReplicatedMetadataRepository,
)
from tgdcfs.errors import TechnicalError

from .test_gitlab_tree import FakeGitlab, gitlab_config


@pytest.fixture(autouse=True)
def stub_timestamp_store():
    """Keep the date cache out of these tests: a real store would start
    background workers and write a cache file into the working
    directory."""
    with patch(
        "tgdcfs.core.repository.impl.metadata.github_repo.DirTimestampStore"
    ) as store_cls:
        store = Mock()
        store.get.return_value = None
        store_cls.return_value = store
        yield store


def github_config() -> GithubRepoConfig:
    return GithubRepoConfig.from_dict(
        {
            "repo": "vectrastack/tgdcfs_metadata",
            "commit": "main",
            "access_token": "ghp-token",
        }
    )


def refuse_requests(message: str):
    def raise_timeout(*args, **kwargs):
        raise requests.exceptions.ConnectTimeout(message)

    return raise_timeout


class CountingTimeout:
    """A ``requestJsonAndCheck`` replacement for an unreachable api
    domain that records every attempt and pays a short sleep first, so a
    test that reaches the slow code path cannot pass by accident: the
    full PyGithub retry cycle costs seconds per call, the breaker is
    supposed to bring that back to one attempt."""

    def __init__(self):
        self.attempts = 0

    def __call__(self, *args, **kwargs):
        self.attempts += 1
        time.sleep(0.25)
        raise requests.exceptions.ConnectTimeout(
            "Connection to api.github.com timed out."
        )


class TestConstructorsStayOffline:
    async def test_the_tree_store_constructs_without_a_request(self):
        with patch("github.Requester.Requester.requestJsonAndCheck") as request:
            request.side_effect = AssertionError("network call in __init__")

            repo = GithubRepoMetadataRepository(github_config())

        assert repo.metadata is None

    async def test_the_document_store_constructs_without_a_request(self):
        with patch("github.Requester.Requester.requestJsonAndCheck") as request:
            request.side_effect = AssertionError("network call in __init__")

            repo = GithubDocumentMetadataRepository(github_config())

        assert repo.metadata is None


class TestFailoverAtBoot:
    async def test_an_unreachable_github_primary_boots_on_the_gitlab_mirror(
        self, monkeypatch
    ):
        fake = FakeGitlab()
        fake.commit(
            "main",
            "seed",
            [
                {
                    "action": "create",
                    "file_path": "docs/readme.42",
                    "content": "",
                    "encoding": "text",
                },
            ],
        )
        monkeypatch.setattr(
            requests.Session,
            "request",
            lambda self, method, url, **kw: fake.handle(
                method, url, kw.get("params"), kw.get("json")
            ),
        )
        monkeypatch.setattr(
            "github.Requester.Requester.requestJsonAndCheck",
            refuse_requests("Connection to api.github.com timed out."),
        )

        layer = ReplicatedMetadataRepository(
            [
                MetadataStore(
                    key="github_repo@vectrastack/tgdcfs_metadata@main",
                    repo=GithubRepoMetadataRepository(github_config()),
                    primary=True,
                ),
                MetadataStore(
                    key="gitlab_repo@group/project@main",
                    repo=GitlabTreeRepoMetadataRepository(gitlab_config()),
                ),
            ]
        )

        await layer.init()

        # The mirror served the tree the primary could not.
        assert layer.metadata is not None
        docs = layer.metadata.dir.find_dir("docs")
        assert docs.find_file("readme").message_id == 42
        assert layer.stale_stores() == ["github_repo@vectrastack/tgdcfs_metadata@main"]

    async def test_an_unreachable_gitlab_mirror_leaves_the_primary_in_charge(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            requests.Session,
            "request",
            refuse_requests("Connection to gitlab.com timed out."),
        )

        def empty_repo(self, method, url, *args, **kwargs):
            raise GithubException(404, {"message": "Not Found"}, None)

        monkeypatch.setattr(
            "github.Requester.Requester.requestJsonAndCheck", empty_repo
        )

        layer = ReplicatedMetadataRepository(
            [
                MetadataStore(
                    key="github_repo@vectrastack/tgdcfs_metadata@main",
                    repo=GithubRepoMetadataRepository(github_config()),
                    primary=True,
                ),
                MetadataStore(
                    key="gitlab_repo@group/project@main",
                    repo=GitlabTreeRepoMetadataRepository(gitlab_config()),
                ),
            ]
        )

        await layer.init()

        assert layer.metadata is not None
        assert layer.metadata.dir.find_files() == []
        assert layer.stale_stores() == ["gitlab_repo@group/project@main"]


class TestOutageStaysUsable:
    """github.com unreachable is the failover scenario, not a slow one:
    the store pays its dead connection once and the layer then refuses
    it, while the other stores keep serving."""

    async def test_boot_and_every_operation_after_stay_quick(self, monkeypatch):
        fake = FakeGitlab()
        fake.commit(
            "main",
            "seed",
            [
                {
                    "action": "create",
                    "file_path": "docs/readme.42",
                    "content": "",
                    "encoding": "text",
                },
            ],
        )
        monkeypatch.setattr(
            requests.Session,
            "request",
            lambda self, method, url, **kw: fake.handle(
                method, url, kw.get("params"), kw.get("json")
            ),
        )
        timeout = CountingTimeout()
        monkeypatch.setattr("github.Requester.Requester.requestJsonAndCheck", timeout)

        gh_store = GithubRepoMetadataRepository(github_config())
        layer = ReplicatedMetadataRepository(
            [
                MetadataStore(
                    key="github_repo@vectrastack/tgdcfs_metadata@main",
                    repo=gh_store,
                    primary=True,
                ),
                MetadataStore(
                    key="gitlab_repo@group/project@main",
                    repo=GitlabTreeRepoMetadataRepository(gitlab_config()),
                ),
            ]
        )

        gh_breaker = gh_store._breaker
        assert gh_breaker is not None
        started = time.monotonic()
        try:
            await layer.init()
            boot = time.monotonic() - started

            assert layer.metadata is not None
            docs = layer.metadata.dir.find_dir("docs")
            assert docs.find_file("readme").message_id == 42
            assert layer.stale_stores() == [
                "github_repo@vectrastack/tgdcfs_metadata@main"
            ]
            # One episode of the dead connection covers the whole boot:
            # the timestamps lookup fails into it, and every later call
            # (the tree read, its fallbacks, the first full sync) is
            # refused outright.
            boot_attempts = timeout.attempts
            assert boot_attempts <= 2

            # A steady heal and pushed mutations never reach for GitHub
            # while it is known down: every sync is refused immediately.
            started = time.monotonic()
            for _ in range(10):
                await layer.heal()
            refusal_cycle = time.monotonic() - started

            assert timeout.attempts == boot_attempts
            assert refusal_cycle < 2.0
            assert boot < 5.0
            assert layer.stale_stores() == [
                "github_repo@vectrastack/tgdcfs_metadata@main"
            ]
            # The mirror still served the tree: the file system is fully
            # usable on the healthy store.
            assert layer.metadata.dir.find_dir("docs").find_file("readme") is not None
        finally:
            # Stop the probe before the request mocks go away.
            gh_breaker.stop()

    async def test_the_document_store_refuses_calls_once_store_known_down(
        self, monkeypatch
    ):
        timeout = CountingTimeout()
        monkeypatch.setattr("github.Requester.Requester.requestJsonAndCheck", timeout)
        repo = GithubDocumentMetadataRepository(github_config())
        doc_breaker = repo._breaker

        try:
            with pytest.raises(TechnicalError):
                await repo.get()
            first = timeout.attempts
            assert first <= 2

            started = time.monotonic()
            with pytest.raises(TechnicalError):
                await repo.get()
            # Refused: not one further attempt, not one further timeout.
            assert timeout.attempts == first
            assert time.monotonic() - started < 1.0
        finally:
            doc_breaker.stop()
