"""The replication queue snapshots itself to the metadata stores.

The local file is the source of truth; the snapshot is a backup for the
day the data directory is lost. These tests pin the contract on both
ends: the queue coalesces its changes into one paced snapshot push, the
tree store stores and reads the payload back, and a boot with a missing
local file seeds the queue from the store -- a boot with a local file
ignores the snapshot.
"""

import asyncio
import base64
import gzip
from unittest.mock import AsyncMock, Mock, patch

import pytest
from github import Github, GithubException
from github.GitBlob import GitBlob
from github.GitCommit import GitCommit
from github.GitRef import GitRef
from github.GitTree import GitTree
from github.Repository import Repository

import vulquantafs.core.replication as replication_module
from vulquantafs.config import GithubRepoConfig
from vulquantafs.core.replication import (
    ReplicationQueue,
    QueueItem,
    encode_queue_backup,
    parse_queue_backup,
    restore_replication_backups,
)
from vulquantafs.core.repository.impl.metadata.github_repo import (
    REPLICATION_BACKUP_PATH,
    GithubRepoMetadataRepository,
)
from vulquantafs.core.repository.impl.metadata.replicated import (
    MetadataStore,
    ReplicatedMetadataRepository,
)


@pytest.fixture(autouse=True)
def fast_backup(monkeypatch):
    monkeypatch.setattr(replication_module, "BACKUP_MIN_INTERVAL_S", 0)


@pytest.fixture
def mock_github_config():
    return GithubRepoConfig(
        access_token="test_token", repo="owner/test-repo", commit="main"
    )


@pytest.fixture
def mock_ghc():
    from vulquantafs.core.repository.impl.metadata.github_repo.gh_directory import (
        GithubConfig,
    )

    return GithubConfig(
        gh=Mock(spec=Github),
        repo_name="owner/test-repo",
        repo=Mock(spec=Repository),
        commit="main",
    )


def build_repository(mock_github_config, mock_ghc):
    with patch("vulquantafs.core.repository.impl.metadata.github_repo.Github"):
        repository = GithubRepoMetadataRepository(mock_github_config)
    repository._ghc = mock_ghc
    return repository


def mock_backup_tree(mock_ghc, payload=None):
    """Make ``load_replication_backup``'s read path answer with one blob.

    ``payload`` is the gzip bytes a backup would have handed in; the raw
    blob bytes are the base64 text the backup committed, and PyGithub
    hands those out base64-encoded again.
    """
    ref, commit, tree, blob = (
        Mock(spec=GitRef),
        Mock(spec=GitCommit),
        Mock(spec=GitTree),
        Mock(spec=GitBlob),
    )
    ref.object.sha = "commit-sha"
    commit.tree.sha = "tree-sha"
    tree.truncated = False
    if payload is None:
        tree.tree = []
        blob.content = ""
    else:
        committed = base64.b64encode(payload).decode("ascii")
        blob.content = base64.b64encode(committed.encode("ascii")).decode("ascii")
        entry = Mock()
        entry.type = "blob"
        entry.path = REPLICATION_BACKUP_PATH
        entry.sha = "blob-sha"
        tree.tree = [entry]
    repo = mock_ghc.repo
    repo.get_git_ref.return_value = ref
    repo.get_git_commit.return_value = commit
    repo.get_git_tree.return_value = tree
    repo.get_git_blob.return_value = blob
    return tree


@pytest.fixture
def queue(tmp_path):
    return ReplicationQueue(str(tmp_path / "queue.json"))


class TestBackupSerialization:
    def test_items_round_trip_through_the_snapshot(self):
        items = [
            QueueItem(
                path="docs/report.txt",
                queued_at=1234.5,
                attempts=3,
                last_error="store down",
            ),
            QueueItem(path="other.bin", queued_at=9.0),
        ]

        parsed = parse_queue_backup(encode_queue_backup(items))

        assert [(i.path, i.queued_at, i.attempts, i.last_error) for i in parsed] == [
            ("docs/report.txt", 1234.5, 3, "store down"),
            ("other.bin", 9.0, 0, None),
        ]

    def test_an_unreadable_snapshot_raises(self):
        with pytest.raises(Exception):
            parse_queue_backup(b"not a gzip file")


class TestQueueBackupPushes:
    async def test_an_enqueue_snapshots_the_queue(self, queue):
        push = AsyncMock()
        queue.register_backup("media", push)

        queue.enqueue("media", "docs/report.txt")
        await queue._backup_tasks["media"]

        call = push.await_args
        assert call is not None
        (snapshot,) = call.args
        assert [i.path for i in parse_queue_backup(snapshot)] == ["docs/report.txt"]

    async def test_a_drained_queue_snapshots_the_empty_state(self, queue):
        push = AsyncMock()
        queue.register_backup("media", push)
        queue.enqueue("media", "docs/report.txt")
        await queue._backup_tasks["media"]

        push.reset_mock()
        queue.done("media", "docs/report.txt")
        await queue._backup_tasks["media"]

        call = push.await_args
        assert call is not None
        (snapshot,) = call.args
        assert parse_queue_backup(snapshot) == []

    async def test_changes_while_a_push_runs_coalesce_into_the_next_one(self, queue):
        first_payloads: list = []
        release = asyncio.Event()

        async def slow_push(snapshot):
            first_payloads.append(parse_queue_backup(snapshot))
            await release.wait()

        queue.register_backup("media", slow_push)
        queue.enqueue("media", "one.txt")
        task = queue._backup_tasks["media"]
        while not first_payloads:
            await asyncio.sleep(0)
        queue.enqueue("media", "two.txt")
        queue.done("media", "one.txt")
        release.set()
        await task

        assert [i.path for i in first_payloads[0]] == ["one.txt"]
        assert [i.path for i in queue.pending("media")] == ["two.txt"]

    async def test_a_failing_push_does_not_disturb_the_queue(self, queue):
        push = AsyncMock(side_effect=RuntimeError("store down"))
        queue.register_backup("media", push)

        queue.enqueue("media", "docs/report.txt")
        await queue._backup_tasks["media"]

        assert [i.path for i in queue.pending("media")] == ["docs/report.txt"]
        queue.enqueue("media", "other.txt")
        await queue._backup_tasks["media"]
        assert len(push.await_args_list) == 2

    async def test_without_a_registered_backup_nothing_runs(self, queue):
        queue.enqueue("media", "docs/report.txt")
        assert queue._backup_tasks == {}

    async def test_a_backup_is_paced_to_the_interval(self, queue, monkeypatch):
        monkeypatch.setattr(replication_module, "BACKUP_MIN_INTERVAL_S", 3600.0)
        push = AsyncMock()
        queue.register_backup("media", push)
        queue.enqueue("media", "one.txt")
        await queue._backup_tasks["media"]
        push.reset_mock()

        queue.done("media", "one.txt")
        second = queue._backup_tasks["media"]
        await asyncio.sleep(0.01)

        assert push.await_count == 0
        assert not second.done()
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)

    async def test_a_change_after_a_finished_backup_starts_a_new_paced_one(
        self, queue, monkeypatch
    ):
        monkeypatch.setattr(replication_module, "BACKUP_MIN_INTERVAL_S", 3600.0)
        push = AsyncMock()
        queue.register_backup("media", push)
        queue.enqueue("media", "one.txt")
        first = queue._backup_tasks["media"]
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)

        queue.done("media", "one.txt")
        second = queue._backup_tasks["media"]

        assert second is not first
        assert not second.done()
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)


class TestQueueRestore:
    async def test_a_boot_without_the_local_file_seeds_from_the_store(self, queue):
        items = [QueueItem(path="docs/report.txt", queued_at=5.0)]
        api = Mock()
        api.load_replication_backup = AsyncMock(return_value=encode_queue_backup(items))
        client = Mock()
        client.name = "media"
        client.metadata_api = api

        await restore_replication_backups({"media": client}, queue)

        assert [i.path for i in queue.pending("media")] == ["docs/report.txt"]
        assert queue.pending("media")[0].queued_at == 5.0

    async def test_a_boot_with_the_local_file_ignores_the_snapshot(self, queue):
        queue.enqueue("media", "local.txt")
        api = Mock()
        api.load_replication_backup = AsyncMock(return_value=b"unused")
        client = Mock()
        client.name = "media"
        client.metadata_api = api

        await restore_replication_backups({"media": client}, queue)

        api.load_replication_backup.assert_not_awaited()
        assert [i.path for i in queue.pending("media")] == ["local.txt"]

    async def test_local_items_stay_ahead_of_a_stale_snapshot(self, queue):
        queue.enqueue("media", "local.txt")
        items = [
            QueueItem(path="local.txt", queued_at=1.0, attempts=7),
            QueueItem(path="snapshot.txt", queued_at=2.0),
        ]

        restored = queue.restore("media", items)

        assert restored == 1
        by_path = {i.path: i for i in queue.pending("media")}
        assert set(by_path) == {"local.txt", "snapshot.txt"}
        assert by_path["local.txt"].attempts == 0

    async def test_a_broken_snapshot_leaves_the_queue_empty(self, queue):
        api = Mock()
        api.load_replication_backup = AsyncMock(return_value=b"not a gzip file")
        client = Mock()
        client.name = "media"
        client.metadata_api = api

        await restore_replication_backups({"media": client}, queue)

        assert queue.pending("media") == []

    async def test_a_client_without_a_metadata_store_is_skipped(self, queue):
        client = Mock()
        client.name = "media"
        client.metadata_api = None

        await restore_replication_backups({"media": client}, queue)

        assert queue.pending("media") == []

    async def test_a_failing_read_does_not_stop_the_other_file_systems(self, queue):
        broken = Mock()
        broken.name = "media"
        broken.metadata_api = Mock()
        broken.metadata_api.load_replication_backup = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        healthy = Mock()
        healthy.name = "notes"
        healthy.metadata_api = Mock()
        healthy.metadata_api.load_replication_backup = AsyncMock(
            return_value=encode_queue_backup([QueueItem(path="a.txt", queued_at=1.0)])
        )

        await restore_replication_backups({"media": broken, "notes": healthy}, queue)

        assert [i.path for i in queue.pending("notes")] == ["a.txt"]


class TestTreeStoreBackup:
    async def test_a_snapshot_is_committed_to_the_branch_root(self, mocker):
        ghc = Mock()
        ghc.lock = asyncio.Lock()
        ghc.write = AsyncMock(side_effect=lambda fn, *a: fn(*a))
        repository = build_repository(
            GithubRepoConfig(
                access_token="test_token", repo="owner/test-repo", commit="main"
            ),
            ghc,
        )
        overlay = mocker.patch(
            "vulquantafs.core.repository.impl.metadata.github_repo.commit_tree_overlay"
        )

        payload = encode_queue_backup([QueueItem(path="a.txt", queued_at=1.0)])
        assert await repository.store_replication_backup(payload) is True

        _fn, _repo, _branch, elements, _message = ghc.write.await_args.args
        (element,) = elements
        assert element._InputGitTreeElement__path == REPLICATION_BACKUP_PATH
        stored = base64.b64decode(element._InputGitTreeElement__content)
        assert [i.path for i in parse_queue_backup(stored)] == ["a.txt"]

    async def test_a_store_without_the_branch_backs_nothing_up(self, mocker):
        ghc = Mock()
        ghc.lock = asyncio.Lock()
        ghc.write = AsyncMock(side_effect=lambda fn, *a: fn(*a))
        repository = build_repository(
            GithubRepoConfig(
                access_token="test_token", repo="owner/test-repo", commit="main"
            ),
            ghc,
        )
        mocker.patch(
            "vulquantafs.core.repository.impl.metadata.github_repo.commit_tree_overlay",
            side_effect=GithubException(404, "not found", None),
        )

        assert await repository.store_replication_backup(b"payload") is False

    async def test_the_snapshot_is_read_back(self, mock_ghc):
        repository = build_repository(
            GithubRepoConfig(
                access_token="test_token", repo="owner/test-repo", commit="main"
            ),
            mock_ghc,
        )
        payload = encode_queue_backup([QueueItem(path="a.txt", queued_at=3.0)])
        mock_backup_tree(mock_ghc, payload)

        assert await repository.load_replication_backup() == payload

    async def test_no_snapshot_on_the_branch_answers_none(self, mock_ghc):
        repository = build_repository(
            GithubRepoConfig(
                access_token="test_token", repo="owner/test-repo", commit="main"
            ),
            mock_ghc,
        )
        mock_backup_tree(mock_ghc, None)

        assert await repository.load_replication_backup() is None

    async def test_a_repository_without_a_branch_answers_none(self, mock_ghc):
        repository = build_repository(
            GithubRepoConfig(
                access_token="test_token", repo="owner/test-repo", commit="main"
            ),
            mock_ghc,
        )
        mock_ghc.repo.get_git_ref.side_effect = GithubException(404, "not found", None)

        assert await repository.load_replication_backup() is None

    @pytest.mark.parametrize(
        "path",
        [REPLICATION_BACKUP_PATH, "data/replication-queue-backup.b64"],
    )
    def test_the_full_sync_leaves_the_snapshot_alone(self, path):
        assert GithubRepoMetadataRepository._is_managed_blob(path) is False


class TestReplicatedDispatch:
    def _replicated(self, stores):
        return ReplicatedMetadataRepository(
            [
                MetadataStore(key=key, repo=repo, primary=(i == 0))
                for i, (key, repo) in enumerate(stores)
            ]
        )

    async def test_a_snapshot_reaches_every_healthy_store(self):
        primary, mirror = Mock(), Mock()
        primary.store_replication_backup = AsyncMock(return_value=True)
        primary.write_in_cooldown = Mock(return_value=False)
        mirror.store_replication_backup = AsyncMock(return_value=True)
        mirror.write_in_cooldown = Mock(return_value=False)
        replicated = self._replicated([("github", primary), ("gitlab", mirror)])

        assert await replicated.store_replication_backup(b"payload") is True
        primary.store_replication_backup.assert_awaited_once_with(b"payload")
        mirror.store_replication_backup.assert_awaited_once_with(b"payload")

    async def test_a_cooldown_store_is_skipped_without_failing(self):
        primary, mirror = Mock(), Mock()
        primary.store_replication_backup = AsyncMock(return_value=True)
        primary.write_in_cooldown = Mock(return_value=False)
        mirror.store_replication_backup = AsyncMock(return_value=True)
        mirror.write_in_cooldown = Mock(return_value=True)
        replicated = self._replicated([("github", primary), ("gitlab", mirror)])

        assert await replicated.store_replication_backup(b"payload") is True
        mirror.store_replication_backup.assert_not_awaited()

    async def test_a_failing_store_does_not_stop_the_other(self):
        primary, mirror = Mock(), Mock()
        primary.store_replication_backup = AsyncMock(
            side_effect=RuntimeError("branch moved")
        )
        primary.write_in_cooldown = Mock(return_value=False)
        mirror.store_replication_backup = AsyncMock(return_value=True)
        mirror.write_in_cooldown = Mock(return_value=False)
        replicated = self._replicated([("github", primary), ("gitlab", mirror)])

        assert await replicated.store_replication_backup(b"payload") is True
        mirror.store_replication_backup.assert_awaited_once_with(b"payload")

    async def test_a_snapshot_is_read_from_the_first_store_that_has_one(self):
        primary, mirror = Mock(), Mock()
        primary.load_replication_backup = AsyncMock(return_value=b"newer")
        mirror.load_replication_backup = AsyncMock(return_value=b"older")
        replicated = self._replicated([("github", primary), ("gitlab", mirror)])

        assert await replicated.load_replication_backup() == b"newer"

    async def test_a_failing_read_falls_through_to_the_next_store(self):
        primary, mirror = Mock(), Mock()
        primary.load_replication_backup = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        mirror.load_replication_backup = AsyncMock(return_value=b"older")
        replicated = self._replicated([("github", primary), ("gitlab", mirror)])

        assert await replicated.load_replication_backup() == b"older"
