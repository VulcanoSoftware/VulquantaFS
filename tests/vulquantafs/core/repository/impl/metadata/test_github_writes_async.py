"""GitHub writes must not block the event loop.

PyGithub is synchronous and sleeps through GitHub's rate limits. Called
straight from a coroutine, one throttled ``create_file`` froze the whole
server -- WebDAV clients timed out after 20 s of silence while a large upload
was running -- so the writes run on a worker thread of their own.
"""

import asyncio
import threading
import time
from unittest.mock import Mock

import pytest
from github import Github
from github.Repository import Repository

from vulquantafs.core.model import TGFSDirectory
from vulquantafs.core.repository.impl.metadata.github_repo.gh_directory import (
    GithubConfig,
    GithubDirectory,
)
from vulquantafs.errors import PendingCommitError


@pytest.fixture
def ghc():
    repo = Mock(spec=Repository)
    repo.name = "test-repo"
    return GithubConfig(
        gh=Mock(spec=Github), repo_name="owner/test-repo", repo=repo, commit="main"
    )


async def _ticks_while(coro, interval: float = 0.01) -> int:
    """How many times the event loop got to run while ``coro`` was awaited."""
    ticks = 0
    task = asyncio.ensure_future(coro)
    while not task.done():
        await asyncio.sleep(interval)
        ticks += 1
    await task
    return ticks


class TestCreateFileRefAsync:
    async def test_concurrent_creates_wait_for_a_single_commit(self, ghc, monkeypatch):
        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        paths: list[str] = []

        def commit(repo, branch, overlay, message):
            paths.extend(item._InputGitTreeElement__path for item in overlay)
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(2)

        monkeypatch.setattr(gh_directory, "commit_tree_overlay", commit)
        directory = GithubDirectory(ghc, "d", None)
        creates = [
            asyncio.create_task(directory.create_file_ref_async(f"f{i}", i))
            for i in range(4)
        ]
        try:
            await asyncio.wait_for(entered.wait(), 2)
            assert not any(task.done() for task in creates)
        finally:
            release.set()
            refs = await asyncio.gather(*creates)

        assert paths == [f"f{i}.{i}" for i in range(4)]
        assert [ref.name for ref in refs] == [f"f{i}" for i in range(4)]
        ghc.repo.create_file.assert_not_called()

    async def test_event_loop_keeps_running_during_a_slow_write(self, ghc):
        ghc.repo.create_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)

        ticks = await _ticks_while(directory.create_file_ref_async("f", 1))

        # A blocking call would have let the loop run once, at most.
        assert ticks >= 10
        assert [fr.name for fr in directory.files] == ["f"]

    async def test_write_runs_on_the_writer_thread(self, ghc):
        seen: list[str] = []
        ghc.repo.create_file.side_effect = lambda **_: seen.append(
            threading.current_thread().name
        )
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_async("f", 1)

        assert seen == [seen[0]]
        assert seen[0].startswith("github-writes")
        assert seen[0] != threading.current_thread().name

    async def test_consecutive_batches_use_the_same_serial_writer(
        self, ghc, monkeypatch
    ):
        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        first_entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        running = 0
        peak = 0
        batches: list[list[str]] = []

        def commit(repo, branch, refs):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            try:
                batches.append([path for path, _ in refs])
                if len(batches) == 1:
                    loop.call_soon_threadsafe(first_entered.set)
                    assert release.wait(2)
            finally:
                running -= 1

        monkeypatch.setattr(gh_directory, "_put_file_refs", commit)
        directory = GithubDirectory(ghc, "d", None)
        first = asyncio.gather(
            *(directory.create_file_ref_async(f"f{i}", i) for i in range(3))
        )
        try:
            await asyncio.wait_for(first_entered.wait(), 2)
            second = asyncio.gather(
                *(directory.create_file_ref_async(f"f{i}", i) for i in range(3, 6))
            )
            await asyncio.sleep(0)
            assert not second.done()
        finally:
            release.set()
            await first
        await second

        assert peak == 1
        assert batches == [
            [f"f{i}.{i}" for i in range(3)],
            [f"f{i}.{i}" for i in range(3, 6)],
        ]
        assert len(directory.files) == 6

    async def test_failure_removes_the_ref_again(self, ghc):
        ghc.repo.create_file.side_effect = Exception("GitHub API error")
        directory = GithubDirectory(ghc, "d", None)

        with pytest.raises(Exception, match="GitHub API error"):
            await directory.create_file_ref_async("f", 1)

        assert len(directory.files) == 0

    async def test_same_blob_as_the_sync_version(self, ghc):
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_async("f", 12345)

        ghc.repo.create_file.assert_called_once_with(
            path="f.12345",
            message="Create file reference for f",
            content="",
            branch="main",
        )

    async def test_a_commit_past_the_deadline_answers_an_error(self, ghc, monkeypatch):
        """A GitHub read timeout plus the write thread's inline retries
        held the awaited commit for over 27 s in the second bulk retest,
        past FileZilla's 20 s. The waiter gives up at its deadline instead:
        the close answers an error, the tree keeps the reference, and the
        blob still lands in the background."""
        from github import GithubException

        from vulquantafs.core.repository.impl.metadata.github_repo import (
            gh_directory,
        )

        monkeypatch.setattr(gh_directory, "AWAITED_COMMIT_MAX_WAIT", 0.05)
        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0)
        monkeypatch.setattr(gh_directory, "PUT_ATTEMPTS", 1)
        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)
        proceed = threading.Event()
        paths: list[str] = []

        def create_file(**kwargs):
            paths.append(kwargs["path"])
            if len(paths) == 1:
                assert proceed.wait(5)
                raise GithubException(500, {}, {})

        ghc.repo.create_file.side_effect = create_file
        directory = GithubDirectory(ghc, "d", None)

        start = time.monotonic()
        # A distinct error: the write landed, only the commit is late, and
        # the caller that owns the file answers it as success.
        with pytest.raises(PendingCommitError, match="Timed out"):
            await directory.create_file_ref_async("f", 1)
        assert time.monotonic() - start < 2

        # The reference stays findable, so a retransmission appends a
        # version to the same file instead of another create.
        assert [fr.name for fr in directory.files] == ["f"]
        directory.files[0].message_id = 99
        proceed.set()

        flusher = ghc.pending_refs_flusher
        if flusher is not None:
            await asyncio.gather(flusher, return_exceptions=True)

        # The blob lands for the retransmitted file's descriptor id, not
        # the stale one the create left behind.
        assert paths == ["f.1", "f.99"]
        assert [fr.name for fr in directory.files] == ["f"]

    async def test_a_transient_failure_keeps_a_timed_out_reference(
        self, ghc, monkeypatch
    ):
        """Once the waiter left on its deadline the blob is nobody's
        reply any more, so GitHub's transient errors must not drop the
        reference out of the tree a retransmission just appended to."""
        from github import GithubException

        from vulquantafs.core.repository.impl.metadata.github_repo import (
            gh_directory,
        )

        monkeypatch.setattr(gh_directory, "AWAITED_COMMIT_MAX_WAIT", 0.05)
        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0)
        monkeypatch.setattr(gh_directory, "PUT_ATTEMPTS", 1)
        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)
        proceed = threading.Event()
        attempts = 0

        def create_file(**_):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                assert proceed.wait(5)
            if attempts <= 2:
                raise GithubException(500, {}, {})

        ghc.repo.create_file.side_effect = create_file
        directory = GithubDirectory(ghc, "d", None)

        with pytest.raises(Exception, match="Timed out"):
            await directory.create_file_ref_async("f", 1)
        proceed.set()

        flusher = ghc.pending_refs_flusher
        if flusher is not None:
            await asyncio.gather(flusher, return_exceptions=True)

        # The rounds nobody waits for kept retrying and the reference
        # survived the transient failures, so the retransmission finds
        # the file in the tree.
        assert attempts == 3
        assert [fr.name for fr in directory.files] == ["f"]


class TestCreateFileRefDeferred:
    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch):
        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0)

    @staticmethod
    async def _wait_for_background(directory: GithubDirectory) -> None:
        pending = [t for t in list(directory._deferred_refs) if not t.done()]
        if pending:
            await asyncio.gather(*pending)

    async def test_the_ref_is_there_before_the_blob_is(self, ghc):
        blob_done = threading.Event()

        def create_file(**_):
            time.sleep(0.3)
            blob_done.set()

        ghc.repo.create_file.side_effect = create_file
        directory = GithubDirectory(ghc, "d", None)

        fr = await directory.create_file_ref_deferred("f", 1)

        assert fr.name == "f"
        assert [r.name for r in directory.files] == ["f"]
        assert not blob_done.is_set()

        await self._wait_for_background(directory)
        assert blob_done.is_set()
        ghc.repo.create_file.assert_called_once_with(
            path="f.1",
            message="Create file reference for f",
            content="",
            branch="main",
        )

    async def test_the_caller_does_not_wait_for_github(self, ghc):
        ghc.repo.create_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)

        start = time.monotonic()
        await directory.create_file_ref_deferred("f", 1)

        assert time.monotonic() - start < 0.1
        await self._wait_for_background(directory)

    async def test_transient_failures_are_retried_in_the_background(self, ghc):
        import requests

        ghc.repo.create_file.side_effect = [
            requests.exceptions.ConnectionError("Max retries"),
            None,
        ]
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        await self._wait_for_background(directory)

        assert ghc.repo.create_file.call_count == 2
        assert [r.name for r in directory.files] == ["f"]

    async def test_a_blob_that_landed_on_the_last_inner_attempt_survives(
        self, ghc, monkeypatch
    ):
        import requests
        from github import GithubException

        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)
        lost_response = requests.exceptions.ConnectionError("IncompleteRead")
        sha_missing = GithubException(
            422,
            {"message": 'Invalid request.\n\n"sha" wasn\'t supplied.'},
            {},
        )
        ghc.repo.create_file.side_effect = [
            lost_response,
            lost_response,
            lost_response,
            sha_missing,
        ]
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        assert [r.name for r in directory.files] == ["f"]

        await self._wait_for_background(directory)

        assert ghc.repo.create_file.call_count == 4
        assert [r.name for r in directory.files] == ["f"]

    async def test_a_permanent_failure_drops_the_ref(self, ghc):
        from github import GithubException

        ghc.repo.create_file.side_effect = GithubException(404, {}, {})
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        assert [r.name for r in directory.files] == ["f"]

        await self._wait_for_background(directory)

        assert len(directory.files) == 0

    async def test_a_delete_that_lands_while_the_create_retries_wins(
        self, ghc, monkeypatch
    ):
        """A file deleted while its deferred create still retries must not
        reappear after a restart: the delete takes the lock between the
        attempts, removes the ref, and the retries then see it gone and
        never write the blob."""
        from github import GithubException

        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)
        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0.01)
        first_call = threading.Event()
        proceed = threading.Event()
        order: list[str] = []

        def create_file(**_):
            order.append("create")
            if not first_call.is_set():
                first_call.set()
                proceed.wait(timeout=5)
            raise GithubException(500, {}, {})

        ghc.repo.create_file.side_effect = create_file
        ghc.repo.get_contents.return_value = Mock(sha="abc")
        directory = GithubDirectory(ghc, "d", None)

        fr = await directory.create_file_ref_deferred("f", 1)
        for _ in range(100):
            if first_call.is_set():
                break
            await asyncio.sleep(0.01)
        assert first_call.is_set()

        deleter = asyncio.ensure_future(directory.delete_file_ref_async(fr))
        await asyncio.sleep(0.05)
        # The delete waits for the retrying create's lock instead of
        # racing it.
        assert not deleter.done()
        proceed.set()

        await deleter
        before = len(order)
        await self._wait_for_background(directory)

        # The delete made no remote write of its own (the blob leaves in
        # the batched deletions of the next push), and every retry that
        # ran after it saw the ref gone from the tree.
        assert "delete" not in order
        assert order[before:] == []
        assert ghc.pending_drops == {"f.1"}
        assert len(directory.files) == 0

    async def test_a_permanent_failure_leaves_the_path_consistent(
        self, ghc, monkeypatch
    ):
        """When the deferred create gives up and drops the ref while the
        upload is still running, a fresh ref for the same name works and
        nothing is left half-written."""
        from github import GithubException

        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)
        monkeypatch.setattr(gh_directory, "DEFERRED_PUT_RETRY_DELAY", 0)
        ghc.repo.create_file.side_effect = GithubException(500, {}, {})
        directory = GithubDirectory(ghc, "d", None)

        await directory.create_file_ref_deferred("f", 1)
        await self._wait_for_background(directory)
        assert len(directory.files) == 0

        ghc.repo.create_file.side_effect = None  # GitHub recovered
        await directory.create_file_ref_deferred("f", 99)
        await self._wait_for_background(directory)

        assert [r.message_id for r in directory.files] == [99]
        ghc.repo.create_file.assert_called_with(
            path="f.99",
            message="Create file reference for f",
            content="",
            branch="main",
        )

    async def test_a_plain_directory_defers_to_the_sync_path(self):
        a = TGFSDirectory("a", None, [], [])

        fr = await a.create_file_ref_deferred("f", 1)

        assert [r.name for r in a.files] == ["f"]
        assert fr.message_id == 1

    async def test_a_burst_commits_in_one_tree_commit(self, ghc, monkeypatch):
        """A bulk upload schedules a create per file it opens; they land in
        one commit for the batch instead of one commit per reference."""
        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        calls: list[tuple[str, list[str]]] = []

        def capture_overlay(repo, branch, overlay, message):
            calls.append(
                (message, [element._InputGitTreeElement__path for element in overlay])
            )

        monkeypatch.setattr(gh_directory, "commit_tree_overlay", capture_overlay)
        directory = GithubDirectory(ghc, "d", None)

        await asyncio.gather(
            *(directory.create_file_ref_deferred(f"f{i}", i) for i in range(6))
        )
        await self._wait_for_background(directory)

        ghc.repo.create_file.assert_not_called()
        assert len(calls) == 1
        message, paths = calls[0]
        assert paths == sorted(f"f{i}.{i}" for i in range(6))
        assert "6" in message
        assert [r.name for r in directory.files] == [f"f{i}" for i in range(6)]

    async def test_a_failed_batch_lands_one_by_one(self, ghc):
        """A batch commit GitHub refuses must not lose any reference: the
        round falls back to a create per reference."""
        from github import GithubException

        ghc.repo.get_git_ref.side_effect = GithubException(422, {}, {})
        directory = GithubDirectory(ghc, "d", None)

        await asyncio.gather(
            *(directory.create_file_ref_deferred(f"f{i}", i) for i in range(3))
        )
        await self._wait_for_background(directory)

        assert [r.name for r in directory.files] == ["f0", "f1", "f2"]
        assert ghc.repo.create_file.call_count == 3

    async def test_a_batch_that_gives_up_drops_its_references(self, ghc):
        from github import GithubException

        ghc.repo.get_git_ref.side_effect = GithubException(422, {}, {})
        ghc.repo.create_file.side_effect = GithubException(404, {}, {})
        directory = GithubDirectory(ghc, "d", None)

        await asyncio.gather(
            *(directory.create_file_ref_deferred(f"f{i}", i) for i in range(2))
        )
        await self._wait_for_background(directory)

        assert len(directory.files) == 0


class TestDeleteFileRefAsync:
    async def test_defers_the_blob_removal_to_the_next_push(self, ghc):
        """A waited-on delete used to cost two API calls on the writer
        thread per file; now it only registers the blob paths, and the
        next push takes them out in one commit."""
        ghc.repo.get_contents.return_value = Mock(sha="abc")
        ghc.repo.delete_file.side_effect = lambda **_: time.sleep(0.3)
        directory = GithubDirectory(ghc, "d", None)
        fr = directory.create_file_ref("f", 7)
        ghc.repo.create_file.reset_mock()

        await directory.delete_file_ref_async(fr)

        ghc.repo.delete_file.assert_not_called()
        ghc.repo.get_contents.assert_not_called()
        assert ghc.pending_drops == {"f.7"}
        assert len(directory.files) == 0

    async def test_missing_blob_still_drops_the_ref(self, ghc):
        ghc.repo.get_contents.side_effect = Exception("404")
        directory = GithubDirectory(ghc, "d", None)
        fr = directory.create_file_ref("f", 7)

        await directory.delete_file_ref_async(fr)

        assert len(directory.files) == 0


class TestRelocateFileRefAsync:
    async def test_moves_the_ref_between_github_directories(self, ghc):
        ghc.repo.get_contents.return_value = Mock(sha="abc")
        src = GithubDirectory(ghc, "src", None)
        dst = GithubDirectory(ghc, "dst", None)
        fr = src.create_file_ref("f", 7)
        fr.mirrors = {"dc": 9}
        fr.store = "tg"

        moved = await src.relocate_file_ref_async(fr, dst, "g")

        assert [r.name for r in dst.files] == ["g"]
        assert moved.message_id == 7
        assert moved.mirrors == {"dc": 9}
        assert moved.store == "tg"
        assert len(src.files) == 0

    async def test_failed_delete_rolls_the_new_ref_back(self, ghc):
        src = GithubDirectory(ghc, "src", None)
        dst = GithubDirectory(ghc, "dst", None)
        fr = src.create_file_ref("f", 7)
        ghc.repo.get_contents.return_value = Mock(sha="abc")

        async def boom(_):
            raise RuntimeError("boom")

        src.delete_file_ref_async = boom  # type: ignore[method-assign]

        with pytest.raises(RuntimeError):
            await src.relocate_file_ref_async(fr, dst)

        assert len(dst.files) == 0
        assert len(src.files) == 1


class TestPlainDirectoryAsync:
    """A directory without a remote repository just runs the sync methods."""

    async def test_create_delete_relocate(self):
        a = TGFSDirectory("a", None, [], [])
        b = TGFSDirectory("b", None, [], [])

        fr = await a.create_file_ref_async("f", 1)
        assert [r.name for r in a.files] == ["f"]

        moved = await a.relocate_file_ref_async(fr, b)
        assert [r.name for r in b.files] == ["f"] and not a.files

        await b.delete_file_ref_async(moved)
        assert not b.files


class TestPutRetries:
    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch):
        from vulquantafs.core.repository.impl.metadata.github_repo import gh_directory

        monkeypatch.setattr(gh_directory, "PUT_RETRY_DELAY", 0)

    @staticmethod
    def _outcomes(ghc, names):
        import requests
        from github import GithubException

        table = {
            "conn": requests.exceptions.ConnectionError("Max retries"),
            "422": GithubException(
                422,
                {
                    "message": 'Invalid request.\n\n"sha" wasn\'t supplied.',
                    "status": "422",
                },
                {},
            ),
            "422-other": GithubException(422, {"message": "Invalid request."}, {}),
            "500": GithubException(500, {}, {}),
            "404": GithubException(404, {}, {}),
            "ok": None,
        }
        ghc.repo.create_file.side_effect = [table[n] for n in names]

    def test_a_dropped_connection_is_retried(self, ghc):
        self._outcomes(ghc, ["conn", "ok"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 2

    def test_an_attempt_that_landed_is_not_an_error(self, ghc):
        self._outcomes(ghc, ["conn", "422"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)

    def test_a_422_on_the_first_attempt_means_the_blob_is_already_there(self, ghc):
        self._outcomes(ghc, ["422"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 1

    def test_a_plain_422_after_an_earlier_attempt_is_accepted(self, ghc):
        self._outcomes(ghc, ["conn", "422-other"])
        GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 2

    def test_an_unrelated_422_is_still_an_error(self, ghc):
        from github import GithubException

        self._outcomes(ghc, ["422-other"])
        with pytest.raises(GithubException):
            GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 1

    def test_a_refusal_is_not_retried(self, ghc):
        from github import GithubException

        self._outcomes(ghc, ["404"])
        with pytest.raises(GithubException):
            GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 1

    def test_gives_up_after_the_attempts(self, ghc):
        from github import GithubException

        self._outcomes(ghc, ["500", "500", "500"])
        with pytest.raises(GithubException):
            GithubDirectory(ghc, "d", None)._put_file_ref("a", 1)
        assert ghc.repo.create_file.call_count == 3
