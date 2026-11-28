"""``MetaDataApi``: coalesced metadata pushes.

Coalesced mode merges the push requests of mutations that arrive within a
window into one store write, so a burst of operations pays one mirror
sync instead of one per operation. Every caller still waits for the push
that covers its change; push failures are logged, not raised.
"""

import asyncio
import sqlite3

import pytest

from vulquantafs.core.api.metadata import UPLOAD_BATCH_SIZE, MetaDataApi
from vulquantafs.core.api.removal_journal import RemovalJournal
from vulquantafs.core.api.upload_journal import UploadJournal
from vulquantafs.core.model import TGFSDirectory, TGFSMetadata
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.errors import NoStoredMetadata, TechnicalError


class CountingRepo(IMetaDataRepository):
    """The push surface of a metadata repository, counted and optionally
    failing; these tests touch nothing else of the interface."""

    push_writes_stats = False

    def __init__(self):
        super().__init__()
        self.pushes = 0
        self.maintains = 0
        self.fail: Exception | None = None
        self.maintain_fail: Exception | None = None

    async def push(self) -> None:
        self.pushes += 1
        if self.fail is not None:
            raise self.fail

    async def maintain(self) -> None:
        self.maintains += 1
        if self.maintain_fail is not None:
            raise self.maintain_fail

    async def get(self):
        raise NoStoredMetadata("fake")


class GatedRepo(CountingRepo):
    """A repo whose push blocks until the test lets it finish, so the
    timing of registrations during a running push is deterministic."""

    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def push(self) -> None:
        self.pushes += 1
        self.entered.set()
        await self.release.wait()
        self.release.clear()
        if self.fail is not None:
            raise self.fail


async def test_removals_replay_from_a_stale_remote_after_restart(tmp_path):
    class Repo(CountingRepo):
        async def get(self):
            root = TGFSDirectory.root_dir()
            root.create_file_ref("one", 11)
            root.create_file_ref("two", 22)
            root.create_file_ref("recreated", 99)
            return TGFSMetadata(root)

    path = str(tmp_path / "removes.sqlite3")
    first = RemovalJournal(path, b"key")
    root = TGFSDirectory.root_dir()
    first.add(root.create_file_ref("one", 11))
    first.add(root.create_file_ref("two", 22))
    first.add(root.create_file_ref("recreated", 33))
    first.close()  # no remote sync took place before the previous exit

    repo = Repo()
    journal = RemovalJournal(path, b"key")
    api = MetaDataApi(repo, push_mode="inline", removal_journal=journal)
    await api.init()
    root = api.get_root_directory()
    assert [ref.name for ref in root.find_files()] == ["recreated"]
    assert not api.defer_removal(root.find_file("recreated"))
    repo.fail = TechnicalError("unavailable")
    with pytest.raises(TechnicalError, match="unavailable"):
        await api.flush_removals()
    assert len(journal.load()[1]) == 3

    repo.fail = None
    await api.flush_removals()
    assert journal.load() == (0, set())
    await api.close()


async def test_failed_journal_write_rolls_back_the_file_reference(
    tmp_path, monkeypatch
):
    class Repo(CountingRepo):
        async def get(self):
            return TGFSMetadata(TGFSDirectory.root_dir())

    journal = UploadJournal(str(tmp_path / "uploads.sqlite3"), None)
    api = MetaDataApi(Repo(), upload_journal=journal)
    await api.init()
    root = api.get_root_directory()

    def disk_full(_ref):
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(journal, "add", disk_full)
    try:
        with pytest.raises(sqlite3.OperationalError, match="disk is full"):
            api.stage_upload(root, "not-acknowledged", 1, {}, None)
        assert not root.find_files()
        assert journal.load() == []
    finally:
        await api.close()


async def test_upload_batches_retry_without_losing_uploads_staged_in_flight(tmp_path):
    class Repo(CountingRepo):
        async def get(self):
            return TGFSMetadata(TGFSDirectory.root_dir())

        async def push_upload_refs(self, refs, *, full=False):
            attempts.append([ref.message_id for ref in refs])
            if len(attempts) == 1:
                entered.set()
                await release.wait()
                raise OSError("primary unavailable")
            if len(attempts) == 4:
                done.set()

    entered, release, done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    attempts: list[list[int]] = []
    path = tmp_path / "uploads.sqlite3"
    journal = UploadJournal(str(path), None)
    api = MetaDataApi(Repo(), upload_journal=journal)
    await api.init()
    root = api.get_root_directory()
    try:
        first = UPLOAD_BATCH_SIZE
        for id_ in range(first):
            api.stage_upload(root, str(id_), id_, {}, None)
        await asyncio.wait_for(entered.wait(), 2)
        total = 2 * first + 48
        for id_ in range(first, total):
            api.stage_upload(root, str(id_), id_, {}, None)
        assert len(journal.load()) == total
        assert path.stat().st_mode & 0o777 == 0o600
        release.set()
        await asyncio.wait_for(done.wait(), 5)
        assert attempts[0] == attempts[1] == list(range(first))
        assert [id_ for batch in attempts[1:] for id_ in batch] == list(range(total))
        assert [len(batch) for batch in attempts[1:]] == [first, first, 48]
        assert not journal.load()
    finally:
        await api.close()


async def test_acting_store_uploads_survive_a_restart_with_their_store(tmp_path):
    """A journaled upload from the acting store comes back naming that store.

    While the primary is down the bytes live on the acting store, so the
    reference has to carry its key; a restart that published it as a
    primary-owned file would point readers at a message that is not there.
    """
    published: list[tuple[str, str | None]] = []

    class Repo(CountingRepo):
        async def get(self):
            return TGFSMetadata(TGFSDirectory.root_dir())

        async def push_upload_refs(self, refs, *, full=False):
            published.extend((ref.name, ref.store) for ref in refs)

    path = str(tmp_path / "uploads.sqlite3")
    root = TGFSDirectory.root_dir()
    ref = root.create_file_ref("512KB.txt", 555)
    ref.mirrors = {"tg:1": 556}
    ref.store = "dc:9"
    journal = UploadJournal(path, None)
    journal.add(ref)
    journal.close()  # the process died before the batch reached GitHub

    api = MetaDataApi(Repo(), upload_journal=UploadJournal(path, None))
    await api.init()
    try:
        restored = api.get_root_directory().find_file("512KB.txt")
        assert restored.store == "dc:9"
        await api.flush_uploads(directory=api.get_root_directory())
        assert published == [("512KB.txt", "dc:9")]
        assert UploadJournal(path, None).load() == []
    finally:
        await api.close()


async def test_mutation_flush_targets_only_its_subtree(tmp_path):
    class Repo(CountingRepo):
        async def get(self):
            return TGFSMetadata(TGFSDirectory.root_dir())

        async def push_upload_refs(self, refs, *, full=False):
            pushed.extend(ref.name for ref in refs)

    pushed: list[str] = []
    journal = UploadJournal(str(tmp_path / "uploads.sqlite3"), None)
    api = MetaDataApi(Repo(), upload_journal=journal)
    await api.init()
    root = api.get_root_directory()
    moving = root.create_dir("moving")
    try:
        api.stage_upload(root, "unrelated", 1, {}, None)
        api.stage_upload(moving, "in-subtree", 2, {}, None)
        await api.flush_uploads(directory=moving)
        moving.move_to(root, "moved")
        assert pushed == ["in-subtree"]
        assert [row.name for row in journal.load()] == ["unrelated"]
    finally:
        await api.close()


async def test_inline_mode_pushes_directly():
    repo = CountingRepo()
    api = MetaDataApi(repo)
    await api.push()
    await api.push()
    assert repo.pushes == 2


async def test_overlapping_pushes_share_one_write():
    repo = CountingRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0.1)
    await asyncio.gather(*(api.push() for _ in range(5)))
    assert repo.pushes == 1


async def test_a_requested_push_writes_in_the_background():
    repo = CountingRepo()
    api = MetaDataApi(repo)
    api.request_push()
    await asyncio.sleep(0.01)
    assert repo.pushes == 1


async def test_a_requested_push_during_a_running_write_is_covered():
    repo = GatedRepo()
    api = MetaDataApi(repo)
    waiter = asyncio.create_task(api.push())
    await repo.entered.wait()
    api.request_push()
    repo.release.set()
    await asyncio.wait_for(waiter, 2)
    await asyncio.sleep(0.01)
    assert repo.pushes == 2


async def test_a_needed_push_does_not_wait_for_the_write():
    repo = GatedRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    api.request_push()
    # No await on the push: the request returns immediately.
    assert not repo.entered.is_set()
    repo.release.set()
    await asyncio.wait_for(asyncio.sleep(0), 2)
    await asyncio.sleep(0.01)
    assert repo.pushes == 1


async def test_a_needed_push_merges_into_the_running_window():
    repo = CountingRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0.01)
    waiter = asyncio.create_task(api.push())
    await asyncio.sleep(0)
    api.request_push()
    # The request rides along with the window's write; it is real but
    # never waited on.
    await asyncio.wait_for(waiter, 2)
    assert repo.pushes == 1


async def test_a_needed_push_during_a_running_write_gets_its_own_write():
    repo = GatedRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    api.request_push()
    await repo.entered.wait()
    api.request_push()
    repo.release.set()
    await asyncio.sleep(0.01)
    assert repo.pushes == 2


async def test_a_needed_push_does_not_hang_the_close():
    repo = CountingRepo()
    api = MetaDataApi(repo)
    api.request_push()
    await asyncio.sleep(0.01)
    repo.pushes = 0
    await api.close()


async def test_close_flushes_a_needed_inline_write():
    repo = GatedRepo()
    api = MetaDataApi(repo)
    api.request_push()
    await repo.entered.wait()
    # While a background write is under way the close still finishes and
    # records the requested change.
    closer = asyncio.create_task(api.close())
    await asyncio.sleep(0.01)
    repo.release.set()
    await asyncio.wait_for(closer, 2)
    assert repo.pushes >= 1


async def test_window_zero_still_coalesces_one_loop_turn():
    repo = CountingRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    await asyncio.gather(*(api.push() for _ in range(4)))
    assert repo.pushes == 1


async def test_a_push_during_a_running_write_gets_its_own_write():
    repo = GatedRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    first = asyncio.create_task(api.push())
    await repo.entered.wait()
    second = asyncio.create_task(api.push())
    await asyncio.sleep(0.01)
    repo.release.set()
    await asyncio.wait_for(first, 2)
    assert not second.done()
    repo.release.set()
    await asyncio.wait_for(second, 2)
    assert repo.pushes == 2


async def test_a_failed_push_does_not_raise_and_does_not_strand_the_callers():
    repo = CountingRepo()
    repo.fail = TechnicalError("every store failed")
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    # Returns normally: the operation behind the push already happened.
    await asyncio.gather(*(api.push() for _ in range(3)))
    assert repo.pushes == 1


async def test_only_strict_waiters_see_a_failed_write_and_can_retry():
    repo = CountingRepo()
    repo.fail = TechnicalError("every store failed")
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    try:
        strict, ordinary = await asyncio.gather(
            api.push(require_success=True), api.push(), return_exceptions=True
        )
        assert isinstance(strict, TechnicalError)
        assert str(strict) == "every store failed"
        assert ordinary is None
        assert repo.pushes == 1
        repo.fail = None
        await api.push(require_success=True)
        assert repo.pushes == 2
    finally:
        await api.close()


async def test_a_batch_writes_at_block_exit_despite_the_coalescer():
    repo = CountingRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=5)
    async with api.batch():
        await api.push()
        await api.push()
        assert repo.pushes == 0
    assert repo.pushes == 1


async def test_close_flushes_a_push_still_in_its_window():
    repo = CountingRepo()
    api = MetaDataApi(repo, push_mode="coalesced", push_window=60)
    waiter = asyncio.create_task(api.push())
    await asyncio.sleep(0.05)
    assert repo.pushes == 0

    await api.close()

    assert repo.pushes == 1
    await asyncio.wait_for(waiter, 2)


async def test_heal_runs_the_repository_maintenance():
    repo = CountingRepo()
    api = MetaDataApi(repo)

    await api.heal()

    assert repo.maintains == 1


async def test_heal_logs_a_failing_maintenance_and_returns():
    repo = CountingRepo()
    repo.maintain_fail = TechnicalError("compaction refused")
    api = MetaDataApi(repo)

    await api.heal()

    assert repo.pushes == 0
