"""``MetaDataApi``: coalesced metadata pushes.

Coalesced mode merges the push requests of mutations that arrive within a
window into one store write, so a burst of operations pays one mirror
sync instead of one per operation. Every caller still waits for the push
that covers its change; push failures are logged, not raised.
"""

import asyncio

import pytest

from vulquantafs.core.api.metadata import MetaDataApi
from vulquantafs.core.api.removal_journal import RemovalJournal
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
