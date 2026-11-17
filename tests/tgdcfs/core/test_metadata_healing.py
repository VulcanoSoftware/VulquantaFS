"""The periodic metadata heal worker."""

import asyncio
from types import SimpleNamespace

from tgdcfs.core.metadata_healing import MetadataHealWorker


class FakeClient:
    metadata_api: SimpleNamespace | None

    def __init__(self, stale=(), heal_error=None, with_metadata_api=True):
        self.heal_calls = 0
        self._heal_error = heal_error
        if with_metadata_api:
            self.metadata_api = SimpleNamespace(
                heal=self._heal, stale_stores=lambda: list(stale)
            )
        else:
            self.metadata_api = None

    async def _heal(self):
        self.heal_calls += 1
        if self._heal_error is not None:
            raise self._heal_error


class Clients(dict):
    """The real registry is a dict of name -> client."""


class TestMetadataHealWorker:
    async def test_run_once_heals_every_file_system(self):
        clients = Clients(
            {
                "fs1": FakeClient(stale=["mirror-a"]),
                "fs2": FakeClient(),
            }
        )
        worker = MetadataHealWorker(clients)

        report = await worker.run_once()

        assert report == {"fs1": 1, "fs2": 0}
        assert clients["fs1"].heal_calls == 1
        assert clients["fs2"].heal_calls == 1

    async def test_a_failing_heal_is_reported_not_raised(self):
        clients = Clients({"fs1": FakeClient(heal_error=RuntimeError("still down"))})
        worker = MetadataHealWorker(clients)

        report = await worker.run_once()

        assert report == {"fs1": 0}

    async def test_a_file_system_without_metadata_api_is_skipped(self):
        clients = Clients(
            {"fs0": FakeClient(with_metadata_api=False), "fs1": FakeClient()}
        )

        worker = MetadataHealWorker(clients)
        report = await worker.run_once()

        assert report == {"fs1": 0}

    async def test_start_runs_once_and_then_on_the_interval(self):
        clients = Clients({"fs1": FakeClient()})
        worker = MetadataHealWorker(clients, interval=0.01)
        worker.start()
        try:
            await asyncio.sleep(0.05)
        finally:
            await worker.stop()

        assert clients["fs1"].heal_calls >= 2
        assert worker._task is None

    async def test_stop_without_start_is_a_noop(self):
        worker = MetadataHealWorker(Clients())
        await worker.stop()
