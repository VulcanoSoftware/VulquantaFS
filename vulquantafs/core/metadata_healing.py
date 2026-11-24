"""Periodic catch-up of the redundant metadata stores.

Healthy GitLab mirrors batch normal pushes here; failed stores are marked
stale and retried on the same interval. Bulk uploads need not wait for a
mirror's rate-limit cooldown or full-tree sync as long as another store
took the write. A lagging store is brought in line without waiting for
user activity, and a restart can fail over to whichever store still answers.

File stores have their own healing (the replication queue and the
backfill); this is exclusively about the metadata stores.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from vulquantafs.core.client import Clients

logger = logging.getLogger(__name__)

# How often the stores that missed a write are retried.
HEAL_INTERVAL_SECONDS = 30.0


class MetadataHealWorker:
    """Runs :meth:`MetaDataApi.heal` for every file system, every interval
    and once right after start. Runs on the event loop; a heal is a
    handful of writes at most and every store that is up to date costs
    nothing."""

    def __init__(self, clients: "Clients", interval: float = HEAL_INTERVAL_SECONDS):
        self._clients = clients
        self._interval = interval
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="metadata-heal")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run_once(self) -> Dict[str, int]:
        """Heal every file system; returns how many stores per file system
        still await healing afterwards."""
        report: Dict[str, int] = {}
        for name, client in self._clients.items():
            if client.metadata_api is None:
                continue
            try:
                await client.metadata_api.heal()
            except Exception:
                logger.exception(f"Metadata heal for file system {name} failed")
            report[name] = len(client.metadata_api.stale_stores())
        return report

    async def run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(self._interval)
