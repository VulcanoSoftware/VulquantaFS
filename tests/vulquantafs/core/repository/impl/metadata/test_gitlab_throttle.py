import asyncio
import time
from email.utils import formatdate
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from vulquantafs.config import GitlabRepoConfig
from vulquantafs.core.repository.impl.metadata import gitlab_throttle
from vulquantafs.core.repository.impl.metadata.gitlab_repo import (
    GitlabRepoMetadataRepository,
)
from vulquantafs.core.repository.impl.metadata.gitlab_throttle import (
    GitlabRateLimited,
    GitlabRequestGate,
)
from vulquantafs.core.repository.impl.metadata.gitlab_tree import GitlabRepository


def test_late_responses_extend_the_shared_cooldown_but_do_not_reset_it(monkeypatch):
    clock = SimpleNamespace(now=100.0, wall=10_000.0)
    monkeypatch.setattr(
        gitlab_throttle,
        "time",
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: clock.wall),
    )
    monkeypatch.setattr(gitlab_throttle.random, "uniform", lambda _low, _high: 1)
    gate = GitlabRequestGate("gitlab.test", 10)
    first = gate.acquire()
    clock.now = 101
    late = gate.acquire()
    clock.now = 102
    success = gate.acquire()
    failure = gate.finish(first, 429, {})
    assert failure is not None and failure.retry_at == 103
    gate.finish(success, 200, {})
    failure = gate.finish(
        late,
        429,
        {
            "Retry-After": formatdate(clock.wall + 300, usegmt=True),
            "RateLimit-Reset": str(clock.wall + 600),
        },
    )
    assert failure is not None and failure.retry_at == 702
    clock.now = 701
    with pytest.raises(GitlabRateLimited):
        gate.acquire()
    clock.now = 702
    probe = gate.acquire()
    clock.now = 703
    with pytest.raises(GitlabRateLimited):
        gate.acquire()
    failure = gate.finish(probe, 429, {"Retry-After": "NaN"})
    assert failure is not None and failure.retry_at == 705
    clock.now = 705
    gate.finish(gate.acquire(), 200, {})
    clock.now = 706
    failure = gate.finish(gate.acquire(), 429, {})
    assert failure is not None and failure.retry_at == 707


async def test_both_transports_share_pacing_and_cooldown_over_local_http():
    received: list[float] = []

    async def gitlab(request):
        received.append(time.monotonic())
        if len(received) == 1:
            return web.json_response({}, status=429, headers={"Retry-After": "1.1"})
        return web.json_response({})

    app = web.Application()
    app.router.add_get("/{path:.*}", gitlab)
    async with TestServer(app) as server:
        cfg = GitlabRepoConfig(
            "group/tree",
            "main",
            "tree-token",
            base_url=str(server.make_url("")).rstrip("/"),
            requests_per_second=20,
        )
        tree = GitlabRepository(cfg, wait_on_rate_limit=False)
        doc = GitlabRepoMetadataRepository(
            GitlabRepoConfig(
                "other/document",
                "main",
                "document-token",
                base_url=cfg.base_url,
                requests_per_second=20,
            )
        )

        async def read_doc():
            return await doc._request("GET", doc._file_url)

        try:
            with pytest.raises(GitlabRateLimited) as exc:
                await asyncio.to_thread(tree.probe)
            for _ in range(4):
                with pytest.raises(GitlabRateLimited):
                    await read_doc()
            assert len(received) == 1
            await asyncio.sleep(max(0.0, exc.value.retry_at - time.monotonic()))
            await asyncio.to_thread(tree.probe)
            await asyncio.gather(
                asyncio.to_thread(tree.probe),
                read_doc(),
                asyncio.to_thread(tree.probe),
                read_doc(),
            )
            assert len(received) == 6
            assert received[1] - received[0] >= 1.09
            assert received[-1] - received[1] >= 0.18
        finally:
            await doc.close()
