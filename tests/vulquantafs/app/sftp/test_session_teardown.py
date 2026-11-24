"""A real SSH/SFTP session that goes away mid-upload.

asyncssh closes every handle that is still open when a session ends --
through ``SFTPServer.close``, *before* ``exit`` -- so a dropped client
looks like a client that finished. These tests use a genuine asyncssh
client and server to pin down that a dropped upload is aborted and never
committed as a (truncated) version, which is what a retrying client such
as FileZilla would otherwise resume from.
"""

import asyncio
import logging
from typing import List
from unittest.mock import Mock

import asyncssh
import pytest

from tests.vulquantafs.app.sftp.test_sftp_server import build_client
from vulquantafs.app.sftp import server as sftp_server
from vulquantafs.app.sftp.handles import WriteHandle
from vulquantafs.app.sftp.server import TGFSSFTPServer
from vulquantafs.auth.user import AdminUser
from vulquantafs.config import SFTPConfig
from vulquantafs.core import Ops
from vulquantafs.errors import FileOrDirectoryDoesNotExist


class Recorder:
    """What the fake stores saw of the uploads."""

    def __init__(self) -> None:
        self.committed: List[int] = []  # byte counts of finished uploads
        self.cancelled = 0
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.hold_upload = False


def make_ops_class(recorder: Recorder):
    def factory(client):
        ops = Mock(spec=Ops)

        def stat_file(path):
            raise FileOrDirectoryDoesNotExist(path)

        async def upload_from_stream(stream, size, remote):
            received = 0
            recorder.started.set()
            try:
                if recorder.hold_upload:
                    await asyncio.Event().wait()
                async for chunk in stream:
                    received += len(chunk)
            except asyncio.CancelledError:
                recorder.cancelled += 1
                recorder.stopped.set()
                raise
            recorder.committed.append(received)

        def cd(path):
            if path in ("", "/"):
                return client.dir_api.root
            raise FileOrDirectoryDoesNotExist(path)

        ops.cd = cd
        ops.stat_file = stat_file
        ops.upload_from_stream = upload_from_stream
        return ops

    return factory


class NoAuthServer(asyncssh.SSHServer):
    def begin_auth(self, username: str) -> bool:
        return False


@pytest.fixture
async def session(mocker):
    recorder = Recorder()
    mocker.patch.object(sftp_server, "Ops", make_ops_class(recorder))

    config = Mock()
    config.vulquantafs.sftp = SFTPConfig.from_dict({"upload_buffer_size_mb": 1})
    client = build_client(mocker, "notes")

    def sftp_factory(chan):
        server = TGFSSFTPServer(chan, {"notes": client}, config)
        server._user = AdminUser("admin")
        return server

    key = asyncssh.generate_private_key("ssh-rsa")
    listener = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=NoAuthServer,
        server_host_keys=[key],
        sftp_factory=sftp_factory,
    )
    port = listener.sockets[0].getsockname()[1]
    conn = await asyncssh.connect("127.0.0.1", port, known_hosts=None, username="x")
    try:
        yield recorder, conn
    finally:
        conn.abort()
        listener.close()
        await listener.wait_closed()


async def settle(seconds: float = 0.5) -> None:
    await asyncio.sleep(seconds)


async def test_a_finished_upload_is_committed(session):
    recorder, conn = session
    sftp = await conn.start_sftp_client()

    async with sftp.open("/notes/a.bin", "wb") as handle:
        await handle.write(b"x" * 100_000, 0)
    await settle()

    assert recorder.committed == [100_000]
    assert recorder.cancelled == 0


@pytest.mark.parametrize("how", ["abort", "close"])
async def test_a_dropped_upload_is_aborted_not_committed(session, how):
    recorder, conn = session
    sftp = await conn.start_sftp_client()
    handle = await sftp.open("/notes/b.bin", "wb")
    await handle.write(b"x" * 100_000, 0)
    await asyncio.wait_for(recorder.started.wait(), 5)

    if how == "abort":
        conn.abort()  # the TCP connection just dies, like a FileZilla timeout
    else:
        conn.close()
    await settle(1.0)

    assert recorder.committed == []
    assert recorder.cancelled == 1


async def test_a_dropped_upload_does_not_flood_the_log(session, caplog):
    recorder, conn = session
    sftp = await conn.start_sftp_client()
    handle = await sftp.open("/notes/c.bin", "wb")
    await handle.write(b"x" * 100_000, 0)
    await asyncio.wait_for(recorder.started.wait(), 5)

    with caplog.at_level(logging.ERROR, logger="vulquantafs.app.sftp.server"):
        conn.abort()
        await settle(1.0)

    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize("request_kind", ["write", "close"])
async def test_disconnect_unblocks_a_request_waiting_on_a_full_queue(
    session, monkeypatch, request_kind
):
    recorder, conn = session
    recorder.hold_upload = True
    blocked = asyncio.Event()
    original = WriteHandle._enqueue

    async def observe_enqueue(self, data):
        if data in (None, b"second"):
            blocked.set()
        await original(self, data)

    monkeypatch.setattr(WriteHandle, "_enqueue", observe_enqueue)
    sftp = await conn.start_sftp_client()
    handle = await sftp.open("/notes/stalled.bin", "wb")
    await handle.write(b"first", 0)  # fills the queue; the backend reads nothing
    await asyncio.wait_for(recorder.started.wait(), 2)
    pending = asyncio.create_task(
        handle.write(b"second", 5) if request_kind == "write" else handle.close()
    )
    try:
        await asyncio.wait_for(blocked.wait(), 2)
        assert not pending.done()
        conn.abort()
        await asyncio.wait_for(recorder.stopped.wait(), 2)
        assert recorder.committed == []
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
