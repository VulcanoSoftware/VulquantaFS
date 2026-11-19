"""End to end: writes fail over to a mirror while the primary store is
down, and the recovered primary is filled back in afterwards.

A Telegram primary with a Discord mirror and redundant pinned metadata.
In tee mode, the same upload finishes on Discord. In inline mode, a fresh
request goes directly to Discord after the first write fails. The replica
and descriptor move back to Telegram once it accepts writes again.
"""

import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import List, Tuple, cast

import asyncssh
import pytest

from tests.fakes.store import FakeStore
from tests.tgdcfs.backends.discord.test_store import FakeBot, FakeChannel
from tgdcfs.app.sftp.server import TGFSSFTPServer
from tgdcfs.auth.user import AdminUser
from tgdcfs.backends.discord.store import DiscordStore
from tgdcfs.backends.telegram import store as telegram_store
from tgdcfs.backends.telegram.store import TelegramStore
from tgdcfs.config import Config, EncryptionConfig, TransferConfig
from tgdcfs.core.client import Client
from tgdcfs.core.replication import ReplicationQueue, ReplicationWorker
from tgdcfs.crypto.header import MAGIC
from tgdcfs.errors import FileOrDirectoryDoesNotExist, TechnicalError
from tgdcfs.reqres import FileMessageFromBuffer, SaveFilePartResp, SendMessageResp
from tgdcfs.utils.message_cache import global_message_cache

pytestmark = pytest.mark.asyncio

# A store that is down is down completely: every remote call raises.
PRIMARY_DOWN = {
    "upload",
    "send_text",
    "edit_message_text",
    "download_file",
    "get_messages",
    "pin_message",
    "get_pinned_message",
    "replace_document",
}


def config_dict() -> dict:
    return {
        "backends": {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
            },
            "discord": {"bot_token": "discord_token"},
        },
        "stores": {
            "tg-main": {"backend": "telegram", "channel": "-1001"},
            "dc-spare": {"backend": "discord", "channel": "9876"},
        },
        "filesystems": {
            "media": {
                "primary": "tg-main",
                "mirrors": ["dc-spare"],
                "sync": "inline",
                "metadata": {"type": "pinned_message"},
                "metadata_mirrors": [{"type": "pinned_message", "store": "dc-spare"}],
            },
        },
        "tgdcfs": {
            "users": {},
            "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
            "server": {"host": "0.0.0.0", "port": 8080},
        },
    }


async def store_factory(store_cfg):
    return FakeStore(
        key=store_cfg.key,
        backend_name=store_cfg.backend,
        max_part_bytes=2048 if store_cfg.backend == "telegram" else 1 << 30,
    )


async def make_client(
    tmp_path=None,
) -> Tuple[Client, FakeStore, FakeStore, ReplicationQueue | None]:
    config = Config.from_dict(config_dict())
    queue = ReplicationQueue(path=str(tmp_path / "queue.json")) if tmp_path else None
    client = await Client.create(
        config.filesystems["media"], config, store_factory, replication=queue
    )
    tg = cast(FakeStore, client.store)
    assert client.mirror_group is not None
    dc = cast(FakeStore, client.mirror_group.stores[0].store)
    return client, tg, dc, queue


def tree(client: Client):
    assert client.metadata_api is not None
    return client.metadata_api.get_root_directory()


def documents(store: FakeStore) -> List[int]:
    return [mid for mid, m in store.messages.items() if m.document is not None]


def texts_containing(store: FakeStore, needle: str) -> int:
    return sum(
        1 for m in store.messages.values() if m.text is not None and needle in m.text
    )


def read(client: Client, fr, name: str) -> bytes:
    async def chunks():
        stream = await client.file_api.retrieve(fr, 0, -1, name)
        async for chunk in stream:
            yield chunk

    return chunks()


class TestWriteFailover:
    @pytest.mark.parametrize("encrypt", [False, True], ids=["plain", "encrypted"])
    async def test_one_sftp_session_survives_primary_loss_and_heals(
        self, tmp_path, mocker, caplog, encrypt
    ):
        caplog.set_level("INFO", logger="tgdcfs.core.replication")
        caplog.set_level("INFO", logger="tgdcfs.backends.telegram.store")
        raw = config_dict()
        raw["filesystems"]["media"]["sync"] = "tee"
        # Discord's message cache is per channel, and the two fake channels
        # have independent message histories.
        channel_id = 98_766 + int(encrypt)
        raw["stores"]["dc-spare"]["channel"] = str(channel_id)
        config = Config.from_dict(raw)
        encryption = EncryptionConfig.from_dict(
            {
                "enabled": encrypt,
                "encrypt_names": encrypt,
                "passphrase": "integration-test",
                "master_salt_file": str(tmp_path / "master.salt"),
                "chunk_size": 1024,
            }
        )
        queue_path = str(tmp_path / "queue.json")
        queue = ReplicationQueue(path=queue_path)
        tg = FakeStore(
            key=config.stores["tg-main"].key,
            backend_name="telegram",
            max_part_bytes=16384,
        )
        discord_channel = FakeChannel()
        discord_sent = asyncio.Event()
        limit_attachment_lookups = False

        class Bot(FakeBot):
            async def send_file(self, channel_id, data, name, caption=""):
                if name.startswith("[part1]"):
                    discord_sent.set()
                return await super().send_file(channel_id, data, name, caption)

            async def get_messages(self, channel_id, message_ids):
                if limit_attachment_lookups and any(
                    self.messages.get(mid, {}).get("name", "").startswith("[part")
                    for mid in message_ids
                ):
                    raise AssertionError("Fetched recently uploaded Discord media")
                return await super().get_messages(channel_id, message_ids)

        dc = DiscordStore(
            [Bot(1, discord_channel), Bot(2, discord_channel)],
            channel_id,
            key=config.stores["dc-spare"].key,
            max_part_bytes=512,
            max_concurrent_uploads=2,
        )

        async def stores(cfg):
            return tg if cfg.backend == "telegram" else dc

        mocker.patch.object(type(config.tgdcfs.transfer), "tee_buffer_bytes", 4096)
        client = await Client.create(
            config.filesystems["media"],
            config,
            stores,
            encryption_cfg=encryption,
            replication=queue,
        )
        data = bytes(range(256)) * 192
        primary_read = asyncio.Event()
        outage = asyncio.Event()
        primary_failed = asyncio.Event()
        publishing = asyncio.Event()
        release_publish = asyncio.Event()
        original_upload = tg.upload
        telegram_chunks: dict[int, dict[int, bytes]] = {}

        class TelegramTransport:
            async def save_file_part(self, req):
                telegram_chunks.setdefault(req.file_id, {})[req.file_part] = req.bytes
                return SaveFilePartResp(success=True)

            async def send_small_file(self, req):
                # As in production, successful part RPCs alone do not
                # publish any document in the channel.
                publishing.set()
                await release_publish.wait()
                chunks = telegram_chunks[req.file.id]
                payload = b"".join(chunks[i] for i in range(req.file.parts))
                return SendMessageResp(
                    message_id=tg._add(document=payload, name=req.name)
                )

        telegram = TelegramStore(SimpleNamespace(next_bot=TelegramTransport()), 1001)  # type: ignore[arg-type]

        async def upload(msg):
            if outage.is_set():
                if tg.fail:
                    return await original_upload(msg)
                return await telegram.upload(msg)
            await msg.open()
            prefix = await msg.read(2048)
            if encrypt:
                assert prefix.startswith(MAGIC)
            else:
                assert prefix == data[:2048]
            primary_read.set()
            await outage.wait()
            primary_failed.set()
            raise TechnicalError("simulated Telegram outage mid-upload")

        mocker.patch.object(tg, "upload", side_effect=upload)

        class NoAuth(asyncssh.SSHServer):
            def begin_auth(self, username: str) -> bool:
                return False

        def factory(chan):
            server = TGFSSFTPServer(chan, {"media": client}, config)
            server._user = AdminUser("admin")
            return server

        async with AsyncExitStack() as stack:
            listener = await stack.enter_async_context(
                asyncssh.listen(
                    "127.0.0.1",
                    0,
                    server_factory=NoAuth,
                    server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
                    sftp_factory=factory,
                )
            )
            conn = await stack.enter_async_context(
                asyncssh.connect(
                    "127.0.0.1", listener.get_port(), username="test", known_hosts=None
                )
            )
            sftp = await stack.enter_async_context(conn.start_sftp_client())
            handle = await sftp.open("/media/outage.bin", "wb")
            await asyncio.wait_for(handle.write(data[:8192], 0), 3)
            await asyncio.wait_for(primary_read.wait(), 3)
            tg.fail = set(PRIMARY_DOWN)
            outage.set()
            await asyncio.wait_for(primary_failed.wait(), 3)
            for offset in range(8192, len(data), 8192):
                await asyncio.wait_for(
                    handle.write(data[offset : offset + 8192], offset), 3
                )
            await asyncio.wait_for(handle.close(), 3)
            assert discord_sent.is_set()
            async with await sftp.open("/media/outage.bin", "rb") as downloaded:
                assert await downloaded.read() == data

        fr = tree(client).find_file("outage.bin")
        failed_over = (await client.file_api.desc(fr)).get_latest_version()
        assert failed_over.store == dc.key and fr.store == dc.key
        assert len(failed_over.message_ids) > 90
        stored = b"".join(
            discord_channel.messages[mid]["data"] for mid in failed_over.message_ids
        )
        if encrypt:
            assert stored.startswith(MAGIC)
            assert data not in stored
        else:
            assert stored == data
        assert [item.path for item in queue.pending("media")] == ["/outage.bin"]
        # Reload the persisted queue before exercising the recovery path.
        persisted = ReplicationQueue(path=queue_path)
        worker = ReplicationWorker(client, persisted)
        assert not await worker.process_one("/outage.bin")
        assert persisted.pending("media")

        tg.fail.clear()
        limit_attachment_lookups = True
        mocker.patch.object(TransferConfig, "upload_part_size_bytes", 4096)
        foreground_deadline = mocker.spy(telegram_store, "bounded_part")
        persisted.retry_now()
        worker.start()
        try:
            await asyncio.wait_for(publishing.wait(), 3)
            foreground_deadline.assert_not_called()
            assert fr.store == dc.key and persisted.pending("media")
        finally:
            await worker.stop()

        # Simulate stopping the server while Telegram is publishing,
        # then resume the on-disk work without re-uploading from SFTP.
        persisted = ReplicationQueue(path=queue_path)
        worker = ReplicationWorker(client, persisted)
        assert [item.path for item in persisted.pending("media")] == ["/outage.bin"]
        global_message_cache.pop(channel_id, None)
        assert await worker.run_once() == 0  # The cold cache hits the simulated limit.
        assert [item.path for item in persisted.pending("media")] == ["/outage.bin"]

        limit_attachment_lookups = False
        persisted.retry_now()
        release_publish.set()
        assert await worker.run_once() == 1
        assert not persisted.pending("media")
        assert "interrupted; still queued" in caplog.text
        assert "Telegram published [part1]" in caplog.text
        assert "(1 versions restored to primary)" in caplog.text
        healed = (await client.file_api.desc(fr)).get_latest_version()
        assert healed.id == failed_over.id and healed.store == tg.key
        assert fr.store == tg.key and healed.has_copy_in(dc.key)
        assert len(healed.message_ids) < len(failed_over.message_ids)
        assert b"".join(tg.document_of(mid) for mid in healed.message_ids) == stored
        for mid in healed.message_ids:
            name = tg.messages[mid].name
            if encrypt:
                assert "]TGFS1_" in name
                assert "outage.bin" not in name
            else:
                assert name.endswith("]outage.bin")

    async def test_sftp_paces_writes_and_waits_for_discord_to_confirm_close(self):
        raw = config_dict()
        raw["filesystems"]["media"]["sync"] = "tee"
        config = Config.from_dict(raw)
        tg = await store_factory(config.stores["tg-main"])
        channel = FakeChannel()
        part_size = 128 * 1024
        full_parts = 16
        data = bytes(range(256)) * (full_parts * part_size // 256) + b"tail"
        started: list[int] = []
        first_batch = asyncio.Event()
        last_attachment = asyncio.Event()
        allow_data = asyncio.Event()
        allow_last = asyncio.Event()

        class Bot(FakeBot):
            async def send_file(self, channel_id, data, name, caption=""):
                if name.endswith("bounded.bin"):
                    part = int(name.split("]", 1)[0][5:])
                    started.append(part)
                    if part == 2:
                        first_batch.set()
                    if part == full_parts + 1:
                        last_attachment.set()
                        await allow_last.wait()
                    else:
                        await allow_data.wait()
                return await super().send_file(channel_id, data, name, caption)

        dc = DiscordStore(
            [Bot(1, channel)],
            channel_id=98_770,
            key=config.stores["dc-spare"].key,
            max_part_bytes=part_size,
            max_concurrent_uploads=11,
        )

        async def stores(cfg):
            return tg if cfg.backend == "telegram" else dc

        client = await Client.create(config.filesystems["media"], config, stores)
        tg.fail = set(PRIMARY_DOWN)

        class NoAuth(asyncssh.SSHServer):
            def begin_auth(self, username: str) -> bool:
                return False

        def factory(chan):
            server = TGFSSFTPServer(chan, {"media": client}, config)
            server._user = AdminUser("admin")
            return server

        async with AsyncExitStack() as stack:
            listener = await stack.enter_async_context(
                asyncssh.listen(
                    "127.0.0.1",
                    0,
                    server_factory=NoAuth,
                    server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
                    sftp_factory=factory,
                )
            )
            conn = await stack.enter_async_context(
                asyncssh.connect(
                    "127.0.0.1", listener.get_port(), username="test", known_hosts=None
                )
            )
            sftp = await stack.enter_async_context(conn.start_sftp_client())
            handle = await sftp.open("/media/bounded.bin", "wb")

            async def write():
                block = 32 * 1024
                for offset in range(0, len(data), block):
                    await handle.write(data[offset : offset + block], offset)

            write_task = asyncio.create_task(write())
            close_task = None
            try:
                await asyncio.wait_for(first_batch.wait(), 3)
                done, _ = await asyncio.wait({write_task}, timeout=0.2)
                # A large tee queue used to ACK the entire upload while
                # this bot was still sending its first batch.
                assert not done
                assert started == [1, 2]
                allow_data.set()
                await asyncio.wait_for(write_task, 3)
                close_task = asyncio.create_task(handle.close())
                await asyncio.wait_for(last_attachment.wait(), 3)
                assert not close_task.done()
                allow_last.set()
                await asyncio.wait_for(close_task, 3)
                async with await sftp.open("/media/bounded.bin", "rb") as downloaded:
                    assert await downloaded.read() == data
            finally:
                allow_data.set()
                allow_last.set()
                tasks = [write_task] if close_task is None else [write_task, close_task]
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        fr = tree(client).find_file("bounded.bin")
        version = (await client.file_api.desc(fr)).get_latest_version()
        assert version.store == dc.key

    async def test_an_aborted_hanging_primary_marks_the_breaker(self):
        """The form a real outage takes: not an error, but silence.

        The primary's upload never raises -- a network that only drops
        packets leaves every call hanging -- so the write has to be
        aborted from the outside (FileZilla gives up, the SFTP session
        ends). The cancelled save must still trip the breaker, or the
        client's next attempt waits out the whole outage again even
        though a mirror store is standing by.
        """
        client, tg, dc, _ = await make_client()
        root = tree(client)
        dc_docs = documents(dc)

        tg.stuck = {"upload"}
        # The bootstrap metadata write of Client.create also shows up as an
        # "upload"; the flight in view is only marked by a second one.
        task = asyncio.create_task(
            client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"data", name="a.txt")
            )
        )
        for _ in range(1000):
            if tg.calls.count("upload") >= 2:
                break
            await asyncio.sleep(0)
        assert tg.calls.count("upload") >= 2
        assert not task.done()

        # Let the store actually be inside its hanging call.
        await asyncio.sleep(0.01)
        assert not task.done()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.mirror_group is not None
        assert client.mirror_group.primary_dead()

        # The next write skips the silent primary altogether: it lands in
        # the acting store, which owns the version and its descriptor, and
        # leaves nothing behind in the stores until replication.
        assert "upload" in tg.calls
        tg.fail = {"upload"}
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="a.txt")
        )
        fr = root.find_file("a.txt")
        assert fr is not None
        assert client.fd_repo is not None
        version = (await client.fd_repo.get(fr)).get_latest_version()
        assert version.store == dc.key
        assert fr.store == dc.key
        assert documents(dc) != dc_docs

    async def test_a_failing_primary_fails_the_write(self):
        client, tg, dc, _ = await make_client()
        root = tree(client)
        dc_docs = documents(dc)

        tg.fail = set(PRIMARY_DOWN)
        with pytest.raises(TechnicalError):
            await client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
            )

        # Nothing of the failed write is lying around anywhere.
        assert documents(dc) == dc_docs
        with pytest.raises(FileOrDirectoryDoesNotExist):
            root.find_file("a.txt")

    async def test_the_retry_lands_in_the_acting_store_and_is_readable(self):
        client, tg, dc, _ = await make_client()
        root = tree(client)
        dc_docs = documents(dc)
        tg_docs = documents(tg)
        tg.fail = set(PRIMARY_DOWN)
        with pytest.raises(TechnicalError):
            await client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
            )

        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="a.txt")
        )

        fr = root.find_file("a.txt")
        assert fr is not None
        assert fr.store == dc.key
        fd = await client.file_api.desc(fr)
        version = fd.get_latest_version()
        # The acting store owns the version: its part and its descriptor
        # sit there, the descriptor not twice.
        part = dc.messages[version.message_ids[0]]
        assert part.document == b"data"
        assert part.name == "[part1]a.txt"
        assert documents(dc) == dc_docs + [part.message_id]
        # One fresh descriptor text message in the acting store -- the
        # store that holds the descriptor gets no mirror copy of it too.
        assert texts_containing(dc, '"type": "F"') == 1
        assert documents(tg) == tg_docs
        assert fr.message_id in dc.messages
        assert '"type": "F"' in dc.messages[fr.message_id].text

        stream = await client.file_api.retrieve(fr, 0, -1, "a.txt")
        assert b"".join([chunk async for chunk in stream]) == b"data"

    async def test_an_existing_file_fails_over_too(self):
        client, tg, dc, _ = await make_client()
        root = tree(client)
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
        )
        fr = root.find_file("a.txt")
        assert fr is not None
        assert fr.store == tg.key
        tg.fail = set(PRIMARY_DOWN)

        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"two", name="a.txt")
        )

        fd = await client.file_api.desc(fr)
        version = fd.get_latest_version()
        assert version.store == dc.key
        # The retry acts as a fresh version of the file that now lives in
        # the acting store, and reads still work.
        assert len(fd.get_versions()) == 2
        stream = await client.file_api.retrieve(fr, 0, -1, "a.txt")
        assert b"".join([chunk async for chunk in stream]) == b"two"

    async def test_a_copy_of_a_failed_over_file_is_taken_in_the_owning_store(self):
        client, tg, dc, _ = await make_client()
        root = tree(client)
        tg.fail = set(PRIMARY_DOWN)
        with pytest.raises(TechnicalError):
            await client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
            )
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="a.txt")
        )
        fr = root.find_file("a.txt")
        assert fr is not None

        await client.file_api.copy(root, fr, "copy.txt")

        copied_fr = root.find_file("copy.txt")
        assert copied_fr is not None
        assert copied_fr.store == dc.key
        stream = await client.file_api.retrieve(copied_fr, 0, -1, "copy.txt")
        assert b"".join([chunk async for chunk in stream]) == b"data"

    async def test_the_queued_gap_exists_only_for_the_failed_over_file(self, tmp_path):
        client, tg, dc, queue = await make_client(tmp_path)
        root = tree(client)
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"healthy", name="ok.txt")
        )
        tg.fail = set(PRIMARY_DOWN)

        with pytest.raises(TechnicalError):
            await client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
            )
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="b.txt")
        )

        assert queue is not None
        # A complete inline write (ok.txt) is never queued; the failed-over
        # one leaves a gap in every store it did not land in.
        assert [i.path for i in queue.pending("media")] == ["/b.txt"]

    async def test_the_recovered_primary_is_filled_back_in(self, tmp_path):
        client, tg, dc, queue = await make_client(tmp_path)
        root = tree(client)
        tg.fail = set(PRIMARY_DOWN)
        with pytest.raises(TechnicalError):
            await client.file_api.upload(
                root, FileMessageFromBuffer.new(buffer=b"one", name="a.txt")
            )
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="b.txt")
        )
        fr = root.find_file("b.txt")
        assert fr is not None
        assert client.fd_repo is not None
        failed_over = (await client.fd_repo.get(fr)).get_latest_version()
        assert failed_over.store == dc.key

        tg.fail = set()
        assert queue is not None
        assert await ReplicationWorker(client, queue).run_once() == 1
        assert queue.pending("media") == []

        fd = await client.fd_repo.get(fr)
        assert fd is not None
        version = fd.get_latest_version()
        assert version.id == failed_over.id
        # Telegram owns the version again; the Discord copy it was written
        # to survives as the mirror entry.
        assert version.owned_by(tg.key) and version.has_copy_in(dc.key)
        stream = await client.file_api.retrieve(fr, 0, -1, "b.txt")
        assert b"".join([chunk async for chunk in stream]) == b"data"
        assert fr.store == tg.key

    async def test_every_mirror_copy_is_named_like_a_primary_part(self):
        """Copies are named by the target store's own part numbering with
        the file's name -- never ``replica-...`` or ``part-<id>`` like
        before, so a copy is as indistinguishable from an original upload
        as the message ids in the metadata allow."""
        client, tg, dc, _ = await make_client()
        root = tree(client)

        await client.file_api.upload(
            root,
            FileMessageFromBuffer.new(buffer=bytes(range(256)) * 20, name="a.bin"),
        )

        fr = root.find_file("a.bin")
        assert fr is not None
        assert client.fd_repo is not None
        version = (await client.fd_repo.get(fr)).get_latest_version()
        # The primary numbered its three parts itself.
        assert {tg.messages[mid].name for mid in version.message_ids} == {
            f"[part{i}]a.bin" for i in range(1, 4)
        }
        copied = version.mirrors.get(dc.key, [])
        assert copied
        # Each copied part went out on its own, so the target's uploader
        # numbered every one of them the way it would number a first part
        # -- with the file's own name and none of the copy markers.
        assert {dc.messages[mid].name for mid in copied} == {"[part1]a.bin"}
        for message in dc.messages.values():
            if message.document is not None and message.name != "metadata.json":
                assert message.name.endswith("a.bin")
                assert "replica" not in message.name
                assert not message.name.startswith("part-")
