"""Tee mirroring (``sync: tee``): re-uploading mirrors fed from the
upload stream while the primary uploads it, without a read-back."""

import asyncio
import os
from dataclasses import dataclass, field
from typing import List, cast

import pytest

from tests.fakes.store import FakeStore
from vulquantafs.backends.base import IStore
from vulquantafs.config import CacheConfig
from vulquantafs.core.api import DirectoryApi, FileApi, FileDescApi, MetaDataApi
from vulquantafs.core.local_cache import LocalCache
from vulquantafs.core.mirror import MirrorGroup, MirrorStore
from vulquantafs.core.model import FileStat, TGFSDirectory, TGFSFileVersion
from vulquantafs.core.replication import (
    ReplicationQueue,
    ReplicationWorker,
    file_path,
    queue_descriptor_gaps,
)
from vulquantafs.core.repository.impl import (
    PinnedMessageMetadataRepository,
    StoreFDRepository,
    StoreFileContentRepository,
)
from vulquantafs.core.tee import TeeBranch, TeeFileMessage
from vulquantafs.errors import TechnicalError
from vulquantafs.reqres import FileMessageFromBuffer, FileMessageFromStream, Replica

pytestmark = pytest.mark.asyncio


async def drain(branch: TeeBranch) -> bytes:
    return b"".join([chunk async for chunk in branch.stream()])


class TestTeeBranch:
    async def test_bytes_come_out_in_order(self):
        branch = TeeBranch("dc", capacity=1024)
        await branch.feed(b"abc")
        await branch.feed(b"def")
        await branch.finish()
        assert await drain(branch) == b"abcdef"

    async def test_a_full_branch_holds_the_producer_until_the_consumer_takes(self):
        branch = TeeBranch("dc", capacity=4)
        await branch.feed(b"abcd")
        feeding = asyncio.ensure_future(branch.feed(b"ef"))
        await asyncio.sleep(0)
        assert not feeding.done()
        assert branch.buffered == 4

        stream = branch.stream()
        assert await anext(stream) == b"abcd"
        await feeding
        assert branch.buffered == 2
        await branch.finish()
        assert await anext(stream) == b"ef"
        with pytest.raises(StopAsyncIteration):
            await anext(stream)

    async def test_a_chunk_larger_than_the_capacity_passes_an_empty_branch(self):
        branch = TeeBranch("dc", capacity=2)
        await branch.feed(b"abcdef")  # would deadlock otherwise
        await branch.finish()
        assert await drain(branch) == b"abcdef"

    async def test_detach_releases_a_waiting_producer_and_fails_the_consumer(self):
        branch = TeeBranch("dc", capacity=4)
        await branch.feed(b"abcd")
        feeding = asyncio.ensure_future(branch.feed(b"ef"))
        consuming = asyncio.ensure_future(drain(branch))
        await asyncio.sleep(0)

        await branch.detach()

        await feeding  # not held up any more
        with pytest.raises(TechnicalError, match="detached"):
            await consuming
        assert branch.buffered == 0
        await branch.feed(b"ignored")  # dropped, never blocks
        assert branch.buffered == 0

    async def test_the_consumer_waits_for_more_until_finish(self):
        branch = TeeBranch("dc", capacity=1024)
        consuming = asyncio.ensure_future(drain(branch))
        await asyncio.sleep(0)
        assert not consuming.done()
        await branch.feed(b"x")
        await branch.finish()
        assert await consuming == b"x"


class TestTeeFileMessage:
    async def test_reads_pass_through_and_feed_every_branch(self):
        data = os.urandom(1000)
        inner = FileMessageFromBuffer.new(buffer=data, name="a.bin")
        branches = [TeeBranch("dc", 4096), TeeBranch("other", 4096)]
        msg = TeeFileMessage.wrap(inner, branches)
        drained = [asyncio.ensure_future(drain(b)) for b in branches]

        out = b""
        await msg.open()
        while chunk := await msg.read(300):
            out += chunk
        await msg.close()

        assert out == data
        assert [await d for d in drained] == [data, data]
        assert msg.fed == len(data)

    async def test_close_is_not_the_end_of_the_stream(self):
        """The Telegram uploader closes the message after every part."""
        data = b"0123456789"
        inner = FileMessageFromBuffer.new(buffer=data, name="a.bin")
        branch = TeeBranch("dc", 4096)
        msg = TeeFileMessage.wrap(inner, [branch])
        drained = asyncio.ensure_future(drain(branch))

        # One open/read/close cycle per part, as the Telegram uploader does.
        await msg.open()
        assert await msg.read(5) == b"01234"
        await msg.close()
        await asyncio.sleep(0)
        assert not drained.done()
        msg.next_part(5)
        await msg.open()
        assert await msg.read(5) == b"56789"
        await msg.close()
        assert await drained == data

    async def test_an_unknown_size_ends_at_the_streams_own_eof(self):
        """An SFTP upload has no size: the branches end when the inner
        stream is exhausted, not at a byte count."""
        data = b"0123456789"

        async def stream():
            yield data[:4]
            yield data[4:]

        inner = FileMessageFromStream.new(stream=stream(), size=-1, name="a.bin")
        branch = TeeBranch("dc", 4096)
        msg = TeeFileMessage.wrap(inner, [branch])
        drained = asyncio.ensure_future(drain(branch))

        await msg.open()
        assert await msg.read(4) == b"0123"
        await asyncio.sleep(0)
        assert not drained.done()
        assert await msg.read(100) == b"456789"
        assert await msg.read(100) == b""
        assert await drained == data

    async def test_close_is_still_not_the_end_with_an_unknown_size(self):
        data = b"0123456789"

        async def stream():
            yield data

        inner = FileMessageFromStream.new(stream=stream(), size=-1, name="a.bin")
        branch = TeeBranch("dc", 4096)
        msg = TeeFileMessage.wrap(inner, [branch])
        drained = asyncio.ensure_future(drain(branch))

        await msg.open()
        assert await msg.read(5) == b"01234"
        await msg.close()
        await asyncio.sleep(0)
        assert not drained.done()
        msg.next_part(5)
        await msg.open()
        assert await msg.read(100) == b"56789"
        assert await msg.read(100) == b""
        assert await drained == data


@dataclass
class RecordingStore(FakeStore):
    """A FakeStore that remembers which messages were downloaded and can
    hold every upload until released, to observe the tee mid-flight."""

    downloaded: List[int] = field(default_factory=list)
    hold_uploads: asyncio.Event | None = None

    async def download_file(self, message_id: int, begin: int, end: int):
        self.downloaded.append(message_id)
        return await super().download_file(message_id, begin, end)

    async def upload(self, file_msg):
        if self.hold_uploads is not None:
            await self.hold_uploads.wait()
        return await super().upload(file_msg)


def telegram_like() -> RecordingStore:
    return RecordingStore(key="tg:1", backend_name="telegram", max_part_bytes=2048)


def telegram_spare() -> RecordingStore:
    return RecordingStore(key="tg:2", backend_name="telegram", max_part_bytes=2048)


def discord_like(key: str = "dc:9") -> RecordingStore:
    return RecordingStore(
        key=key, backend_name="discord", max_part_bytes=64, server_copy=False
    )


DATA = bytes(range(256)) * 20  # 5120 bytes: 3 primary parts, 80 replica parts


def make_group(primary: FakeStore, *mirrors: FakeStore, strict=False, mode="auto"):
    return MirrorGroup(
        primary=primary,
        stores=[MirrorStore(key=m.key, store=m) for m in mirrors],
        mode=mode,
        strict=strict,
    )


class TestMirrorGroupTee:
    async def test_targets_are_the_mirrors_that_re_upload(self):
        tg, spare, dc = telegram_like(), telegram_spare(), discord_like()
        group = make_group(tg, spare, dc)
        assert [ch.key for ch in group.tee_targets()] == ["dc:9"]

        assert [
            ch.key for ch in make_group(tg, spare, mode="reupload").tee_targets()
        ] == ["tg:2"]
        assert [ch.key for ch in make_group(dc, tg).tee_targets()] == ["tg:1"]
        assert make_group(tg, spare).tee_targets() == []

    async def test_no_targets_or_no_bytes_means_no_tee(self):
        group = make_group(telegram_like(), telegram_spare())
        assert (
            group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a"), 1 << 20) is None
        )
        group = make_group(telegram_like(), discord_like())
        assert (
            group.tee(FileMessageFromBuffer.new(buffer=b"", name="e"), 1 << 20) is None
        )

    async def test_the_mirror_gets_a_replica_in_its_own_layout(self):
        tg, dc = telegram_like(), discord_like()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 1 << 20)
        assert tee is not None

        sent = await tg.upload(tee.message)
        replicas = group.collect_tee(await tee.collect())

        assert [m.size for m in sent] == [2048, 2048, 1024]
        replica = replicas["dc:9"]
        assert isinstance(replica, Replica)
        assert len(replica.message_ids) == 80 and set(replica.part_sizes) == {64}
        assert b"".join(dc.document_of(m) for m in replica.message_ids) == DATA
        assert tg.downloaded == []  # nothing was read back

    async def test_the_mirror_uploads_while_the_primary_does(self):
        tg, dc = telegram_like(), discord_like()
        tg.hold_uploads = asyncio.Event()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 1 << 20)
        assert tee is not None

        primary = asyncio.ensure_future(tg.upload(tee.message))
        await asyncio.sleep(0)
        # The primary has not read a byte yet, so the mirror has nothing.
        assert dc.messages == {}
        tg.hold_uploads.set()
        await primary
        # By the time the primary is done the mirror has consumed the same
        # stream; collect only waits for its last part.
        replicas = group.collect_tee(await tee.collect())
        assert len(replicas["dc:9"].message_ids) == 80

    async def test_a_failing_mirror_leaves_the_primary_upload_intact(self):
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None

        sent = await tg.upload(tee.message)  # never held up by the dead branch
        outcome = await tee.collect()

        assert [m.size for m in sent] == [2048, 2048, 1024]
        assert isinstance(outcome["dc:9"], TechnicalError)
        assert group.collect_tee(outcome) == {}

    async def test_a_failing_mirror_raises_with_strict(self):
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        group = make_group(tg, dc, strict=True)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None
        await tg.upload(tee.message)
        with pytest.raises(TechnicalError):
            group.collect_tee(await tee.collect())

    async def test_abort_stops_the_mirror_uploads(self):
        tg, dc = telegram_like(), discord_like()
        group = make_group(tg, dc)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16)
        assert tee is not None
        await tee.message.open()
        await tee.message.read(100)  # the mirror has started
        await asyncio.sleep(0)

        await tee.abort()

        outcome = await tee.collect()
        assert all(isinstance(o, Exception) for o in outcome.values())
        assert all(b.detached for b in tee.message.branches)

    async def test_cancelling_collection_stops_all_mirror_uploads(self):
        tg, a, b = telegram_like(), discord_like("dc:1"), discord_like("dc:2")
        a.hold_uploads = b.hold_uploads = asyncio.Event()
        tee = make_group(tg, a, b).tee(
            FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 16
        )
        assert tee is not None

        collecting = asyncio.create_task(tee.collect())
        await asyncio.sleep(0)
        collecting.cancel()
        try:
            done, _ = await asyncio.wait({collecting}, timeout=2)
            assert collecting in done
            with pytest.raises(asyncio.CancelledError):
                await collecting
            assert all(branch.detached for branch in tee.message.branches)
        finally:
            a.hold_uploads.set()
            await asyncio.gather(collecting, return_exceptions=True)

    async def test_an_unknown_size_is_still_teed(self):
        tg, dc = telegram_like(), discord_like()
        group = make_group(tg, dc)

        async def stream():
            yield DATA[:2000]
            yield DATA[2000:]

        tee = group.tee(
            FileMessageFromStream.new(stream=stream(), size=-1, name="a.bin"),
            1 << 20,
        )
        assert tee is not None

        sent = await tg.upload(tee.message)
        replicas = group.collect_tee(await tee.collect())

        assert sum(m.size for m in sent) == len(DATA)
        replica = replicas["dc:9"]
        assert b"".join(dc.document_of(m) for m in replica.message_ids) == DATA
        assert tg.downloaded == []  # nothing was read back

    async def test_two_mirrors_share_one_stream(self):
        tg, a, b = telegram_like(), discord_like("dc:1"), discord_like("dc:2")
        group = make_group(tg, a, b)
        tee = group.tee(FileMessageFromBuffer.new(buffer=DATA, name="a.bin"), 200)
        assert tee is not None
        await tg.upload(tee.message)
        replicas = group.collect_tee(await tee.collect())
        for store, key in ((a, "dc:1"), (b, "dc:2")):
            assert b"".join(
                store.document_of(m) for m in replicas[key].message_ids
            ) == (DATA)


class Stack:
    """The write path from the file API down, with a tee'd repository."""

    def __init__(
        self,
        primary: FakeStore,
        *mirrors: FakeStore,
        queue: ReplicationQueue | None = None,
        inline: bool = False,
        cache: LocalCache | None = None,
    ):
        self.name = "fs"
        self.primary = primary
        self.mirror_group = make_group(primary, *mirrors)
        self.fc_repo = StoreFileContentRepository(
            primary,
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            tee_mirroring=True,
            tee_buffer=256,
            cache=cache,
            cache_scope="fs",
        )
        self.fd_repo = StoreFDRepository(primary, mirror_group=self.mirror_group)
        self.metadata_api = MetaDataApi(
            PinnedMessageMetadataRepository(
                primary, self.fc_repo, mirror_group=self.mirror_group
            )
        )
        self.queue = queue

        async def enqueue(fr):
            assert queue is not None
            queue.enqueue(self.name, file_path(fr))

        self.file_api = FileApi(
            self.metadata_api,
            FileDescApi(self.fd_repo, self.fc_repo),
            cast(IStore, primary),
            mirror_group=self.mirror_group,
            inline_mirroring=inline,
            on_written=enqueue if queue is not None else None,
            written_gaps_only=True,
        )
        self.dir_api = DirectoryApi(self.metadata_api, self.file_api, primary)

    async def init(self) -> "Stack":
        await self.metadata_api.init()
        return self

    @property
    def root(self) -> TGFSDirectory:
        return self.metadata_api.get_root_directory()

    async def put(self, name: str, data: bytes):
        return await self.file_api.upload(
            self.root, FileMessageFromBuffer.new(buffer=data, name=name)
        )

    async def read(self, name: str) -> bytes:
        fr = self.root.find_file(name)
        stream = await self.file_api.retrieve(fr, 0, -1, name)
        return b"".join([chunk async for chunk in stream])

    async def version(self, name: str) -> TGFSFileVersion:
        fr = self.root.find_file(name)
        return (await self.fd_repo.get(fr)).get_latest_version()


class TestWritePath:
    async def test_a_known_down_primary_is_skipped_for_the_entire_stream(self):
        tg, dc = telegram_like(), discord_like()
        stack = Stack(tg, dc)
        stack.mirror_group.mark_primary_dead()

        sent = await stack.fc_repo.save(FileMessageFromBuffer.new(DATA, "a.bin"))

        assert "upload" not in tg.calls
        assert all(part.store == dc.key for part in sent)
        assert b"".join(dc.document_of(part.message_id) for part in sent) == DATA

    async def test_disconnect_cancels_the_producer_and_all_store_uploads(self, mocker):
        tg, dc = telegram_like(), discord_like()
        stack = Stack(tg, dc)
        reading, uploading_primary, uploading_mirror = (
            asyncio.Event() for _ in range(3)
        )
        stopped: set[asyncio.Event] = set()

        async def stall(event):
            event.set()
            try:
                await asyncio.Future()
            finally:
                stopped.add(event)

        async def stream():
            yield b"prefix"
            await stall(reading)

        async def upload_primary(msg):
            await stall(uploading_primary)

        async def upload_mirror(msg):
            await stall(uploading_mirror)

        mocker.patch.object(tg, "upload", side_effect=upload_primary)
        mocker.patch.object(dc, "upload", side_effect=upload_mirror)
        task = asyncio.create_task(
            stack.fc_repo.save(FileMessageFromStream.new(stream()))
        )
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    reading.wait(), uploading_primary.wait(), uploading_mirror.wait()
                ),
                3,
            )
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
        assert stopped == {reading, uploading_primary, uploading_mirror}

    async def test_an_upload_lands_in_both_stores_without_a_read_back(self):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert version.has_copy_in("dc:9")
        assert len(version.replicas["dc:9"].message_ids) == 80
        # The primary was never asked for the content parts.
        assert not any(mid in version.message_ids for mid in tg.downloaded)
        assert await stack.read("a.bin") == DATA

    async def test_a_tee_replica_is_named_like_a_primary_upload(self):
        """The copy carries the file's own name, numbered by the target's
        own uploader -- no ``replica-`` marker, no message id."""
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert {tg.messages[mid].name for mid in version.message_ids} == {
            f"[part{i}]a.bin" for i in range(1, 4)
        }
        replica_ids = version.replicas["dc:9"].message_ids
        assert {dc.messages[mid].name for mid in replica_ids} == {
            f"[part{i}]a.bin" for i in range(1, 81)
        }

    async def test_nothing_is_staged_in_the_cache(self, tmp_path):
        """The mirrors take the bytes from the stream, so an upload goes to
        the stores only, even with ``stage_uploads`` on."""
        cache = LocalCache(
            CacheConfig.from_dict(
                {
                    "enabled": True,
                    "max_size_mb": 0,
                    "max_files": 0,
                    "max_file_size_mb": 0,
                    "block_kb": 64,
                    "stage_uploads": True,
                    "keep_for_reads": True,
                }
            ),
            directory=str(tmp_path / "cache"),
        )
        cache.load()
        staged: List[str] = []
        open_staging = cache.open_staging

        def recording_open_staging(fs, version_id, size):
            staged.append(version_id)
            return open_staging(fs, version_id, size)

        cache.open_staging = recording_open_staging  # type: ignore[method-assign]
        stack = await Stack(telegram_like(), discord_like(), cache=cache).init()
        await stack.put("a.bin", DATA)

        assert staged == []
        assert cache.stats()["entries"] == 0
        assert (await stack.version("a.bin")).has_copy_in("dc:9")

    async def test_a_forwarding_mirror_is_still_copied_inline(self):
        tg, spare, dc = telegram_like(), telegram_spare(), discord_like()
        stack = await Stack(tg, spare, dc).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert version.has_copy_in("tg:2") and version.has_copy_in("dc:9")
        assert "tg:2" in version.mirrors and "dc:9" in version.replicas

    async def test_a_complete_write_is_not_queued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("a.bin", DATA)
        assert queue.pending("fs") == []

    async def test_a_mirror_that_failed_mid_stream_is_queued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        version = await stack.version("a.bin")

        assert not version.has_copy_in("dc:9")
        assert [item.path for item in queue.pending("fs")] == ["/a.bin"]
        assert await stack.read("a.bin") == DATA

    async def test_the_queued_gap_is_replicated_by_the_worker(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("upload")
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        assert [i.path for i in queue.pending("fs")] == ["/a.bin"]
        dc.fail.clear()
        client = type(
            "C",
            (),
            {
                "name": "fs",
                "mirror_group": stack.mirror_group,
                "fd_repo": stack.fd_repo,
                "metadata_api": stack.metadata_api,
                "dir_api": stack.dir_api,
            },
        )()

        assert await ReplicationWorker(client, queue).run_once() == 1  # type: ignore[arg-type]

        assert queue.pending("fs") == []
        version = await stack.version("a.bin")
        assert version.has_copy_in("dc:9")
        assert await stack.read("a.bin") == DATA

    async def test_a_primary_that_fails_before_reading_uses_the_mirror(self):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        tg.fail.add("upload")

        await stack.put("a.bin", DATA)

        assert (await stack.version("a.bin")).store == dc.key
        assert await stack.read("a.bin") == DATA

    async def test_all_failed_stores_stop_without_waiting_for_the_client(self):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        tg.fail.add("upload")
        dc.fail.add("upload")

        async def source():
            await asyncio.Event().wait()
            yield b"never"

        with pytest.raises(TechnicalError):
            await asyncio.wait_for(
                stack.fc_repo.save(FileMessageFromStream.new(source(), name="a")),
                2,
            )

    async def test_early_sink_success_cannot_commit_an_incomplete_stream(self, mocker):
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc).init()
        tg.fail.add("upload")
        mocker.patch.object(dc, "upload", return_value=[])

        with pytest.raises(TechnicalError):
            await stack.put("a.bin", DATA)

    async def test_a_copy_has_no_stream_and_goes_to_the_queue(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("a.bin", DATA)
        fr = stack.root.find_file("a.bin")

        await stack.file_api.copy(stack.root, fr, "b.bin")

        assert [item.path for item in queue.pending("fs")] == ["/b.bin"]


class TestDescriptorMirrorHealing:
    """A file whose descriptor has no copy in a mirror store is
    unreadable while the store that holds the descriptor is down, even
    when all content copies are safe. Its gap must reach the queue like
    any other, so the worker heals it while the stores are up."""

    def client_of(self, stack: Stack) -> object:
        return type(
            "C",
            (),
            {
                "name": "fs",
                "mirror_group": stack.mirror_group,
                "fd_repo": stack.fd_repo,
                "metadata_api": stack.metadata_api,
                "dir_api": stack.dir_api,
            },
        )()

    async def test_a_descriptor_mirror_that_failed_is_queued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("send_text")
        stack = await Stack(tg, dc, queue=queue).init()

        await stack.put("a.bin", DATA)

        version = await stack.version("a.bin")
        fr = stack.root.find_file("a.bin")
        assert version.has_copy_in("dc:9")
        assert "dc:9" not in fr.mirrors
        assert [item.path for item in queue.pending("fs")] == ["/a.bin"]

    async def test_the_descriptor_mirror_gap_is_healed_by_the_worker(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        dc.fail.add("send_text")
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        dc.fail.clear()

        assert await ReplicationWorker(self.client_of(stack), queue).run_once() == 1  # type: ignore[arg-type]

        assert queue.pending("fs") == []
        fr = stack.root.find_file("a.bin")
        assert fr.mirrors.get("dc:9") in dc.messages

    async def test_a_legacy_file_is_queued_at_startup_and_healed(self, tmp_path):
        """Files written before their descriptor was mirrored are caught
        up by the startup sweep instead of staying single-store forever."""
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        fr = stack.root.find_file("a.bin")
        legacy = fr.mirrors["dc:9"]
        fr.mirrors = {}
        del dc.messages[legacy]

        queued = queue_descriptor_gaps(self.client_of(stack), queue)  # type: ignore[arg-type]

        assert queued == 1
        assert [item.path for item in queue.pending("fs")] == ["/a.bin"]
        assert await ReplicationWorker(self.client_of(stack), queue).run_once() == 1  # type: ignore[arg-type]
        assert fr.mirrors.get("dc:9") in dc.messages

    async def test_a_complete_descriptor_is_not_requeued(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("a.bin", DATA)

        assert queue_descriptor_gaps(self.client_of(stack), queue) == 0  # type: ignore[arg-type]
        assert queue.pending("fs") == []

    async def test_a_file_system_without_mirrors_is_not_swept(self, tmp_path):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), queue=queue).init()
        await stack.put("a.bin", DATA)

        assert queue_descriptor_gaps(self.client_of(stack), queue) == 0  # type: ignore[arg-type]
        assert queue.pending("fs") == []

    async def test_a_client_without_a_mirror_group_attribute_is_not_swept(
        self, tmp_path
    ):
        """Duck-typed clients in tests and partial stubs survive the sweep."""
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        client = type("C", (), {"name": "fs"})()

        assert queue_descriptor_gaps(client, queue) == 0  # type: ignore[arg-type]
        assert queue.pending("fs") == []

    async def test_the_sweep_queues_newest_files_first(self, tmp_path):
        """The files being worked with are protected first: a backlog that
        takes hours must not start at the oldest file."""
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        await stack.put("old.bin", DATA)
        await stack.put("new.bin", DATA)
        for name, age in (("old.bin", 1_000), ("new.bin", 2_000)):
            fr = stack.root.find_file(name)
            fr.mirrors = {}
            fr.stat = FileStat(size=len(DATA), created_at=age, modified_at=age)

        queue_descriptor_gaps(self.client_of(stack), queue)  # type: ignore[arg-type]

        assert [i.path for i in queue.pending("fs")] == ["/new.bin", "/old.bin"]


class TestHealThroughput:
    """A backlog of legacy files only drains when the heal skips work it
    does not need: no part lookups, no content copies, no metadata push
    per file, and no queue rewrite per item."""

    def client_of(self, stack: Stack) -> object:
        return type(
            "C",
            (),
            {
                "name": "fs",
                "mirror_group": stack.mirror_group,
                "fd_repo": stack.fd_repo,
                "metadata_api": stack.metadata_api,
                "dir_api": stack.dir_api,
            },
        )()

    async def legacy(self, stack: Stack, name: str) -> None:
        fr = stack.root.find_file(name)
        dc = stack.mirror_group.store_for("dc:9")
        assert dc is not None and fr.mirrors.get("dc:9") in dc.messages
        del dc.messages[fr.mirrors["dc:9"]]
        fr.mirrors = {}

    async def test_the_heal_does_not_touch_content_that_is_already_mirrored(
        self, tmp_path
    ):
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        tg, dc = telegram_like(), discord_like()
        stack = await Stack(tg, dc, queue=queue).init()
        await stack.put("a.bin", DATA)
        await self.legacy(stack, "a.bin")
        queue.enqueue("fs", "/a.bin")
        dc.calls.clear()
        tg.calls.clear()

        assert await ReplicationWorker(self.client_of(stack), queue).run_once() == 1  # type: ignore[arg-type]

        assert "upload" not in dc.calls and "upload" not in tg.calls
        fr = stack.root.find_file("a.bin")
        assert fr.mirrors.get("dc:9") in dc.messages

    async def test_healed_files_are_pushed_in_batches(self, tmp_path, monkeypatch):
        monkeypatch.setattr("vulquantafs.core.replication.PUSH_BATCH", 2)
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        for name, path in (
            ("a.bin", "/a.bin"),
            ("b.bin", "/b.bin"),
            ("c.bin", "/c.bin"),
        ):
            await stack.put(name, DATA)
            await self.legacy(stack, name)
            queue.enqueue("fs", path)
        pushes = 0
        original = stack.metadata_api.request_push

        def counting_request_push(*args, **kwargs):
            nonlocal pushes
            pushes += 1
            original(*args, **kwargs)

        stack.metadata_api.request_push = counting_request_push  # type: ignore[method-assign]

        assert await ReplicationWorker(self.client_of(stack), queue).run_once() == 3  # type: ignore[arg-type]

        assert pushes == 2  # one after the batch of two, one final flush

    async def test_ready_files_are_healed_concurrently(self, tmp_path, monkeypatch):
        monkeypatch.setattr("vulquantafs.core.replication.HEAL_CONCURRENCY", 8)
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        stack = await Stack(telegram_like(), discord_like(), queue=queue).init()
        for name in ("a.bin", "b.bin"):
            await stack.put(name, DATA)
        queue.enqueue("fs", "/a.bin")
        queue.enqueue("fs", "/b.bin")
        worker = ReplicationWorker(self.client_of(stack), queue)  # type: ignore[arg-type]
        entered = []
        second_in = asyncio.Event()

        async def overlapping(path):
            entered.append(path)
            if len(entered) == 1:
                await asyncio.wait_for(second_in.wait(), 2)
            else:
                second_in.set()
            return True

        worker.process_one = overlapping  # type: ignore[method-assign]

        assert await worker.run_once() == 2
        assert len(entered) == 2

    async def test_serial_processing_would_time_out(self, tmp_path, monkeypatch):
        """The guard for the test above: serial processing cannot pass it."""
        monkeypatch.setattr("vulquantafs.core.replication.HEAL_CONCURRENCY", 1)
        queue = ReplicationQueue(path=str(tmp_path / "queue.json"))
        queue.enqueue("fs", "/a.bin")
        queue.enqueue("fs", "/b.bin")
        worker = ReplicationWorker(self.client_of_stack_only(), queue)  # type: ignore[arg-type]
        entered = []
        second_in = asyncio.Event()

        async def overlapping(path):
            entered.append(path)
            if len(entered) == 1:
                await asyncio.wait_for(second_in.wait(), 0.2)
            else:
                second_in.set()
            return True

        worker.process_one = overlapping  # type: ignore[method-assign]

        with pytest.raises(asyncio.TimeoutError):
            await worker.run_once()

    def client_of_stack_only(self) -> object:
        return type("C", (), {"name": "fs", "metadata_api": None})()
