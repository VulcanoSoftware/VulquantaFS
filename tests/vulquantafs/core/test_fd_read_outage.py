"""A failed descriptor read must not take the primary away from everyone.

One request the client cannot even encode -- a message id outside the
range the protocol allows -- used to be treated as a primary outage. The
read path then marked the primary dead for a minute and, during that
minute, returned "not found" for every descriptor it could not read from a
mirror without asking anything. The clients received empty files for
thousands of healthy files, and the size in the listing stayed correct.

These tests pin the three rules that fix that: only a real outage trips
the breaker, the primary is still asked when it is the only copy, and a
failed read is reported as unreadable instead of missing.
"""

import asyncio
import datetime
import logging
import struct
import time
from typing import Optional

import pytest

from tests.fakes.store import FakeMessage, FakeStore
from vulquantafs.core.mirror import MirrorGroup, MirrorStore
from vulquantafs.core.model import TGFSDirectory, TGFSFileDesc, TGFSFileVersion
from vulquantafs.core.repository.impl.fd import store_msg
from vulquantafs.core.repository.impl.fd.store_msg import StoreFDRepository
from vulquantafs.core.repository.impl.file_content import StoreFileContentRepository
from vulquantafs.core.repository.impl import file_content
from vulquantafs.errors import TechnicalError, is_store_outage


def _descriptor_text() -> str:
    fd = TGFSFileDesc(name="a.txt")
    fd.add_version(
        TGFSFileVersion(
            id="v1",
            updated_at=datetime.datetime.now(),
            message_ids=[77],
            part_sizes=[5],
        )
    )
    return fd.to_json()


TEXT = _descriptor_text()


def _message(message_id: int, text: str) -> FakeMessage:
    return FakeMessage(message_id, text=text)


def _record_ids(store: FakeStore) -> list:
    """Collect every id ``store`` is asked for."""
    requested: list = []
    original = store.get_messages

    async def spy(ids):
        requested.extend(ids)
        return await original(ids)

    store.get_messages = spy  # type: ignore[method-assign]
    return requested


def _with_part(store: FakeStore, descriptor_id: int) -> None:
    """The descriptor and the content message it lists, in ``store``."""
    store.messages[descriptor_id] = _message(descriptor_id, TEXT)
    store.messages[77] = FakeMessage(77, document=b"12345")


class BrokenStore(FakeStore):
    """A store with one request that cannot be built at all."""

    def __init__(self, error: Exception, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error = error

    async def get_messages(self, ids):
        self.calls.append("get_messages")
        raise self.error


def _group(primary, mirror=None) -> MirrorGroup:
    stores = [MirrorStore(key=mirror.key, store=mirror)] if mirror else []
    return MirrorGroup(primary=primary, stores=stores, mode="auto")


class SelectiveStore(FakeStore):
    """A store that cannot encode one particular id."""

    bad_id: int = 0

    async def get_messages(self, ids):
        self.calls.append("get_messages")
        if self.bad_id in ids:
            raise ValueError("cannot encode")
        return [m.to_resp() if (m := self.messages.get(i)) else None for i in ids]


class TestBreakerClassification:
    def test_an_unencodable_identifier_is_not_an_outage(self):
        assert not is_store_outage(
            struct.error("'i' format requires -2147483648 <= number")
        )
        assert not is_store_outage(ValueError("bad id"))

    def test_a_timeout_or_network_failure_is_an_outage(self):
        assert is_store_outage(TimeoutError())
        assert is_store_outage(TechnicalError("connection reset"))
        assert is_store_outage(ConnectionError("refused"))

    @pytest.mark.asyncio
    async def test_a_bad_request_does_not_disable_the_primary(self):
        primary = BrokenStore(ValueError("cannot encode"), key="tg:111")
        group = _group(primary)
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        await repo.get(fr)

        assert not group.primary_dead()

    @pytest.mark.asyncio
    async def test_a_real_outage_does_disable_the_primary(self):
        primary = BrokenStore(TechnicalError("connection reset"), key="tg:111")
        group = _group(primary)
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        await repo.get(fr)

        assert group.primary_dead()

    @pytest.mark.asyncio
    async def test_one_bad_descriptor_does_not_cost_the_next_file_its_read(
        self,
    ):
        """The storm: a dead primary sent every later read back as missing."""
        primary = SelectiveStore(key="tg:111")
        primary.bad_id = 42
        _with_part(primary, 42)
        _with_part(primary, 44)
        group = _group(primary, FakeStore(key="dc:9"))
        repo = StoreFDRepository(primary, mirror_group=group)

        await repo.get(TGFSDirectory.root_dir().create_file_ref("a.txt", 42))
        fd = await repo.get(TGFSDirectory.root_dir().create_file_ref("b.txt", 44))

        assert fd.complete is True
        assert fd.get_latest_version().part_sizes == [5]


class TestDeadPrimaryWithoutMirror:
    @pytest.mark.asyncio
    async def test_the_primary_is_still_asked_when_nothing_else_has_the_file(
        self,
    ):
        primary = FakeStore(key="tg:111")
        mirror = FakeStore(key="dc:9")
        _with_part(primary, 42)
        requested = _record_ids(primary)
        group = _group(primary, mirror)
        group.mark_primary_dead()
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        fd = await repo.get(fr)

        # The descriptor was read from the primary even though the breaker
        # said it was down: a mirror copy would have been the only
        # alternative, and this file has none.
        assert fd.get_latest_version().part_sizes == [5]
        assert 42 in requested

    @pytest.mark.asyncio
    async def test_a_mirror_copy_still_wins_while_the_primary_is_dead(self):
        primary = FakeStore(key="tg:111")
        mirror = FakeStore(key="dc:9")
        _with_part(mirror, 43)
        _with_part(primary, 78)
        requested = _record_ids(primary)
        group = _group(primary, mirror)
        group.mark_primary_dead()
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)
        fr.mirrors = {"dc:9": 43}

        fd = await repo.get(fr)

        assert fd.get_latest_version().part_sizes == [5]
        # The mirror's copy served the descriptor; the primary was never
        # asked for it while the breaker was open.
        assert 42 not in requested


class TestFailedReadIsNotAMissingFile:
    @pytest.mark.asyncio
    async def test_the_descriptor_is_marked_unreadable_and_logged(self, caplog):
        primary = BrokenStore(TechnicalError("connection reset"), key="tg:111")
        group = _group(primary)
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        with caplog.at_level(logging.ERROR):
            fd = await repo.get(fr)

        assert fd.complete is False
        assert "could not be read" in caplog.text
        assert "not found" not in caplog.text

    @pytest.mark.asyncio
    async def test_a_descriptor_that_really_is_gone_is_reported_missing(self, caplog):
        primary = FakeStore(key="tg:111")
        group = _group(primary)
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        with caplog.at_level(logging.ERROR):
            fd = await repo.get(fr)

        assert fd.complete is False
        assert "not found" in caplog.text


def _mirrored_text(mirror_part: int, primary_part: int = 77, own: bool = False) -> str:
    """A descriptor whose one part also lives in the mirror."""
    fd = TGFSFileDesc(name="a.txt")
    mirrors = {"dc:9": [mirror_part]} if mirror_part else {}
    if own:
        mirrors["tg:111"] = [primary_part]
    fd.add_version(
        TGFSFileVersion(
            id="v1",
            updated_at=datetime.datetime.now(),
            message_ids=[primary_part],
            part_sizes=[5],
            mirrors=mirrors,
        )
    )
    return fd.to_json()


class HangingPartsStore(FakeStore):
    """A store that answers a descriptor but never its part lookup.

    A backend whose network went away does not refuse the call: it stops
    answering it. ``get_messages`` for the content messages is the one
    that used to hang in front of a download's first byte.
    """

    part_ids: set = set()

    async def get_messages(self, ids):
        if self.part_ids & set(ids):
            self.calls.append("get_messages(stuck)")
            await asyncio.Event().wait()
        return await super().get_messages(ids)


class TestDownloadsDuringAnOutage:
    """A download must not be held by a store the breaker took away.

    Every download opens with a "are these parts still there" lookup. It
    ran without a bound, so with the primary gone a single file waited out
    the whole reconnect cycle of the Telegram client -- FileZilla drops a
    transfer after 20 s of silence -- while a mirror held the file. These
    tests pin that a mirrored file is served from its mirror, that a store
    which stops answering is bounded, and that nothing turns into a
    zero-byte success.
    """

    @pytest.mark.asyncio
    async def test_the_parts_of_a_mirrored_file_do_not_wait_on_the_primary(self):
        primary = FakeStore(key="tg:111")
        mirror = FakeStore(key="dc:9")
        text = _mirrored_text(mirror_part=44)
        primary.messages[42] = _message(42, text)
        primary.messages[77] = FakeMessage(77, document=b"12345")
        mirror.messages[43] = _message(43, text)
        mirror.messages[44] = FakeMessage(44, document=b"12345")
        requested = _record_ids(primary)
        group = _group(primary, mirror)
        group.mark_primary_dead()
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)
        fr.mirrors = {"dc:9": 43}

        started = time.monotonic()
        fd = await repo.get(fr)
        elapsed = time.monotonic() - started

        assert fd.get_latest_version().part_sizes == [5]
        # Not one question went to the store the breaker had taken away.
        assert requested == []
        assert elapsed < 5

    @pytest.mark.asyncio
    async def test_a_version_that_lists_its_own_store_is_not_a_mirror(self):
        """Every mirror group lists the owning store among the copies."""
        primary = FakeStore(key="tg:111")
        mirror = FakeStore(key="dc:9")
        text = _mirrored_text(mirror_part=0)
        primary.messages[42] = _message(42, text)
        primary.messages[77] = FakeMessage(77, document=b"12345")
        requested = _record_ids(primary)
        group = _group(primary, mirror)
        group.mark_primary_dead()
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)
        fr.mirrors = {"dc:9": 43}
        mirror.messages[43] = _message(43, _mirrored_text(mirror_part=0, own=True))

        fd = await repo.get(fr)

        # The only copy of the part is the primary's own, so it is still
        # asked for it -- the short way round.
        assert 77 in requested
        assert fd.get_latest_version().part_sizes == [5]

    @pytest.mark.asyncio
    async def test_a_part_lookup_that_never_answers_is_bounded_and_served(
        self, monkeypatch
    ):
        monkeypatch.setattr(store_msg, "PART_LOOKUP_TIMEOUT_S", 0.05)
        primary = HangingPartsStore(key="tg:111")
        primary.part_ids = {77}
        mirror = FakeStore(key="dc:9")
        text = _mirrored_text(mirror_part=44)
        primary.messages[42] = _message(42, text)
        primary.messages[77] = FakeMessage(77, document=b"12345")
        mirror.messages[43] = _message(43, text)
        mirror.messages[44] = FakeMessage(44, document=b"12345")
        group = _group(primary, mirror)
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        started = time.monotonic()
        fd = await repo.get(fr)
        elapsed = time.monotonic() - started

        assert fd.get_latest_version().part_sizes == [5]
        assert elapsed < 5
        # A store that does not answer is a store that is down.
        assert group.primary_dead()

    @pytest.mark.asyncio
    async def test_a_part_lookup_that_never_answers_without_a_mirror_raises(
        self, monkeypatch, caplog
    ):
        monkeypatch.setattr(store_msg, "PART_LOOKUP_TIMEOUT_S", 0.05)
        primary = HangingPartsStore(key="tg:111")
        primary.part_ids = {77}
        text = _descriptor_text()
        primary.messages[42] = _message(42, text)
        primary.messages[77] = FakeMessage(77, document=b"12345")
        group = _group(primary, FakeStore(key="dc:9"))
        repo = StoreFDRepository(primary, mirror_group=group)
        fr = TGFSDirectory.root_dir().create_file_ref("a.txt", 42)

        with caplog.at_level(logging.WARNING):
            fd = await repo.get(fr)

        # Nothing was verified, so the read is not complete -- and there is
        # no version left to serve, which FileApi turns into an error, never
        # into five zero bytes.
        assert fd.complete is False
        assert fd.get_versions(exclude_invalid=True) == []
        assert "checking the mirrors" in caplog.text


def _version(mirror_part: Optional[int] = None) -> TGFSFileVersion:
    return TGFSFileVersion(
        id="v1",
        updated_at=datetime.datetime.now(),
        message_ids=[77],
        part_sizes=[5],
        mirrors={"dc:9": [mirror_part]} if mirror_part else {},
    )


class TestContentReadsDuringAnOutage:
    """The bytes themselves must not wait on a store that is gone either.

    A read with nowhere to fail over to used to have no deadline at all:
    with the primary taken away by the breaker and holding the only copy,
    the transfer simply stopped, and the client's own 20 s timeout was the
    only thing that ended it.
    """

    @pytest.mark.asyncio
    async def test_a_dead_store_holding_the_only_copy_answers_before_the_client(
        self, monkeypatch
    ):
        monkeypatch.setattr(file_content, "DEAD_STORE_READ_TIMEOUT_S", 0.05)
        primary = FakeStore(key="tg:111")
        primary.stuck = {"download_file"}
        group = _group(primary, FakeStore(key="dc:9"))
        group.mark_primary_dead()
        repo = StoreFileContentRepository(primary, mirror_group=group)

        stream = await repo.get(_version(), 0, -1, "a.txt")

        started = time.monotonic()
        with pytest.raises(TimeoutError):
            async for _ in stream:
                pass
        assert time.monotonic() - started < 5

    @pytest.mark.asyncio
    async def test_the_bytes_come_from_the_mirror_while_the_primary_is_dead(self):
        primary = FakeStore(key="tg:111")
        primary.stuck = {"download_file"}
        mirror = FakeStore(key="dc:9")
        mirror.messages[44] = FakeMessage(44, document=b"12345")
        group = _group(primary, mirror)
        group.mark_primary_dead()
        repo = StoreFileContentRepository(primary, mirror_group=group)

        started = time.monotonic()
        stream = await repo.get(_version(mirror_part=44), 0, -1, "a.txt")
        data = b"".join([chunk async for chunk in stream])

        assert data == b"12345"
        assert time.monotonic() - started < 5
