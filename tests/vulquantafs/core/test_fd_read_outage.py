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

import datetime
import logging
import struct

import pytest

from tests.fakes.store import FakeMessage, FakeStore
from vulquantafs.core.mirror import MirrorGroup, MirrorStore
from vulquantafs.core.model import TGFSDirectory, TGFSFileDesc, TGFSFileVersion
from vulquantafs.core.repository.impl.fd.store_msg import StoreFDRepository
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
