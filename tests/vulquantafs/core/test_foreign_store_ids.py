"""Descriptors whose ids belong to another store than the one they name.

A file written while another store was the primary records its ids without
a store, which tgfs always read as "the configured primary". Ids are only
meaningful inside the store that issued them, so a 19-digit Discord
snowflake in a Telegram descriptor cannot name a Telegram message at all:
Telegram cannot even encode the request, and the failure took down every
descriptor read that shared the batch -- the symptom being downloads that
deliver an empty file while the listing still shows the real size.
"""

import datetime

import pytest

from tests.fakes.store import FakeMessage, FakeStore
from vulquantafs.core.mirror import MirrorGroup, MirrorStore
from vulquantafs.core.model import TGFSDirectory, TGFSFileDesc, TGFSFileVersion
from vulquantafs.core.repository.impl.fd.store_msg import StoreFDRepository

# A Discord message id, far past what Telegram's signed 32-bit ids allow.
SNOWFLAKE = 1574871427142914142
PART = b"x" * 512


def _document(store: FakeStore, message_id: int, data: bytes) -> None:
    store.messages[message_id] = FakeMessage(message_id, document=data)


def _descriptor_json(name: str, message_ids, part_sizes) -> str:
    fd = TGFSFileDesc(name=name)
    fd.add_version(
        TGFSFileVersion(
            id="v1",
            updated_at=datetime.datetime.now(),
            message_ids=list(message_ids),
            part_sizes=list(part_sizes),
        )
    )
    return fd.to_json()


@pytest.fixture
def stores():
    """A Telegram primary with a Discord mirror, ids kept apart by range."""
    primary = FakeStore(key="tg:111", backend_name="telegram", max_message_id=2**31 - 1)
    mirror = FakeStore(key="dc:9", backend_name="discord")
    group = MirrorGroup(
        primary=primary,
        stores=[MirrorStore(key="dc:9", store=mirror)],
        mode="auto",
        strict=True,
    )
    return StoreFDRepository(primary, mirror_group=group), primary, mirror


class TestDescriptorsWrittenForAnotherStore:
    @pytest.mark.asyncio
    async def test_the_descriptor_is_read_from_the_store_that_issued_its_id(
        self, stores
    ):
        repo, primary, mirror = stores
        # Written when Discord was the primary: no store recorded, and the
        # ids are Discord's, not Telegram's.
        mirror.messages[SNOWFLAKE] = FakeMessage(
            SNOWFLAKE,
            text=_descriptor_json("512KB.txt", [SNOWFLAKE + 1], [len(PART)]),
        )
        _document(mirror, SNOWFLAKE + 1, PART)
        fr = TGFSDirectory.root_dir().create_file_ref("512KB.txt", SNOWFLAKE)

        fd = await repo.get(fr)

        version = fd.get_latest_version()
        assert version.is_valid()
        assert version.part_sizes == [len(PART)]
        # The primary was never asked for an id it cannot hold: that request
        # raised while being encoded and failed the whole batch.
        assert "get_messages" not in primary.calls

    @pytest.mark.asyncio
    async def test_the_version_records_the_store_that_owns_it(self, stores):
        repo, _, mirror = stores
        mirror.messages[SNOWFLAKE] = FakeMessage(
            SNOWFLAKE,
            text=_descriptor_json("512KB.txt", [SNOWFLAKE + 1], [len(PART)]),
        )
        _document(mirror, SNOWFLAKE + 1, PART)
        fr = TGFSDirectory.root_dir().create_file_ref("512KB.txt", SNOWFLAKE)

        fd = await repo.get(fr)

        assert fd.get_latest_version().store == "dc:9"

    @pytest.mark.asyncio
    async def test_telegram_ids_are_still_read_from_the_primary(self, stores):
        repo, primary, mirror = stores
        fd_mid = primary._add(text=_descriptor_json("plain.txt", [77], [len(PART)]))
        _document(primary, 77, PART)
        fr = TGFSDirectory.root_dir().create_file_ref("plain.txt", fd_mid)

        fd = await repo.get(fr)

        assert fd.get_latest_version().part_sizes == [len(PART)]
        assert "get_messages" in primary.calls
        assert "get_messages" not in mirror.calls
