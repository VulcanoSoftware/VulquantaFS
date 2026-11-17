"""End to end: redundant metadata over real (in-memory) stores.

A Telegram primary store and a Discord mirror store, with the metadata
stored as a pinned message in each -- the shape ``metadata_mirrors``
gives a user. The scenarios cover the promise of the feature: a store
that is down never blocks the others, and it is brought back in line
once it returns.
"""

from typing import Dict, cast

import pytest

from tgdcfs.config import Config
from tgdcfs.core.client import Client
from tgdcfs.reqres import FileMessageFromBuffer
from tests.fakes.store import FakeStore


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
    return FakeStore(key=store_cfg.key, backend_name=store_cfg.backend)


def build_clients(config: Config, fail: Dict[str, set] | None = None):
    fail = fail or {}

    async def factory(store_cfg):
        return FakeStore(
            key=store_cfg.key,
            backend_name=store_cfg.backend,
            fail=set(fail.get(store_cfg.name, set())),
        )

    return factory


def discord_store(client: Client) -> FakeStore:
    assert client.mirror_group is not None
    return cast(
        FakeStore,
        next(s.store for s in client.mirror_group.stores if s.key == "dc:9876"),
    )


def api(client: Client):
    assert client.metadata_api is not None
    return client.metadata_api


class TestRedundantMetadata:
    async def test_every_store_holds_a_pinned_copy_of_the_tree(self):
        config = Config.from_dict(config_dict())
        client = await Client.create(
            config.filesystems["media"], config, build_clients(config)
        )

        root = api(client).get_root_directory()
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"hello world", name="a.txt")
        )

        tg_store = cast(FakeStore, client.store)
        assert tg_store.pinned is not None
        assert b"a.txt" in tg_store.document_of(tg_store.pinned)

    async def test_the_discord_copy_is_an_independent_store(self):
        config = Config.from_dict(config_dict())
        client = await Client.create(
            config.filesystems["media"], config, build_clients(config)
        )

        root = api(client).get_root_directory()
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"hello world", name="a.txt")
        )

        # The Discord store is not the client's primary; its pinned copy
        # lives there on its own.
        dc_store = discord_store(client)
        assert dc_store.backend_name == "discord"
        assert dc_store.pinned is not None
        assert b"a.txt" in dc_store.document_of(dc_store.pinned)

    async def test_a_down_store_does_not_block_writes_and_turns_stale(self):
        config = Config.from_dict(config_dict())
        client = await Client.create(
            config.filesystems["media"], config, build_clients(config)
        )
        assert api(client).stale_stores() == []

        # Take the Discord store down.
        dc_store = discord_store(client)
        dc_store.fail = {"upload", "pin_message", "replace_document"}

        root = api(client).get_root_directory()
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"payload", name="b.txt")
        )

        assert "pinned_message@dc-spare" in api(client).stale_stores()
        tg_store = cast(FakeStore, client.store)
        assert tg_store.pinned is not None
        assert b"b.txt" in tg_store.document_of(tg_store.pinned)

    async def test_heal_brings_a_recovered_store_back_in_line(self):
        config = Config.from_dict(config_dict())
        client = await Client.create(
            config.filesystems["media"], config, build_clients(config)
        )
        dc_store = discord_store(client)
        dc_store.fail = {"upload", "pin_message", "replace_document"}
        root = api(client).get_root_directory()
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"payload", name="b.txt")
        )
        assert "pinned_message@dc-spare" in api(client).stale_stores()

        dc_store.fail = set()
        await api(client).heal()

        assert api(client).stale_stores() == []
        assert dc_store.pinned is not None
        assert b"b.txt" in dc_store.document_of(dc_store.pinned)

    async def test_a_down_primary_fails_over_to_the_mirror_on_start(self):
        config = Config.from_dict(config_dict())
        # First run: leave a tree behind in both stores.
        client = await Client.create(
            config.filesystems["media"], config, build_clients(config)
        )
        root = api(client).get_root_directory()
        await client.file_api.upload(
            root, FileMessageFromBuffer.new(buffer=b"data", name="keep.txt")
        )
        dc_store = discord_store(client)

        # Second run: the Telegram store cannot answer.
        tg_store = FakeStore(
            key="tg:-1001", backend_name="telegram", fail={"get_pinned_message"}
        )

        async def factory(store_cfg):
            if store_cfg.backend == "telegram":
                return tg_store
            return dc_store

        client2 = await Client.create(config.filesystems["media"], config, factory)

        root2 = api(client2).get_root_directory()
        assert root2.find_file("keep.txt") is not None
        # The Telegram primary was stale and got the tree written back.
        assert api(client2).stale_stores() == []
        assert tg_store.pinned is not None
        assert b"keep.txt" in tg_store.document_of(tg_store.pinned)
