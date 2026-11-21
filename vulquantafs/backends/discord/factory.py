"""Logins and store construction for the Discord backend."""

import asyncio
import logging
from typing import List, Optional

from vulquantafs.config import Config, StoreConfig

from .client import DiscordBotAPI, login_as_bot
from .store import DiscordStore

logger = logging.getLogger(__name__)


async def login(config: Config) -> List[DiscordBotAPI]:
    """Log every configured bot in, concurrently.

    The gateways are independent sessions, so their handshakes overlap;
    logged one after another they lined most of the boot up behind
    them (about two seconds per bot, more than half a minute for the
    eleven this file system runs on). A failure closes the bots that
    already made it in, like the sequential login did."""
    if config.discord is None:
        raise ValueError("configuration block 'backends.discord' is missing")
    results = await asyncio.gather(
        *(login_as_bot(token) for token in config.discord.bot_tokens),
        return_exceptions=True,
    )
    bots: List[DiscordBotAPI] = []
    failure: Optional[BaseException] = None
    for result in results:
        if isinstance(result, BaseException):
            failure = failure or result
            continue
        client = result
        bots.append(DiscordBotAPI(client, name=client.user.name if client.user else ""))
    if failure is not None:
        for bot in bots:
            await bot.close()
        raise failure
    return bots


async def create_store(
    store_cfg: StoreConfig, config: Config, bots: List[DiscordBotAPI]
) -> DiscordStore:
    """Bind a store config to a Discord channel through the logged-in bots."""
    if config.discord is None:
        raise ValueError("configuration block 'backends.discord' is missing")
    try:
        channel_id = int(store_cfg.channel)
    except ValueError:
        raise ValueError(
            f"stores.{store_cfg.name}: Discord channel ids are numeric, "
            f"got '{store_cfg.channel}'"
        )
    discord_cfg = config.discord
    return DiscordStore(
        bots,
        channel_id,
        key=store_cfg.key,
        max_part_bytes=discord_cfg.max_file_size_bytes,
        delete_on_remove=discord_cfg.delete_messages_on_remove,
        max_retries=discord_cfg.upload_max_retries,
        retry_interval=discord_cfg.upload_retry_interval,
        max_concurrent_uploads=discord_cfg.max_concurrent_uploads,
        max_concurrent_downloads=discord_cfg.max_concurrent_downloads,
    )
