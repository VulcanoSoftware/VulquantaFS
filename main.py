import asyncio
import logging
import os
from typing import List, Optional

from vulquantafs.utils.logger import setup_logging
from vulquantafs.utils.watchdog import EventLoopWatchdog

setup_logging()

from uvicorn.config import Config as UvicornConfig
from uvicorn.server import Server

from vulquantafs.app import create_app
from vulquantafs.app.sftp import start_sftp_server
from vulquantafs.backends.base import IStore
from vulquantafs.backends.discord import factory as discord
from vulquantafs.backends.telegram import factory as telegram
from vulquantafs.config import Config, StoreConfig, get_config
from vulquantafs.core import Client, Clients
from vulquantafs.core.client import StoreFactory
from vulquantafs.core.local_cache import CacheSweeper, LocalCache
from vulquantafs.core.metadata_healing import MetadataHealWorker
from vulquantafs.core.replication import (
    ReplicationQueue,
    ReplicationWorker,
    queue_backup_pusher,
    queue_descriptor_gaps,
    restore_replication_backups,
)
from vulquantafs.utils.message_cache import set_message_cache_size


async def create_store_factory(config: Config) -> StoreFactory:
    """Log in to every backend the config uses and return the factory
    that turns a store config into a live store."""

    async def login_telegram():
        return await telegram.login(config) if config.uses_backend["telegram"] else None

    async def login_discord():
        return await discord.login(config) if config.uses_backend["discord"] else None

    # The providers are independent: Discord doesn't need to wait for
    # Telegram's handshakes (or its data-center redirects).
    async with asyncio.TaskGroup() as logins:
        tg = logins.create_task(login_telegram())
        dc = logins.create_task(login_discord())
    tdlib_api, discord_bots = tg.result(), dc.result()
    logging.getLogger(__name__).info("Storage backend logins complete")

    async def create_store(store_cfg: StoreConfig) -> IStore:
        if store_cfg.backend == "telegram" and tdlib_api is not None:
            return await telegram.create_store(store_cfg, config, tdlib_api)
        if store_cfg.backend == "discord" and discord_bots is not None:
            return await discord.create_store(store_cfg, config, discord_bots)
        raise ValueError(f"Unsupported backend: {store_cfg.backend}")

    return create_store


def create_cache(config: Config) -> Optional[LocalCache]:
    """The local cache when the config turns it on; loaded from disk."""
    if not config.vulquantafs.cache.enabled:
        return None
    cache = LocalCache(config.vulquantafs.cache)
    cache.load()
    return cache


async def create_clients(
    config: Config,
    replication: Optional[ReplicationQueue] = None,
    cache: Optional[LocalCache] = None,
) -> Clients:
    store_factory = await create_store_factory(config)

    clients: Clients = {}
    for filesystem in config.filesystems.values():
        logging.getLogger(__name__).info("Initializing filesystem %s", filesystem.name)
        clients[filesystem.name] = await Client.create(
            filesystem,
            config,
            store_factory,
            encryption_cfg=config.vulquantafs.encryption,
            replication=replication,
            cache=cache,
        )
    return clients


def start_cache_sweeper(
    cache: Optional[LocalCache], replication: ReplicationQueue
) -> Optional[CacheSweeper]:
    """The periodic cache housekeeping; a pin counts as stale only when
    nothing is queued for its file system."""
    if cache is None:
        return None
    sweeper = CacheSweeper(cache, lambda fs: not replication.pending(fs))
    sweeper.start()
    return sweeper


async def start_replication_workers(
    clients: Clients, config: Config, replication: ReplicationQueue
) -> List[ReplicationWorker]:
    """One background worker per replicated file system.

    For ``sync: background`` it fills every mirror; for ``inline`` and
    ``tee`` writes it closes the gaps a failed mirror write left behind,
    so a store that was temporarily down is caught up without manual
    action. It also drains the bytes a ``write_ack: cache`` write left
    on the local disk. On start every file whose descriptor lacks a
    mirror copy is queued, so the gap is healed while the stores are up
    instead of surfacing as unreadable files during an outage. A file
    system with a metadata store also gets its queue snapshotted there,
    so losing the data directory does not lose pending content gaps.
    """
    await restore_replication_backups(clients, replication)
    workers: List[ReplicationWorker] = []
    for filesystem in config.filesystems.values():
        client = clients.get(filesystem.name)
        if client is None:
            continue
        if not filesystem.mirrors and filesystem.write_ack != "cache":
            continue
        queue_descriptor_gaps(client, replication)
        if getattr(client, "metadata_api", None) is not None:
            replication.register_backup(filesystem.name, queue_backup_pusher(client))
        worker = ReplicationWorker(client, replication)
        worker.start()
        workers.append(worker)
    return workers


def start_metadata_heal_worker(clients: Clients) -> MetadataHealWorker:
    """The periodic re-sync of the metadata stores that missed a write.

    Cheap for file systems whose metadata is not replicated (the heal is
    a no-op there), so one worker serves them all."""
    worker = MetadataHealWorker(clients)
    worker.start()
    return worker


async def run_server(app, host: str, port: int, name: str):
    """Run a server with proper configuration"""
    logger = logging.getLogger(__name__)
    logger.info(f"Starting {name} server on {host}:{port}")

    server_config = UvicornConfig(
        app,
        host=host,
        port=port,
        loop="none",
        log_level="info",
    )
    server = Server(config=server_config)
    await server.serve()


async def main():
    logger = logging.getLogger(__name__)
    config = get_config()
    # Size and dates read by listings since the last write (see the
    # finally block): the watchdog reads the loop's pace off them while
    # a stall is ongoing, before any handler could log it.
    watchdog = EventLoopWatchdog()
    watchdog.start()
    # Before the first store exists, so every channel starts at this size.
    set_message_cache_size(config.vulquantafs.listing.message_cache_size)

    replication = ReplicationQueue(config.replication_queue_file)
    cache = create_cache(config)
    clients = await create_clients(config, replication, cache)
    workers = await start_replication_workers(clients, config, replication)
    heal_worker = start_metadata_heal_worker(clients)

    app = create_app(clients, config, replication=replication, cache=cache)
    sweeper = start_cache_sweeper(cache, replication)

    try:
        sftp_acceptor = await start_sftp_server(clients, config)
    except Exception as ex:
        # A broken SFTP setup (port taken, unwritable host key) must not keep
        # the HTTP interface from coming up.
        logger.error("Failed to start the SFTP server: %s", ex)
        sftp_acceptor = None

    try:
        await run_server(
            app,
            config.vulquantafs.server.host,
            config.vulquantafs.server.port,
            "VulquantaFS",
        )
    finally:
        for worker in workers:
            await worker.stop()
        await heal_worker.stop()
        # Size and dates read by listings since the last write
        # (``listing.stat_in_metadata``), and the metadata stores' own
        # resources (the GitLab HTTP session).
        for client in clients.values():
            await client.file_api.stats.close()
            if client.metadata_api is not None:
                await client.metadata_api.close()
        if sweeper is not None:
            await sweeper.stop()
        if sftp_acceptor:
            sftp_acceptor.close()
            await sftp_acceptor.wait_closed()
        await watchdog.stop()


def run() -> None:
    """Run the shared server loop without changing the process-wide policy."""
    logger = logging.getLogger(__name__)
    loop_name = os.environ.get("VULQUANTAFS_EVENT_LOOP", "asyncio").strip().lower()
    if loop_name == "uvloop":
        try:
            import uvloop
        except ImportError:
            logger.warning("uvloop is unavailable; falling back to asyncio")
        else:
            logger.info("Using uvloop event loop (explicitly enabled)")
            uvloop.run(main())
            return
    elif loop_name != "asyncio":
        raise SystemExit("VULQUANTAFS_EVENT_LOOP must be 'asyncio' or 'uvloop'")

    logger.info("Using asyncio event loop")
    asyncio.run(main())


if __name__ == "__main__":
    run()
