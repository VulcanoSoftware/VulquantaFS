import asyncio
import logging
from dataclasses import dataclass
from functools import reduce
from typing import Dict, List, Optional, Set, Tuple

from vulquantafs.reqres import GetMessagesReq, GetMessagesResp, MessageResp
from vulquantafs.utils.message_cache import channel_cache

from .interface import ITDLibClient, TDLibApi

# How long a batch collects further requests before it is sent. Counted from
# the *first* pending request, so a steady stream of requests cannot keep
# pushing the flush further out.
DELAY = 0.5

# Largest message id a Telegram channel can hold. MTProto encodes it as a
# signed 32-bit integer, so a bigger id -- a 19-digit Discord snowflake that
# reached a Telegram descriptor, or a corrupt file reference -- makes the
# client fail while serializing the request, before anything is sent.
MAX_MESSAGE_ID = 2**31 - 1

# How long one batch attempt may stay silent before it is abandoned. Every
# caller bounds its own wait far below the wide ``rpc_timeout_s`` leash of
# an RPC, and cancelling that wait only abandons the answer, not the
# request: the RPC keeps running in the single collector task, so one
# sender wedged in a half-open connection -- the state a packet-dropping
# outage leaves behind -- held the collector, and with it every queued
# request of every download, stuck for as long as the silence lasted. An
# attempt that outlives this bound is cancelled, which is exactly what
# lets ``_connected_rpc`` drop the wedged sender, so a later attempt
# starts from a fresh connection and answers as soon as the store is
# actually back.
ATTEMPT_TIMEOUT_S = 8.0


logger = logging.getLogger(__name__)


@dataclass
class Request:
    ids: list[int]
    future: asyncio.Future[GetMessagesResp]


class MessageBroker:
    def __init__(self, tdlib: TDLibApi, private_file_channel: int):
        self.tdlib = tdlib
        self.__requests: List[Request] = []
        self.__lock = asyncio.Lock()
        self.__task: Optional[asyncio.Task] = None
        self.private_file_channel = private_file_channel

    async def get_messages(self, ids: list[int]) -> list[Optional[MessageResp]]:
        if cached_messages := channel_cache(self.private_file_channel).id.gets(ids):
            if all(msg is not None for msg in cached_messages):
                return cached_messages

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async with self.__lock:
            self.__requests.append(Request(ids, future))
            if self.__task is None:
                self.__task = loop.create_task(self.__collect())
        return await future

    @property
    def __bots_count(self) -> int:
        """How many bot clients a failing request may be retried on.

        Taken from the live client list rather than the config: a
        deployment using the legacy single ``bot.token`` key has an empty
        ``bot.tokens`` list, which would leave the retry loop with nothing
        to iterate over.
        """
        return max(1, len(self.tdlib.bots))

    async def __collect(self) -> None:
        """Flush batches until a whole window passes with nothing pending.

        Keeping one collector alive across batches is what bounds the wait:
        requests that arrive while a batch is in flight join the next one
        instead of each restarting the timer.
        """
        in_flight: List[Request] = []
        try:
            while True:
                await asyncio.sleep(DELAY)
                async with self.__lock:
                    in_flight, self.__requests = self.__requests, []
                await self.process_requests(in_flight)
                in_flight = []
                async with self.__lock:
                    # Retire as soon as the queue runs dry rather than
                    # idling through another window: a request arriving
                    # later simply starts a fresh collector.
                    if not self.__requests:
                        self.__task = None
                        return
        except BaseException as ex:
            # Nobody else will ever complete these futures, so a collector
            # that dies -- cancelled at shutdown, or killed by an unexpected
            # error -- has to hand the failure to its waiters instead of
            # leaving them hanging. No await in between, so the swap needs
            # no lock.
            pending, self.__requests = self.__requests, []
            self.__task = None
            for request in (*in_flight, *pending):
                if not request.future.done():
                    request.future.set_exception(ex)
            raise

    async def process_requests(self, requests: List[Request]) -> None:
        if not requests:
            return

        ids: Set[int] = reduce(lambda full, req: full.union(req.ids), requests, set())

        # An id this channel cannot hold is answered as missing without being
        # sent: one such id in a batch used to make the client raise while
        # encoding the request, which failed every request that happened to
        # share the window -- including the reads of unrelated files, whose
        # clients then received empty files.
        foreign = {mid for mid in ids if not self.__addressable(mid)}
        if foreign:
            logger.warning(
                f"Ignoring {len(foreign)} message id(s) that cannot belong to "
                f"this Telegram channel: {sorted(foreign)[:5]}"
            )

        messages_map, failures = await self.__fetch(ids - foreign)

        for r in requests:
            if r.future.done():
                continue
            result = [
                None if mid in foreign else messages_map.get(mid) for mid in r.ids
            ]
            # A request is only failed when nothing it asked for came back;
            # one unusable id must not hide the messages next to it.
            error = next((failures[mid] for mid in r.ids if mid in failures), None)
            if error is not None and not any(msg is not None for msg in result):
                r.future.set_exception(error)
            else:
                r.future.set_result(result)

    @staticmethod
    def __addressable(message_id: int) -> bool:
        return 0 < message_id <= MAX_MESSAGE_ID

    async def __fetch_batch(self, ids: Tuple[int, ...]) -> GetMessagesResp:
        """Send one batch, moving to the next bot on failure."""
        e: Optional[Exception] = None
        bot = self.tdlib.next_bot
        for _ in range(self.__bots_count):
            try:
                return await asyncio.wait_for(
                    bot.get_messages(
                        GetMessagesReq(chat=self.private_file_channel, message_ids=ids)
                    ),
                    ATTEMPT_TIMEOUT_S,
                )
            except Exception as ex:
                e = ex
                name = await self.__bot_name(bot)
                logger.error(
                    f"{name} failed to get messages: {ex}, retrying with next bot..."
                )
                bot = self.tdlib.next_bot
        if e is None:
            raise RuntimeError("no bot was available to get messages")
        logger.error(f"All bots failed to get messages: {e}")
        raise e

    @staticmethod
    async def __bot_name(bot: ITDLibClient) -> str:
        """The failing bot's name for the log, itself kept on a leash."""
        try:
            return (await asyncio.wait_for(bot.get_me(), ATTEMPT_TIMEOUT_S)).name
        except Exception:
            return "a bot"

    async def __fetch(
        self, ids: Set[int]
    ) -> Tuple[Dict[int, MessageResp], Dict[int, Exception]]:
        """Fetch ``ids``, isolating the ids that cannot be fetched.

        The batch is retried on another bot as a whole first, so a bot that
        is down still costs one request per batch. Only when every bot
        refuses the batch is it split and each half tried on its own: a
        single id the channel rejects then fails just the requests that
        asked for it instead of every request in the window.
        """
        if not ids:
            return {}, {}
        ordered = tuple(sorted(ids))
        try:
            messages = await self.__fetch_batch(ordered)
        except Exception as ex:
            if len(ordered) == 1:
                return {}, {ordered[0]: ex}
            middle = len(ordered) // 2
            found: Dict[int, MessageResp] = {}
            failed: Dict[int, Exception] = {}
            for half in (ordered[:middle], ordered[middle:]):
                half_found, half_failed = await self.__fetch(set(half))
                found.update(half_found)
                failed.update(half_failed)
            if failed:
                logger.warning(
                    f"{len(failed)} of {len(ordered)} message id(s) could not "
                    f"be fetched; the other {len(found)} were served"
                )
            return found, failed
        return {msg.message_id: msg for msg in messages if msg is not None}, {}
