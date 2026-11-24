import asyncio
import logging
import os
import random
from itertools import cycle
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from telethon import TelegramClient
from telethon import errors as tle
from telethon import functions as tlf
from telethon import types as tlt
from telethon.client.downloads import MAX_CHUNK_SIZE
from telethon.errors import FileReferenceExpiredError, SessionPasswordNeededError
from telethon.helpers import TotalList
from telethon.sessions import StringSession
from telethon.tl.types import InputDocumentFileLocation, PeerChannel

from vulquantafs.backends.telegram.interface import ITDLibClient
from vulquantafs.config import Config, get_config
from vulquantafs.errors import TechnicalError, UnDownloadableMessage
from vulquantafs.reqres import (
    DeleteMessagesReq,
    Document,
    DownloadFileReq,
    DownloadFileResp,
    EditMessageMediaReq,
    EditMessageTextReq,
    ForwardMessagesReq,
    GetMeResp,
    GetMessagesReq,
    GetMessagesResp,
    GetMessagesRespNoNone,
    GetPinnedMessageReq,
    Message,
    MessageResp,
    PinMessageReq,
    SaveBigFilePartReq,
    SaveFilePartReq,
    SaveFilePartResp,
    SearchMessageReq,
    SendFileReq,
    SendMessageResp,
    SendTextReq,
    aclose_content,
)
from vulquantafs.utils.chunk_cache import chunk_cache
from vulquantafs.utils.message_cache import channel_cache
from vulquantafs.utils.others import exclude_none

logger = logging.getLogger(__name__)

# Telegram answers ``upload.getFile`` requests that straddle a 1 MiB
# boundary with the bytes up to that boundary only, and Telethon takes any
# short reply for the end of the file -- so a download starting 8 KiB
# before such a boundary silently stops after 8 KiB. A request that starts
# at a multiple of its own size never straddles one, hence every download
# starts at such an offset and the bytes before the wanted one are dropped.
DOWNLOAD_REQUEST_SIZE = MAX_CHUNK_SIZE
EXTRA_CONNECTION_WARMUP_SECONDS = 5.0
STARTUP_DISCONNECT_SECONDS = 2.0
BOT_LOGIN_ATTEMPTS = 5
BOT_LOGIN_RETRY_DELAY = 5.0
BOT_LOGIN_STAGGER_SECONDS = 2.0
FLOOD_WAIT_MAX_SLEEP = 30.0
# Official Telegram production data centres as (dc_id, ip, port).
TELEGRAM_DATACENTERS: Tuple[Tuple[int, str, int], ...] = (
    (1, "149.154.175.53", 443),
    (2, "149.154.167.51", 443),
    (3, "149.154.175.100", 443),
    (4, "149.154.167.91", 443),
    (5, "91.108.56.130", 443),
)
T = TypeVar("T")


class _RPCOnlyTelegramClient(TelegramClient):
    """The storage backend uses requests, not Telegram event handlers."""

    async def _update_loop(self) -> None:
        # Telethon 1.40 starts a difference/catch-up loop even when the
        # server was told not to push updates. That loop can crash after
        # reconnecting and disconnect the sender in the middle of an upload.
        # Keep Telethon's sender, keepalive and RPC response handling intact.
        return


class TelethonAPI(ITDLibClient):
    def __init__(
        self,
        client: TelegramClient,
        extra_connections: Sequence[TelegramClient] = (),
    ):
        super().__init__()
        self._client = client
        # One MTProto connection carries its requests one after another, so
        # the file-part calls -- and only those -- are spread over every
        # connection this session has. Metadata calls stay on the primary
        # one, where the message cache and the session state live.
        self._transfer_clients: List[TelegramClient] = [client, *extra_connections]
        self.__transfer_cycle = cycle(self._transfer_clients)
        self._reconnect_locks = {
            id(conn): asyncio.Lock() for conn in self._transfer_clients
        }

    @property
    def _transfer_client(self) -> TelegramClient:
        return next(self.__transfer_cycle)

    async def _connected_rpc(
        self, what: str, client: TelegramClient, action: Callable[[], Awaitable[T]]
    ) -> T:
        async def run() -> T:
            # Telethon stops auto-reconnecting after its retry budget.
            # A later queue attempt must be able to use the same session
            # after the network returns, without restarting the server.
            if not client.is_connected():
                async with self._reconnect_locks[id(client)]:
                    if not client.is_connected():
                        await client.connect()
            return await action()

        try:
            return await self._bounded(what, run())
        except asyncio.CancelledError as ex:
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise
            # Disconnecting a Telethon sender cancels its pending RPC
            # futures, not the caller's task. Part uploads can retry that
            # request; an intentional caller shutdown must still propagate.
            raise ConnectionError(
                f"Telegram transport cancelled the request during {what}"
            ) from ex

    async def __get_messages(self, *args, **kwargs) -> Sequence[tlt.Message]:
        messages = await self._connected_rpc(
            "get_messages",
            self._client,
            lambda: self._client.get_messages(*args, **kwargs),
        )
        if not isinstance(messages, TotalList):
            raise TechnicalError("Unexpected response type from get_messages")
        return messages

    @staticmethod
    def _transform_messages(
        messages: Sequence[Optional[tlt.Message]],
    ) -> GetMessagesResp:
        res = GetMessagesResp()

        for m in messages:
            if not m:
                res.append(None)
                continue

            obj = MessageResp(
                message_id=m.id,
                text="",
                document=None,
            )

            if m.message:
                obj.text = m.message

            if (
                isinstance(m.media, tlt.MessageMediaDocument)
                and (doc := m.media.document)
                and not isinstance(doc, tlt.DocumentEmpty)
            ):
                obj.document = Document(
                    size=doc.size,
                    id=doc.id,
                    access_hash=doc.access_hash,
                    file_reference=doc.file_reference,
                    mime_type=doc.mime_type,
                )

            res.append(obj)
        return res

    async def get_messages(self, req: GetMessagesReq) -> GetMessagesResp:
        cache = channel_cache(req.chat).id
        if message_id_to_fetch := cache.find_nonexistent(req.message_ids):
            fetched_messages = await self.__get_messages(
                entity=PeerChannel(channel_id=req.chat), ids=message_id_to_fetch
            )

            for message in exclude_none(self._transform_messages(fetched_messages)):
                cache[message.message_id] = message

        return GetMessagesResp(cache.gets(req.message_ids))

    async def send_text(self, req: SendTextReq) -> SendMessageResp:
        message = await self._connected_rpc(
            "send_text",
            self._client,
            lambda: self._client.send_message(
                entity=PeerChannel(channel_id=req.chat), message=req.text
            ),
        )
        return SendMessageResp(message_id=message.id)

    async def edit_message_text(self, req: EditMessageTextReq) -> SendMessageResp:
        channel_cache(req.chat).id[req.message_id] = None
        chunk_cache().invalidate(req.chat, req.message_id)
        message = await self._connected_rpc(
            "edit_message_text",
            self._client,
            lambda: self._client.edit_message(
                entity=PeerChannel(channel_id=req.chat),
                message=req.message_id,
                text=req.text,
            ),
        )
        return SendMessageResp(message_id=message.id)

    async def edit_message_media(self, req: EditMessageMediaReq) -> Message:
        channel_cache(req.chat).id[req.message_id] = None
        chunk_cache().invalidate(req.chat, req.message_id)
        message = await self._bounded(
            "edit_message_media",
            self._client.edit_message(
                entity=PeerChannel(channel_id=req.chat),
                message=req.message_id,
                file=tlt.InputFile(
                    id=req.file.id,
                    parts=req.file.parts,
                    name=req.file.name,
                    md5_checksum="",
                ),
            ),
        )
        return Message(message_id=message.id)

    async def search_messages(self, req: SearchMessageReq) -> GetMessagesRespNoNone:
        cache = channel_cache(req.chat).search
        if req.search not in cache:
            messages = await self.__get_messages(
                entity=PeerChannel(channel_id=req.chat), search=req.search
            )
            cache[req.search] = tuple(exclude_none(self._transform_messages(messages)))
        return GetMessagesRespNoNone(cache[req.search])

    async def get_pinned_messages(
        self, req: GetPinnedMessageReq
    ) -> GetMessagesRespNoNone:
        return GetMessagesRespNoNone(
            list(
                exclude_none(
                    self._transform_messages(
                        await self.__get_messages(
                            entity=PeerChannel(channel_id=req.chat),
                            filter=tlt.InputMessagesFilterPinned(),
                        )
                    )
                )
            )
        )

    async def pin_message(self, req: PinMessageReq) -> None:
        await self._bounded(
            "pin_message",
            self._client.pin_message(
                entity=PeerChannel(channel_id=req.chat),
                message=req.message_id,
                notify=False,
            ),
        )

    async def save_big_file_part(self, req: SaveBigFilePartReq) -> SaveFilePartResp:
        client = self._transfer_client
        success = await self._connected_rpc(
            "save_big_file_part",
            client,
            lambda: client(
                tlf.upload.SaveBigFilePartRequest(
                    file_id=req.file_id,
                    file_part=req.file_part,
                    bytes=req.bytes,
                    file_total_parts=req.file_total_parts,
                )
            ),
        )
        return SaveFilePartResp(success=success)

    async def save_file_part(self, req: SaveFilePartReq) -> SaveFilePartResp:
        client = self._transfer_client
        success = await self._connected_rpc(
            "save_file_part",
            client,
            lambda: client(
                tlf.upload.SaveFilePartRequest(
                    file_id=req.file_id,
                    file_part=req.file_part,
                    bytes=req.bytes,
                )
            ),
        )
        return SaveFilePartResp(success=success)

    async def send_big_file(self, req: SendFileReq) -> SendMessageResp:
        file = tlt.InputFileBig(
            id=req.file.id,
            parts=req.file.parts,
            name=req.file.name,
        )
        message = await self._connected_rpc(
            "send_big_file",
            self._client,
            lambda: self._client.send_file(
                entity=PeerChannel(channel_id=req.chat),
                file=file,
                caption=req.caption,
                force_document=True,
            ),
        )
        if not isinstance(message, tlt.Message):
            raise TechnicalError("Unexpected response type from send_file")
        return SendMessageResp(message_id=message.id)

    async def send_small_file(self, req: SendFileReq) -> SendMessageResp:
        file = tlt.InputFile(
            id=req.file.id,
            parts=req.file.parts,
            name=req.file.name,
            md5_checksum="",
        )
        message = await self._connected_rpc(
            "send_small_file",
            self._client,
            lambda: self._client.send_file(
                entity=PeerChannel(channel_id=req.chat),
                file=file,
                caption=req.caption,
                force_document=True,
            ),
        )
        if not isinstance(message, tlt.Message):
            raise TechnicalError("Unexpected response type from send_file")
        return SendMessageResp(message_id=message.id)

    async def forward_messages(self, req: ForwardMessagesReq) -> list[SendMessageResp]:
        forwarded = await self._client.forward_messages(
            entity=PeerChannel(channel_id=req.to_chat),
            messages=list(req.message_ids),
            from_peer=PeerChannel(channel_id=req.from_chat),
        )
        if isinstance(forwarded, tlt.Message):
            forwarded = [forwarded]
        if len(forwarded) != len(req.message_ids) or any(m is None for m in forwarded):
            raise TechnicalError(
                f"Forwarding {len(req.message_ids)} messages from {req.from_chat} "
                f"to {req.to_chat} returned {len(forwarded)} messages"
            )
        return [SendMessageResp(message_id=m.id) for m in forwarded]

    async def _document_for(
        self, chat: int, message_id: int, refresh: bool = False
    ) -> Document:
        """Resolve the document of a file message through the message cache.

        A range request only needs the document's id, access hash and file
        reference, so resolving the message again for every request costs a
        round trip that the cache already paid for. ``refresh`` drops the
        cached entry first, which is how an expired file reference is
        recovered -- Telegram lets those go stale, and the cached copy would
        otherwise keep failing.
        """
        if refresh:
            channel_cache(chat).id[message_id] = None

        messages = await self.get_messages(
            GetMessagesReq(chat=chat, message_ids=(message_id,))
        )

        if not (message := messages[0]) or not message.document:
            raise UnDownloadableMessage(message_id)
        return message.document

    async def download_file(self, req: DownloadFileReq) -> DownloadFileResp:
        document = await self._document_for(req.chat, req.message_id)

        bytes_to_read = req.end - req.begin + 1

        async def chunks():
            rest = bytes_to_read

            if req.end < req.begin:
                raise TechnicalError(
                    f"Invalid range: end must be greater than or equal to begin, got begin={req.begin} end={req.end}"
                )

            doc = document
            offset = req.begin
            refreshed = False
            retried_connection = False

            while rest > 0:
                aligned = offset - offset % DOWNLOAD_REQUEST_SIZE
                skip = offset - aligned
                client = self._transfer_client
                downloader: Optional[AsyncIterator[bytes]] = None

                async def read_chunk() -> bytes:
                    nonlocal downloader
                    if downloader is None:
                        iterator: AsyncIterator[bytes] = client.iter_download(
                            file=InputDocumentFileLocation(
                                id=doc.id,
                                access_hash=doc.access_hash,
                                file_reference=doc.file_reference,
                                thumb_size="",
                            ),
                            offset=aligned,
                            request_size=DOWNLOAD_REQUEST_SIZE,
                        )
                        downloader = iterator
                    return await downloader.__anext__()

                try:
                    while True:
                        try:
                            # A cached document can skip the metadata RPC,
                            # and the reader may use a different pooled client
                            # anyway. Check the connection on the actual read.
                            chunk = await self._connected_rpc(
                                f"download of message {req.message_id}",
                                client,
                                read_chunk,
                            )
                        except StopAsyncIteration:
                            break
                        if skip:
                            if len(chunk) <= skip:
                                skip -= len(chunk)
                                continue
                            chunk = chunk[skip:]
                            skip = 0
                        if len(chunk) > rest:
                            chunk = chunk[:rest]
                        rest -= len(chunk)
                        offset += len(chunk)
                        yield chunk
                        if rest <= 0:
                            break
                except FileReferenceExpiredError:
                    # Re-resolve once and resume where the stream stopped.
                    if refreshed:
                        raise
                    refreshed = True
                    doc = await self._document_for(
                        req.chat, req.message_id, refresh=True
                    )
                    continue
                except ConnectionError:
                    # A disconnect can race the connection check or cancel
                    # an in-flight request. Rebuild the iterator at the last
                    # delivered byte; don't reuse its internal offset.
                    if retried_connection:
                        raise
                    retried_connection = True
                    continue
                finally:
                    if downloader is not None:
                        # Telethon's iterator uses close(), not aclose().
                        if (close := getattr(downloader, "close", None)) is not None:
                            await close()
                        else:
                            await aclose_content(downloader)
                break

            if rest > 0:
                # A stream that stops early would otherwise be passed on as
                # a complete one, and the caller has already promised the
                # full range to its client.
                raise TechnicalError(
                    f"Download of message {req.message_id} ended after "
                    f"{bytes_to_read - rest} of {bytes_to_read} bytes "
                    f"(range {req.begin}-{req.end})"
                )

        return DownloadFileResp(chunks=chunks(), size=bytes_to_read)

    async def delete_messages(self, req: DeleteMessagesReq) -> None:
        if not req.message_ids:
            return
        cache = channel_cache(req.chat).id
        for mid in req.message_ids:
            cache[mid] = None
            chunk_cache().invalidate(req.chat, mid)
        await self._client.delete_messages(
            entity=PeerChannel(channel_id=req.chat),
            message_ids=list(req.message_ids),
        )

    async def resolve_channel_id(self, channel_id: str) -> int:
        try:
            return int(channel_id)
        except ValueError:
            entity = await self._client.get_entity(f"@{channel_id}")
            if not isinstance(entity, tlt.Channel):
                raise TechnicalError("Expected a Telegram channel")
            return entity.id

    async def _get_me(self) -> GetMeResp:
        me = await self._client.get_me()
        if not isinstance(me, tlt.User):
            raise TechnicalError("Expected a Telegram user")
        return GetMeResp(
            name=(
                f"@{me.username}"
                if me.username
                else f"{me.first_name} {me.last_name or ''}".strip()
            ),
            is_premium=bool(me.premium),
        )


class Session:
    def __init__(self, session_file: str):
        self.session_file = session_file

    def get(self) -> Optional[StringSession]:
        if os.path.exists(self.session_file):
            with open(self.session_file, "r") as f:
                return StringSession(f.read().strip())
        return None

    def save_multibot(self, session_string: str):
        dir_ = os.path.dirname(self.session_file)
        if os.path.isfile(dir_):
            raise Exception(
                f"{dir_} is a session file which only supports one bot session. "
                f"Please remove the file to upgrade to multi-bot version."
            )
        os.makedirs(dir_, exist_ok=True)
        with open(self.session_file, "w") as f:
            f.write(session_string)

    def save(self, session_string: str):
        if not os.path.exists(self.session_file):
            dir_ = os.path.dirname(self.session_file)
            os.makedirs(dir_, exist_ok=True)
        with open(self.session_file, "w") as f:
            f.write(session_string)


def _is_transient_telegram_login_error(ex: BaseException) -> bool:
    if isinstance(
        ex,
        (
            tle.ServerError,
            tle.RpcCallFailError,
            tle.RpcMcgetFailError,
            tle.FloodWaitError,
            tle.FloodPremiumWaitError,
            tle.SlowModeWaitError,
        ),
    ):
        return True
    # Telethon raises this plain ValueError once its own retries against
    # Telegram's flaky internal errors (RPC_CALL_FAIL and friends) run
    # out; every other ValueError points at a broken session or call and
    # stays fatal.
    return isinstance(ex, ValueError) and str(ex).startswith("Request was unsuccessful")


async def _disconnect_after_startup_failure(client: TelegramClient) -> None:
    try:
        async with asyncio.timeout(STARTUP_DISCONNECT_SECONDS):
            await client.disconnect()
    except Exception as ex:
        logger.warning(
            "Telegram startup connection cleanup failed (%s)", type(ex).__name__
        )


async def open_extra_connections(
    config: Config, client: TelegramClient
) -> List[TelegramClient]:
    """Open the additional connections ``connection_pool_size`` asks for.

    Telegram accepts several simultaneous connections per authorization,
    and a single connection sends its requests one at a time -- so this is
    what lets one bot transfer more than one part at a time. A connection
    that cannot be opened is not fatal: the session simply keeps the ones
    it got, which is the same behaviour as a pool size of one.
    """
    extra = get_config().vulquantafs.transfer.connection_pool_size - 1
    if extra < 1:
        return []

    session_string = client.session.save()  # type: ignore[attr-defined]
    connections: List[TelegramClient] = []
    pending: TelegramClient | None = None
    try:
        # One budget for the entire optional pool, not one per connection.
        async with asyncio.timeout(EXTRA_CONNECTION_WARMUP_SECONDS):
            for _ in range(extra):
                pending = _RPCOnlyTelegramClient(
                    StringSession(session_string),
                    config.telegram.api_id,
                    config.telegram.api_hash,
                    receive_updates=False,
                )
                await pending.connect()
                connections.append(pending)
                pending = None
    except asyncio.CancelledError:
        if pending is not None:
            connections.append(pending)
        await asyncio.gather(
            *(_disconnect_after_startup_failure(c) for c in connections),
        )
        raise
    except Exception as ex:
        logger.warning(
            "Telegram optional connections: opened %s of %s extras (%s); continuing",
            len(connections),
            extra,
            type(ex).__name__,
        )
        if pending is not None:
            await _disconnect_after_startup_failure(pending)

    if connections:
        logger.info(f"Opened {len(connections) + 1} connections for this session")
    return connections


async def login_as_account(config: Config) -> TelegramClient:
    if not config.telegram.account:
        raise TechnicalError("Account configuration is missing")

    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash

    session = Session(config.telegram.account.session_file)
    if sess := session.get():
        client = _RPCOnlyTelegramClient(sess, api_id, api_hash, receive_updates=False)
        await client.connect()
    else:
        client = _RPCOnlyTelegramClient(
            StringSession(), api_id, api_hash, receive_updates=False
        )
        await client.connect()

        phone_number = input("Phone number (with country code): ")
        sms_req = await client.send_code_request(phone_number, force_sms=False)
        code = input("Enter the code you received: ")

        try:
            await client.sign_in(
                phone=phone_number, code=code, phone_code_hash=sms_req.phone_code_hash
            )
        except SessionPasswordNeededError:
            password = input("Enter the 2FA password: ")
            await client.sign_in(password=password)
        except Exception as e:
            logger.error(f"Failed to sign in: {e}")
            raise
        session.save(client.session.save())  # type: ignore

    if (me := await client.get_me()) and isinstance(me, tlt.User) and me.username:
        logger.info(f"logged in as @{me.username}")
    else:
        logger.warning("logged in as account, but no username found")

    return client


async def login_as_bots(config: Config) -> List[TelegramClient]:
    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash

    bot_tokens = config.telegram.bot.tokens or [config.telegram.bot.token]
    timeout = config.telegram.bot.login_timeout_s
    logger.info("Telegram: logging in %s bot(s) via Telethon", len(bot_tokens))
    connected: List[TelegramClient] = []

    async def login(index: int, token: str) -> TelegramClient | None:
        bot_id, _ = token.split(":")

        session = Session(
            os.path.join(config.telegram.bot.session_file, f"{bot_id}.session")
        )

        sess = session.get()
        if sess is None:
            # Fresh sign-ins authenticate one by one so the bot fleet
            # never hammers Telegram's auth endpoint at the same time.
            await asyncio.sleep(BOT_LOGIN_STAGGER_SECONDS * index)

        # A fresh sign-in has no auth key yet, so it may start on any data
        # centre. Shuffle once per login sequence: the first DC is random
        # and each retry moves on to one that has not been tried yet.
        # A stored session's auth key belongs to its own DC, so moving it
        # elsewhere would only invalidate it; it keeps the DC it was saved on.
        dc_order = random.sample(TELEGRAM_DATACENTERS, len(TELEGRAM_DATACENTERS))

        attempts = 0
        while True:
            client = _RPCOnlyTelegramClient(
                sess if sess is not None else StringSession(),
                api_id,
                api_hash,
                receive_updates=False,
            )
            if sess is None:
                dc_id, dc_ip, dc_port = dc_order[attempts % len(dc_order)]
                client.session.set_dc(dc_id, dc_ip, dc_port)  # type: ignore[attr-defined]
            else:
                dc_id = sess.dc_id
                dc_ip = sess.server_address
            try:
                async with asyncio.timeout(timeout):
                    # Bound the auth-key exchange, data-centre migration,
                    # and first RPC, not just the network connect.
                    await client.connect()
                    if sess is None:
                        await client.start(bot_token=token)  # type: ignore
                        session.save_multibot(client.session.save())  # type: ignore
                    me = await client.get_me()
            except (TimeoutError, ConnectionError) as ex:
                logger.warning(
                    "Telegram bot %s not ready (%s; login budget %gs; DC%s %s); excluding it from startup",
                    bot_id,
                    type(ex).__name__,
                    timeout,
                    dc_id,
                    dc_ip,
                )
                await _disconnect_after_startup_failure(client)
                return None
            except asyncio.CancelledError:
                await _disconnect_after_startup_failure(client)
                raise
            except Exception as ex:
                await _disconnect_after_startup_failure(client)
                if not _is_transient_telegram_login_error(ex):
                    raise
                attempts += 1
                if attempts >= BOT_LOGIN_ATTEMPTS:
                    logger.warning(
                        "Telegram bot %s not ready (%s after %s attempts; last DC%s %s); excluding it from startup",
                        bot_id,
                        type(ex).__name__,
                        attempts,
                        dc_id,
                        dc_ip,
                    )
                    return None
                wait = getattr(ex, "seconds", None)
                if not isinstance(wait, (int, float)) or wait <= 0:
                    wait = BOT_LOGIN_RETRY_DELAY * 2 ** (attempts - 1)
                wait = min(wait, FLOOD_WAIT_MAX_SLEEP)
                logger.warning(
                    "Telegram bot %s login failed on DC%s %s (%s; attempt %s/%s); trying again in %gs",
                    bot_id,
                    dc_id,
                    dc_ip,
                    type(ex).__name__,
                    attempts,
                    BOT_LOGIN_ATTEMPTS,
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                if isinstance(me, tlt.User) and me.username:
                    logger.info(f"logged in as @{me.username}")
                else:
                    logger.warning("logged in as bot, but no username found")

                connected.append(client)
                return client

    try:
        results = await asyncio.gather(
            *(login(index, token) for index, token in enumerate(bot_tokens)),
            return_exceptions=True,
        )
    except asyncio.CancelledError:
        await asyncio.gather(*(_disconnect_after_startup_failure(c) for c in connected))
        raise
    clients: List[TelegramClient] = []
    failure: BaseException | None = None
    for result in results:
        if isinstance(result, BaseException):
            failure = failure or result
        elif result is not None:
            clients.append(result)
    if failure is not None:
        await asyncio.gather(*(_disconnect_after_startup_failure(c) for c in clients))
        raise failure
    if not clients:
        raise TechnicalError(
            "None of the configured Telegram bots could log in; check the connection or session"
        )
    logger.info("Telegram: %s/%s bot(s) ready", len(clients), len(bot_tokens))
    return clients
