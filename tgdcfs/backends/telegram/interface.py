import asyncio
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from itertools import cycle
from typing import Awaitable, Optional, Sequence

from tgdcfs.config import get_config
from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import (
    DeleteMessagesReq,
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
    PinMessageReq,
    SaveBigFilePartReq,
    SaveFilePartReq,
    SaveFilePartResp,
    SearchMessageReq,
    SendFileReq,
    SendMessageResp,
    SendTextReq,
)


class ITDLibClient(metaclass=ABCMeta):
    def __init__(self):
        self._me: Optional[GetMeResp] = None

    async def _bounded(self, what: str, action: Awaitable):
        """Turn the endless silence of a down service into an error.

        The library behind this client re-connects to Telegram forever on
        its own, so a call into a network that simply drops the packets
        never returns: the write failover of a mirror group -- which
        trips its breaker only on exceptions -- would then wait out the
        whole outage without ever failing over. A call that stays silent
        for the configured ``rpc_timeout_s`` becomes a
        :class:`TechnicalError`, which reads and saves treat like any
        other store failure. ``0`` switches the bound off.
        """
        timeout = get_config().tgdcfs.transfer.rpc_timeout_s
        if timeout <= 0:
            return await action
        try:
            return await asyncio.wait_for(action, float(timeout))
        except asyncio.TimeoutError as ex:
            raise TechnicalError(
                f"Telegram did not answer {what} within {timeout}s"
            ) from ex

    @abstractmethod
    async def get_messages(self, req: GetMessagesReq) -> GetMessagesResp:
        pass

    @abstractmethod
    async def send_text(self, req: SendTextReq) -> SendMessageResp:
        pass

    @abstractmethod
    async def edit_message_text(self, req: EditMessageTextReq) -> SendMessageResp:
        pass

    @abstractmethod
    async def search_messages(self, req: SearchMessageReq) -> GetMessagesRespNoNone:
        pass

    @abstractmethod
    async def get_pinned_messages(
        self, req: GetPinnedMessageReq
    ) -> GetMessagesRespNoNone:
        pass

    @abstractmethod
    async def pin_message(self, req: PinMessageReq) -> None:
        pass

    @abstractmethod
    async def save_big_file_part(self, req: SaveBigFilePartReq) -> SaveFilePartResp:
        pass

    @abstractmethod
    async def save_file_part(self, req: SaveFilePartReq) -> SaveFilePartResp:
        pass

    @abstractmethod
    async def send_big_file(self, req: SendFileReq) -> SendMessageResp:
        pass

    @abstractmethod
    async def send_small_file(self, req: SendFileReq) -> SendMessageResp:
        pass

    @abstractmethod
    async def edit_message_media(self, req: EditMessageMediaReq) -> Message:
        pass

    @abstractmethod
    async def forward_messages(self, req: ForwardMessagesReq) -> list[SendMessageResp]:
        """Server-side copy of messages into another chat.

        Returns the new message ids in ``to_chat``, in the same order as
        ``req.message_ids``. No file content is re-uploaded.
        """
        pass

    @abstractmethod
    async def download_file(self, req: DownloadFileReq) -> DownloadFileResp:
        pass

    @abstractmethod
    async def delete_messages(self, req: DeleteMessagesReq) -> None:
        pass

    @abstractmethod
    async def resolve_channel_id(self, channel_id: str) -> int:
        pass

    @abstractmethod
    async def _get_me(self) -> GetMeResp:
        pass

    async def get_me(self) -> GetMeResp:
        if self._me is None:
            self._me = await self._get_me()
        return self._me


@dataclass
class TDLibApi:
    bots: Sequence[ITDLibClient]
    account: Optional[ITDLibClient] = None

    def __post_init__(self):
        self.__bots_cycle = cycle(self.bots)

    @property
    def bot(self) -> ITDLibClient:
        return self.bots[0]

    @property
    def next_bot(self) -> ITDLibClient:
        return next(self.__bots_cycle)
