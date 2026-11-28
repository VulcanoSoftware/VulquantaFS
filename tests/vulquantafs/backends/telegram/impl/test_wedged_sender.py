"""A Telegram request that is cancelled or times out must leave a sender
the next call can reconnect.

The RPC-only client disables Telethon's update loop, so nothing pings the
connection. When the network disappears with its packets dropped instead
of refused, the socket stays half-open, ``is_connected()`` keeps returning
True, and every request hangs until the caller's bound cancels it. The
sender then looks dead forever -- after Telegram recovers, no read ever
succeeds again, and with the mirror store down too, downloads have
nowhere left to go. Dropping the sender on a dead request forces the next
call through a fresh ``connect()``.
"""

import asyncio
from unittest.mock import Mock

import pytest

from vulquantafs.backends.telegram.impl.telethon import TelethonAPI
from vulquantafs.errors import RpcTimeoutError


class WedgedClient:
    """A client that claims to be connected while every request hangs."""

    def __init__(self):
        self.connected = True
        self.connect_calls = 0
        self.disconnect_calls = 0

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connect_calls += 1
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False


async def _hang() -> None:
    await asyncio.Event().wait()


async def _answer(value: int) -> int:
    return value


class TestWedgedSenderRecovery:
    @pytest.mark.asyncio
    async def test_a_cancelled_request_drops_the_sender_and_the_next_call_reconnects(
        self,
    ):
        client = WedgedClient()
        api = TelethonAPI(client)  # type: ignore[arg-type]

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(api._connected_rpc("the probe", client, _hang), 0.05)  # type: ignore[arg-type]

        await asyncio.sleep(0.1)
        assert client.disconnect_calls == 1
        assert client.is_connected() is False

        assert await api._connected_rpc("the probe", client, lambda: _answer(7)) == 7  # type: ignore[arg-type]
        assert client.connect_calls == 1

    @pytest.mark.asyncio
    async def test_a_timed_out_request_drops_the_sender_too(self, mocker):
        client = WedgedClient()
        api = TelethonAPI(client)  # type: ignore[arg-type]
        cfg = Mock()
        cfg.vulquantafs.transfer.rpc_timeout_s = 0.05
        mocker.patch(
            "vulquantafs.backends.telegram.impl.telethon.get_config",
            return_value=cfg,
        )

        with pytest.raises(RpcTimeoutError):
            await api._connected_rpc("the probe", client, _hang)  # type: ignore[arg-type]

        await asyncio.sleep(0.1)
        assert client.disconnect_calls == 1

        assert await api._connected_rpc("the probe", client, lambda: _answer(7)) == 7  # type: ignore[arg-type]
        assert client.connect_calls == 1

    @pytest.mark.asyncio
    async def test_a_connected_sender_is_not_dropped_after_a_successful_call(self):
        client = WedgedClient()
        api = TelethonAPI(client)  # type: ignore[arg-type]

        assert await api._connected_rpc("the probe", client, lambda: _answer(7)) == 7  # type: ignore[arg-type]

        await asyncio.sleep(0.1)
        assert client.disconnect_calls == 0
        assert client.is_connected() is True
