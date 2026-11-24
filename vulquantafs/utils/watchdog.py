"""Warn about (and dump evidence of) event-loop stalls.

A stall freezes everything at once -- every SFTP session, the HTTP API
and the bot gateways go silent, and the log records nothing over the
whole stall. The watchdog measures how long the loop did not run, and a
sampler thread prints the event-loop thread's Python stack. If the sampler
also missed its deadline, the stack may show where the loop resumed rather
than what stalled it.
"""

import asyncio
import logging
import sys
import threading
import time
import traceback
from typing import Optional

logger = logging.getLogger(__name__)

CHECK_INTERVAL_SECONDS = 1.0
WARN_LAG_SECONDS = 5.0
DUMP_LAG_SECONDS = 10.0
STACK_FRAME_LIMIT = 15


def _thread_stack(ident: Optional[int]) -> Optional[str]:
    """The Python frames of the thread ``ident`` at call time, or ``None``
    when the thread is gone or sits inside a call without a Python frame."""
    if ident is None:
        return None
    frame = sys._current_frames().get(ident)
    if frame is None:
        return None
    return "".join(traceback.format_stack(frame, limit=STACK_FRAME_LIMIT)).strip()


class EventLoopWatchdog:
    """One task measures loop responsiveness; one daemon thread samples it.

    The sampler can be late too, for example when the process is suspended
    or a native call holds the GIL. Report its own delay separately rather
    than attributing the stall to whichever frame is observed on recovery.
    """

    def __init__(
        self,
        interval: float = CHECK_INTERVAL_SECONDS,
        warn_after: float = WARN_LAG_SECONDS,
        dump_after: float = DUMP_LAG_SECONDS,
    ):
        self._interval = interval
        self._warn_after = warn_after
        self._dump_after = dump_after
        self._last_tick = time.monotonic()
        self._closing = asyncio.Event()
        self._sampler_stop = threading.Event()
        self._task: Optional[asyncio.Task[None]] = None
        self._loop_thread: Optional[int] = None

    def start(self) -> None:
        self._task = asyncio.get_running_loop().create_task(self.run())

    async def stop(self) -> None:
        self._closing.set()
        self._sampler_stop.set()
        if self._task is not None and not self._task.done():
            await self._task

    async def run(self) -> None:
        self._loop_thread = threading.get_ident()
        self._last_tick = time.monotonic()
        sampler = threading.Thread(
            target=self._sample, name="loop-stall-sampler", daemon=True
        )
        sampler.start()
        while True:
            self._last_tick = time.monotonic()
            try:
                await asyncio.wait_for(self._closing.wait(), timeout=self._interval)
                return
            except asyncio.TimeoutError:
                pass
            frozen = time.monotonic() - self._last_tick
            if frozen >= self._warn_after:
                logger.warning("The event loop did not run for %.1f seconds", frozen)

    def _sample(self) -> None:
        while True:
            deadline = time.monotonic() + self._interval
            if self._sampler_stop.wait(self._interval):
                return
            now = time.monotonic()
            late_by = now - deadline
            stalled_for = now - self._last_tick
            if late_by >= self._dump_after:
                logger.warning(
                    "The watchdog sampler itself ran %.1f seconds late; "
                    "a stack sampled at recovery may not identify the blocker",
                    late_by,
                )
            if stalled_for < self._dump_after:
                continue
            stack = _thread_stack(self._loop_thread)
            if stack is not None:
                logger.warning(
                    "The event-loop thread has not run for %.1f seconds; "
                    "sampled stack:\n%s",
                    stalled_for,
                    stack,
                )
            # One dump per stall: wait for the loop to tick again (or
            # the shutdown) before the sampler may look at it next.
            while time.monotonic() - self._last_tick >= self._interval:
                if self._sampler_stop.wait(self._interval):
                    return
