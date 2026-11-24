"""Shared request pacing and 429 recovery for both GitLab transports."""

import asyncio
import logging
import math
import random
import threading
import time
from collections.abc import Mapping
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import NamedTuple
from urllib.parse import urlsplit
from weakref import WeakValueDictionary

from github import GithubException

logger = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 300
_GATES: WeakValueDictionary[str, "GitlabRequestGate"] = WeakValueDictionary()
_GATES_LOCK = threading.Lock()


class GitlabRateLimited(GithubException):
    def __init__(self, retry_at: float):
        super().__init__(429, {"message": "GitLab rate-limit cooldown active"})
        self.retry_at = retry_at


class RequestTicket(NamedTuple):
    epoch: int
    probe: bool


def _header_delay(headers: Mapping[str, str]) -> float:
    """Honor Retry-After (seconds or HTTP date) and RateLimit-Reset."""
    retry_after = headers.get("Retry-After", "")
    try:
        seconds = float(retry_after)
    except ValueError:
        try:
            date = parsedate_to_datetime(retry_after)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = date.timestamp() - time.time()
        except (ValueError, OverflowError, OSError):
            seconds = 0
    try:
        reset = float(headers.get("RateLimit-Reset", "")) - time.time()
    except ValueError:
        reset = 0
    return max((0.0, *(n for n in (seconds, reset) if math.isfinite(n))))


def gate_for(base_url: str, requests_per_second: float) -> "GitlabRequestGate":
    # GitLab may limit by account, project, endpoint or source IP. Pace
    # every store talking to the same host, including different layouts
    # and tokens. Where rates differ, the most conservative one wins.
    host = urlsplit(base_url).hostname or ""
    with _GATES_LOCK:
        gate = _GATES.get(host)
        if gate is None:
            gate = _GATES[host] = GitlabRequestGate(host, requests_per_second)
        else:
            gate.limit_to(requests_per_second)
        return gate


class GitlabRequestGate:
    def __init__(self, host: str, requests_per_second: float):
        self._host = host
        self._spacing = 1 / requests_per_second
        self._lock = threading.Lock()
        self._last_send = -math.inf
        self._paused_until = 0.0
        self._epoch = 0
        self._streak = 0
        self._probing = False

    def limit_to(self, requests_per_second: float) -> None:
        with self._lock:
            self._spacing = max(self._spacing, 1 / requests_per_second)

    def in_cooldown(self) -> bool:
        with self._lock:
            return self._probing or time.monotonic() < self._paused_until

    def _claim(self, wait_on_cooldown: bool) -> tuple[RequestTicket | None, float]:
        with self._lock:
            now = time.monotonic()
            if self._probing or now < self._paused_until:
                ready = max(self._paused_until, now + 0.05 if self._probing else now)
                if not wait_on_cooldown:
                    raise GitlabRateLimited(ready)
                return None, ready - now
            delay = self._last_send + self._spacing - now
            if delay > 0:
                return None, delay
            self._last_send = now
            self._probing = bool(self._streak)
            return RequestTicket(self._epoch, self._probing), 0.0

    def acquire(self, *, wait_on_cooldown: bool = False) -> RequestTicket:
        while True:
            ticket, delay = self._claim(wait_on_cooldown)
            if ticket is not None:
                return ticket
            time.sleep(delay)

    async def acquire_async(self) -> RequestTicket:
        while True:
            ticket, delay = self._claim(False)
            if ticket is not None:
                return ticket
            await asyncio.sleep(delay)

    def finish(
        self, ticket: RequestTicket, status: int | None, headers: Mapping[str, str]
    ) -> GitlabRateLimited | None:
        """Return a 429 for the caller, and publish at most one backoff per wave."""
        server_delay = _header_delay(headers) if status == 429 else 0.0
        with self._lock:
            if ticket.probe:
                self._probing = False
            if status != 429:
                # A response from before the 429 must not clear a cooldown.
                if ticket.probe and ticket.epoch == self._epoch and status is not None:
                    self._streak = 0
                return None
            now = time.monotonic()
            if ticket.epoch == self._epoch or not self._streak:
                self._epoch += 1
                self._streak = min(self._streak + 1, 10)
                backoff = min(2.0 ** (self._streak - 1), MAX_BACKOFF_SECONDS)
                jitter = random.uniform(1, 1.25)  # noqa: S311 (retry jitter)
                delay = max(server_delay, min(backoff * jitter, MAX_BACKOFF_SECONDS))
                self._paused_until = max(self._paused_until, now + delay)
                logger.warning(
                    "GitLab rate limited %s; waiting %.1fs", self._host, delay
                )
            elif server_delay:
                self._paused_until = max(self._paused_until, now + server_delay)
            return GitlabRateLimited(self._paused_until)
