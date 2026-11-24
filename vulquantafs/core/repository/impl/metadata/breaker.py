"""Fail fast while a metadata store's service is unreachable.

The GitHub metadata stores talk to their service through PyGithub, a
synchronous client whose retry policy pays a full connect or read timeout
for every attempt. With api.github.com down, each call costs half a
minute or more -- for the boot failover, but also for every incremental
write, stats commit, timestamp lookup and heal that comes after it: the
replicated layer copes with the failures (the write still lands on the
other stores and the store is marked stale), but every caller pays the
same dead connection over and over.

The breaker remembers that the service just proved unreachable and
refuses every further call immediately, raising a
:class:`StoreUnreachable` that behaves like the failed call it
shortcuts. Nothing about the guarantees changes: the caller marks the
store stale exactly as before, and a background probe watches the
service -- once it answers again, the breaker closes and the heal worker
brings the store back in line.

Every further trip in a row doubles the probe interval, up to a cap, and
a call the service actually answers resets the escalation. A store that
answers its probe but whose writes keep deadlining (storage that cannot
serve the work, not a network outage) then costs a single call timeout
now and then instead of one per interval.

Only transport-level failures (a ConnectionError, a timeout) open the
breaker. An HTTP status answer, however unfriendly, means the service is
up; the per-request retry handling already deals with those.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Optional, TypeVar

import requests

from vulquantafs.errors import TechnicalError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Transport-level failures: the attempt never reached a working HTTP
# server. A status code in the answer means the service is up.
UNREACHABLE_ERRORS = (requests.exceptions.RequestException, OSError)

# After a connection-level failure the store's calls are refused for this
# long while a background probe waits for the service. Matches the
# metadata heal interval: the heal finds the store eventually answer, at
# most one probe interval after it came back.
PROBE_COOLDOWN_SECONDS = 30.0

# Every consecutive trip multiplies the probe interval by two, up to this
# factor: a store that deadlines its writes while answering reads then
# costs one write timeout per escalated interval instead of per interval.
# With the 30 s cooldown the last step sits at 8 minutes; a store that
# recovers is at most one interval late in coming back.
PROBE_COOLDOWN_MAX_MULTIPLIER = 16.0

# ``None`` as a cooldown reads the module constant above, so patching
# that constant applies to breakers built after it.
_COOLDOWN_DEFAULT: Optional[float] = None


def is_unreachable_error(ex: BaseException) -> bool:
    """Whether ``ex`` means the call never reached a working server."""
    return isinstance(ex, UNREACHABLE_ERRORS) or isinstance(ex, StoreUnreachable)


class StoreUnreachable(TechnicalError):
    """The store's service is (still) unreachable; the breaker refused
    the call instead of letting it run into the connection timeout."""


def probe_github_branch(repo: Any, branch: str) -> Callable[[], None]:
    """A cheap reachability probe: read the branch's ref. A missing
    branch raises a ``GithubException`` -- the service answered, which is
    what the probe is after."""

    def probe() -> None:
        repo.get_git_ref(f"heads/{branch}")

    return probe


class StoreCircuitBreaker:
    """Refuses calls while the service is unreachable.

    The first transport-level failure opens the breaker. Every call
    until the probe finds the service again raises immediately, so the
    callers of a down store never queue for its connection timeouts. The
    probe runs on a one-at-a-time daemon thread: it waits for the
    cooldown, performs the cheap reachability call and either closes the
    breaker or pushes the next attempt back by another cooldown. Every
    further trip in a row doubles that cooldown until the cap, and a
    call the service answers resets the escalation: a store that answers
    reads but deadlines its writes is re-tried later and later, not
    once an interval.
    """

    def __init__(
        self,
        probe: Callable[[], None],
        cooldown: Optional[float] = _COOLDOWN_DEFAULT,
    ):
        self._probe = probe
        self._cooldown = PROBE_COOLDOWN_SECONDS if cooldown is None else cooldown
        self._lock = threading.Lock()
        self._open = False
        self._reopen_at = 0.0
        self._trips_in_a_row = 0
        self._last_error: Optional[BaseException] = None
        self._prober: Optional[threading.Thread] = None
        # Wakes the prober for a prompt stop, instead of letting it run
        # out its last cooldown.
        self._wake = threading.Event()

    def stop(self) -> None:
        """End the background probing; a breaker that is open keeps
        refusing calls. The daemon thread makes this optional for the
        stores, but tests use it so a leftover prober never reaches the
        network mocks of a follow-up test."""
        self._wake.set()

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run ``fn``; refuse it immediately while the breaker is open."""
        with self._lock:
            if self._open:
                self._start_prober_locked()
                blocked = self._last_error
            else:
                blocked = None
        if blocked is not None:
            raise StoreUnreachable(
                f"the service is unreachable; the call was refused until "
                f"the next probe: {blocked}"
            ) from blocked
        try:
            result = fn(*args, **kwargs)
        except Exception as ex:
            if is_unreachable_error(ex):
                with self._lock:
                    self._trips_in_a_row += 1
                    self._open = True
                    self._last_error = ex
                    self._reopen_at = (
                        time.monotonic() + self._escalated_cooldown_locked()
                    )
                    self._start_prober_locked()
            else:
                # The service answered this call: the outage it refused
                # for is over as far as this store is concerned.
                with self._lock:
                    self._trips_in_a_row = 0
            raise
        with self._lock:
            self._trips_in_a_row = 0
        return result

    def _escalated_cooldown_locked(self) -> float:
        """The probe interval after ``self._trips_in_a_row`` consecutive
        unreachable results: the base cooldown, doubled per further trip
        until the multiplier above caps it."""
        growth = 2.0 ** max(self._trips_in_a_row - 1, 0)
        return self._cooldown * min(PROBE_COOLDOWN_MAX_MULTIPLIER, growth)

    def _start_prober_locked(self) -> None:
        if self._prober is None:
            self._prober = threading.Thread(
                target=self._probe_until_it_answers,
                name="metadata-store-probe",
                daemon=True,
            )
            self._prober.start()

    def _probe_until_it_answers(self) -> None:
        try:
            while not self._wake.is_set():
                with self._lock:
                    if not self._open:
                        return
                    probe, reopen_at = self._probe, self._reopen_at
                delay = reopen_at - time.monotonic()
                if delay > 0 and self._wake.wait(delay):
                    return
                try:
                    probe()
                except Exception as ex:
                    if not is_unreachable_error(ex):
                        # The service answered, the request itself
                        # refused: this is an outage over, not a
                        # hanging one.
                        pass
                    else:
                        with self._lock:
                            if self._open:
                                self._last_error = ex
                                self._reopen_at = (
                                    time.monotonic() + self._escalated_cooldown_locked()
                                )
                        continue
                with self._lock:
                    self._open = False
                    self._last_error = None
                    return
        finally:
            with self._lock:
                self._prober = None


class FailFastRepository:
    """A PyGithub ``Repository`` behind a circuit breaker.

    Every attribute fetch and every method call goes through the
    breaker: fetching a property completes a lazily resolved repository,
    which is a request too, and the metadata store's flows all begin on
    the repository itself. Derived objects (a ``GitRef``, a
    ``ContentFile``) keep the raw requester -- but they only come to life
    after a guarded call succeeded, so a hanging service never produces
    them.
    """

    def __init__(self, inner: Any, breaker: StoreCircuitBreaker):
        self._inner = inner
        self._breaker = breaker

    def __getattr__(self, name: str) -> Any:
        inner, breaker = self._inner, self._breaker
        # The fetch itself is guarded -- on a lazy object it completes the
        # repository (a request), and it evaluates the property.
        value = breaker.call(getattr, inner, name)
        if callable(value):

            def guarded(*args: Any, **kwargs: Any) -> Any:
                return breaker.call(value, *args, **kwargs)

            return guarded
        return value
