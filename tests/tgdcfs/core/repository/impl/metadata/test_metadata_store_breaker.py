"""The circuit breaker behind the fail-fast GitHub stores.

An api.github.com outage used to cost the full PyGithub retry cycle
(connection timeouts) for every listing, write, stats commit and heal.
The breaker refuses those calls once the service proved unreachable,
and a background probe alone waits for it to answer again. These tests
pin the breaker down on its own, without a whole GitHub behind them.
"""

import time

import pytest
import requests
from github import GithubException

from tgdcfs.core.repository.impl.metadata.breaker import (
    PROBE_COOLDOWN_SECONDS,
    FailFastRepository,
    StoreCircuitBreaker,
    StoreUnreachable,
    is_unreachable_error,
)
from tgdcfs.core.repository.impl.metadata.github_repo.gh_directory import (
    _is_transient,
)

CONNECT_TIMEOUT = requests.exceptions.ConnectTimeout(
    "Connection to api.github.com timed out."
)


class Probe:
    """A probe script: the first ``failing`` attempts raise the timeout,
    afterwards the service answers."""

    def __init__(self, failing: int = 0):
        self.calls = 0
        self.failing = failing

    def __call__(self) -> None:
        self.calls += 1
        if self.failing > 0:
            self.failing -= 1
            raise CONNECT_TIMEOUT


class Sunk:
    """A breaker-protected call: every call raises, and they count."""

    def __init__(self, error: Exception):
        self.calls = 0
        self.error = error

    def __call__(self, *args, **kwargs):
        self.calls += 1
        raise self.error


@pytest.fixture
def make_breaker():
    """Tracks every breaker a test made and stops their probe threads in
    teardown: a leftover prober must never reach the network or the
    mocks of a follow-up test."""

    made: list[StoreCircuitBreaker] = []

    def make(**kwargs):
        breaker = StoreCircuitBreaker(**kwargs)
        made.append(breaker)
        return breaker

    yield make
    for breaker in made:
        breaker.stop()


class TestStoreCircuitBreaker:
    def test_a_down_service_is_refused_without_further_attempts(self, make_breaker):
        breaker = make_breaker(probe=Probe(failing=99), cooldown=60.0)
        sink = Sunk(CONNECT_TIMEOUT)

        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)
        assert sink.calls == 1

        start = time.monotonic()
        for _ in range(3):
            with pytest.raises(StoreUnreachable):
                breaker.call(sink)
        # The refusals are immediate; the service got no further calls.
        assert sink.calls == 1
        assert time.monotonic() - start < 1.0

    def test_a_refusal_names_the_service_problem_it_shortcuts(self, make_breaker):
        breaker = make_breaker(probe=Probe(failing=99), cooldown=60.0)
        sink = Sunk(CONNECT_TIMEOUT)
        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)

        with pytest.raises(StoreUnreachable) as refused:
            breaker.call(sink)
        assert "Connection to api.github.com timed out." in str(refused.value)

    def test_an_answered_request_never_opens_the_breaker(self, make_breaker):
        breaker = make_breaker(probe=Probe(), cooldown=0.01)
        sink = Sunk(GithubException(500, {"message": "Internal error"}, None))

        for _ in range(3):
            with pytest.raises(GithubException):
                breaker.call(sink)
        assert not breaker._open
        assert sink.calls == 3

    def test_an_answerless_probe_keeps_the_breaker_open(self, make_breaker):
        probe = Probe(failing=10)
        breaker = make_breaker(probe=probe, cooldown=0.05)
        sink = Sunk(CONNECT_TIMEOUT)
        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)

        start = time.monotonic()
        deadline = time.monotonic() + 10
        while breaker._open and time.monotonic() < deadline:
            time.sleep(0.005)
        assert not breaker._open
        # Ten answered-failed probes are ten cooldowns apart: a dead
        # service is probed at a steady, slow pace, never in a loop.
        assert probe.calls == 11
        assert time.monotonic() - start >= 10 * 0.05 - 0.01

    def test_the_probe_closes_the_breaker_when_the_service_answers(self, make_breaker):
        probe = Probe(failing=2)
        breaker = make_breaker(probe=probe, cooldown=0.01)
        sink = Sunk(CONNECT_TIMEOUT)
        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)

        deadline = time.monotonic() + 5
        while breaker._open and time.monotonic() < deadline:
            time.sleep(0.005)
        assert not breaker._open

        healthy = Sunk(RuntimeError("the request itself refused (a bug)"))
        with pytest.raises(RuntimeError):
            breaker.call(healthy)
        assert healthy.calls == 1

    def test_the_default_cooldown_is_the_module_constant(self, make_breaker):
        assert make_breaker(probe=Probe())._cooldown == PROBE_COOLDOWN_SECONDS

    def test_consecutive_trips_double_the_cooldown_up_to_the_cap(self):
        breaker = StoreCircuitBreaker(probe=Probe(), cooldown=30.0)
        breaker.stop()
        with breaker._lock:
            assert breaker._escalated_cooldown_locked() == 30.0
            breaker._trips_in_a_row = 2
            assert breaker._escalated_cooldown_locked() == 60.0
            breaker._trips_in_a_row = 3
            assert breaker._escalated_cooldown_locked() == 120.0
            breaker._trips_in_a_row = 99
            assert breaker._escalated_cooldown_locked() == 30.0 * 16

    def test_every_trip_in_a_row_reopens_with_a_longer_cooldown(self, make_breaker):
        probe = Probe()
        breaker = make_breaker(probe=probe, cooldown=0.05)
        sink = Sunk(CONNECT_TIMEOUT)

        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)
        assert breaker._trips_in_a_row == 1
        deadline = time.monotonic() + 5
        while breaker._open and time.monotonic() < deadline:
            time.sleep(0.005)
        assert probe.calls == 1

        # The probe answered, the breaker closed -- and the very next real
        # call deadlines again. That second trip in a row must hold the
        # calls for twice the cooldown, not once more the first interval.
        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)
        assert breaker._trips_in_a_row == 2
        start = time.monotonic()
        time.sleep(0.075)
        assert breaker._open, "the second trip must outlast one cooldown"
        deadline = time.monotonic() + 5
        while breaker._open and time.monotonic() < deadline:
            time.sleep(0.005)
        assert not breaker._open
        assert time.monotonic() - start >= 2 * 0.05 - 0.01

    def test_a_call_the_service_answers_resets_the_escalation(self, make_breaker):
        breaker = make_breaker(probe=Probe(), cooldown=0.05)
        sink = Sunk(CONNECT_TIMEOUT)
        for _ in range(2):
            with pytest.raises(requests.exceptions.ConnectTimeout):
                breaker.call(sink)
            deadline = time.monotonic() + 5
            while breaker._open and time.monotonic() < deadline:
                time.sleep(0.005)
        assert breaker._trips_in_a_row == 2

        assert breaker.call(Probe()) is None
        assert breaker._trips_in_a_row == 0

        # And the next trip therefore starts over at the base cooldown.
        with pytest.raises(requests.exceptions.ConnectTimeout):
            breaker.call(sink)
        assert breaker._trips_in_a_row == 1


class TestFailFastRepository:
    def test_a_property_fetch_hits_the_breaker(self, make_breaker):
        breaker = make_breaker(probe=Probe(failing=99), cooldown=60.0)

        class Lazy:
            fetches = 0

            @property
            def created_at(self):
                Lazy.fetches += 1
                raise CONNECT_TIMEOUT

        repo = FailFastRepository(Lazy(), breaker)

        with pytest.raises(requests.exceptions.ConnectTimeout):
            repo.created_at
        assert Lazy.fetches == 1

        # Known down: the property fetch is refused outright.
        with pytest.raises(StoreUnreachable):
            repo.created_at
        assert Lazy.fetches == 1

    def test_methods_run_through_the_breaker(self, make_breaker):
        breaker = make_breaker(probe=Probe(failing=99), cooldown=60.0)
        sink = Sunk(CONNECT_TIMEOUT)

        class Repo:
            def get_git_ref(self, ref):
                return sink(ref)

        repo = FailFastRepository(Repo(), breaker)

        with pytest.raises(requests.exceptions.ConnectTimeout):
            repo.get_git_ref("heads/main")
        assert sink.calls == 1
        with pytest.raises(StoreUnreachable):
            repo.get_git_ref("heads/main")
        assert sink.calls == 1


class TestErrorClassification:
    def test_transport_failures_and_refusals_count_as_unreachable(self):
        assert is_unreachable_error(CONNECT_TIMEOUT)
        assert is_unreachable_error(StoreUnreachable("refused"))
        assert is_unreachable_error(ConnectionError("Network is unreachable"))
        assert not is_unreachable_error(GithubException(404, {"message": "x"}, None))
        assert not is_unreachable_error(ValueError("nope"))

    def test_the_deferred_file_ref_writes_treat_a_refusal_as_transient(self):
        # Otherwise a fail-fast refusal would give up on the file
        # reference after its first refused attempt and drop it from the
        # tree, while the breaker exists precisely so such a write waits
        # for the service to come back instead.
        assert _is_transient(StoreUnreachable("refused"))
        assert _is_transient(CONNECT_TIMEOUT)
        assert not _is_transient(GithubException(422, {"message": "bad"}, None))
