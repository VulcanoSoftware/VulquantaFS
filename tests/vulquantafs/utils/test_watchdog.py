"""The event loop is the one thing every server shares: when a call
holds it, every session goes silent at once. The watchdog measures how
long that lasts and, while it lasts, has a sampler thread print the main
thread's stack -- the evidence the log itself cannot record.
"""

import asyncio
import time


from vulquantafs.utils import watchdog
from vulquantafs.utils.watchdog import EventLoopWatchdog


async def test_a_stalled_loop_is_reported_once_the_loop_runs_again(caplog):
    dog = EventLoopWatchdog(interval=0.05, warn_after=0.2, dump_after=60)
    dog.start()
    await asyncio.sleep(0.1)
    time.sleep(0.3)
    await asyncio.sleep(0.2)
    await dog.stop()

    warnings = [
        record for record in caplog.records if "did not run for" in record.getMessage()
    ]
    assert warnings


async def test_a_needed_look_reports_the_stalled_thread(caplog):
    dog = EventLoopWatchdog(interval=0.05, warn_after=0.2, dump_after=0.2)
    dog.start()
    await asyncio.sleep(0.1)
    time.sleep(0.4)
    await asyncio.sleep(0.3)
    await dog.stop()

    dumps = [
        record for record in caplog.records if "has not run for" in record.getMessage()
    ]
    assert dumps
    assert "test_watchdog.py" in dumps[0].getMessage()


async def test_a_quiet_loop_stays_silent(caplog):
    dog = EventLoopWatchdog(interval=0.05, warn_after=10, dump_after=60)
    dog.start()
    await asyncio.sleep(0.2)
    await dog.stop()

    assert not [
        record
        for record in caplog.records
        if "did not run for" in record.getMessage()
        or "has not run for" in record.getMessage()
    ]


def test_a_delayed_sampler_does_not_blame_the_recovery_stack(caplog, mocker):
    dog = EventLoopWatchdog(interval=1, dump_after=10)
    dog._last_tick = 0
    clock = mocker.Mock()
    clock.monotonic.side_effect = [0, 62, 62]
    mocker.patch.object(watchdog, "time", clock)
    mocker.patch.object(dog._sampler_stop, "wait", side_effect=[False, True])
    mocker.patch.object(watchdog, "_thread_stack", return_value="socket.recv_into")

    dog._sample()

    assert "sampler itself ran 61.0 seconds late" in caplog.text
    assert "may not identify the blocker" in caplog.text
    assert "socket.recv_into" in caplog.text
