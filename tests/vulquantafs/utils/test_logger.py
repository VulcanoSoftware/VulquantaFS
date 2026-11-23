"""Logging must never hold the event loop: the console handler's write
happens on a listener thread now, and a ``logger.*`` call only puts a
record on a queue.
"""

import logging
import logging.handlers
import time

import pytest

from vulquantafs.utils.logger import setup_logging


@pytest.fixture
def routed():
    setup_logging()
    yield


def test_setup_logging_installs_a_queue_handler(routed):
    handlers = logging.getLogger().handlers
    assert any(isinstance(h, logging.handlers.QueueHandler) for h in handlers)


def test_setup_logging_is_idempotent(routed):
    setup_logging()
    setup_logging()
    handlers = logging.getLogger().handlers
    assert sum(isinstance(h, logging.handlers.QueueHandler) for h in handlers) == 1


async def test_a_record_put_on_the_queue_does_not_block_the_caller(routed):
    started = time.monotonic()
    logging.getLogger("test.quiet").info("returns immediately")
    assert time.monotonic() - started < 0.5
