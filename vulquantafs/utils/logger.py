import logging
import logging.handlers
import queue
import threading

_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

_lock = threading.Lock()
_installed = False


def setup_logging(
    level: int = logging.INFO,
    format: str = _FORMAT,
    datefmt: str = _DATEFMT,
) -> None:
    """Route every log record through a background writer thread.

    A console handler writes where the process's stdout goes, and on a
    Docker Desktop (Windows/WSL2) volume that write can block for many
    seconds at a time. Every logger call runs on the event loop, so one
    slow write froze the whole server: every SFTP session, the HTTP API
    and the bot gateways at once, with nothing in the log over the stall.
    The queue put never blocks; the listener thread pays the write, and
    a stalled stream costs it buffer instead of the event loop.
    """
    global _installed
    with _lock:
        if _installed:
            return
        # Unbounded: a stalled stdout may hold records far longer than a
        # bounded buffer survives, and losing them is the one outcome
        # worse than waiting.
        records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format, datefmt))
        listener = logging.handlers.QueueListener(
            records, handler, respect_handler_level=False
        )
        listener.start()
        root = logging.getLogger()
        # ``basicConfig`` would silently do nothing once any library has
        # configured the root logger; the queue handler has to be there.
        root.setLevel(level)
        root.addHandler(logging.handlers.QueueHandler(records))
        _installed = True


setup_logging()
