"""Tee mirroring: feed re-uploading mirrors from the upload stream itself.

A mirror that cannot take a server-side copy (a Discord store on either
side, another backend, ``mode: reupload``) has to receive every byte of
a version through this process. Today those bytes are read back from
the primary store after the upload, or from the local cache when one
is configured. With ``sync: tee`` an independent producer feeds the
client's stream into a bounded buffer per store. A mirror can finish the
same stream when the primary fails. Nothing is read back or staged to disk.

The pieces:

* :class:`TeeBranch` is one buffer: the producer ``feed``\\s chunks and
  waits while the branch holds ``capacity`` bytes; the consumer takes
  them off through ``stream``. ``finish`` marks the end of the bytes,
  ``detach`` drops the branch when its store stops so the producer is
  never held up by a consumer that is gone.
* :class:`TeeFileMessage` wraps the source and feeds every branch as a
  side effect of ``read``. The end of the
  stream is signalled by the byte count when the size is known, by the
  stream's own EOF when it is not, never by ``close``: the Telegram
  uploader closes the message after every part.
* :class:`TeeUpload` coordinates the producer and store tasks, then
  returns each store's outcome.

Encryption sits above the repository, so the branches carry exactly
what the primary receives: ciphertext when encryption is on.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Awaitable, Callable, Deque, Dict, List

from tgdcfs.errors import TechnicalError
from tgdcfs.reqres import (
    FileMessageFromStream,
    Replica,
    SentFileMessage,
    UploadableFileMessage,
)

logger = logging.getLogger(__name__)

# A source read occupies space before it joins any branch, and a consumer
# can take a whole chunk out of its branch to satisfy a smaller read. Keep
# those extra buffers small on live streams as well as bounding the branch.
LIVE_FEED_CHUNK_BYTES = 64 * 1024


class TeeBranch:
    """A bounded byte queue between the source and one store."""

    def __init__(self, key: str, capacity: int):
        self.key = key
        self._capacity = max(1, capacity)
        self._chunks: Deque[bytes] = deque()
        self._buffered = 0
        self._eof = False
        self._detached = False
        self._changed = asyncio.Condition()
        self.has_alternative = False

    @property
    def buffered(self) -> int:
        return self._buffered

    @property
    def detached(self) -> bool:
        return self._detached

    async def feed(self, data: bytes) -> None:
        """Queue ``data``; waits while the branch is full.

        A chunk larger than the whole capacity is admitted into an empty
        branch, so a producer with big reads never deadlocks.
        """
        if not data or self._detached:
            return
        async with self._changed:
            while (
                self._buffered
                and self._buffered + len(data) > self._capacity
                and not self._detached
            ):
                await self._changed.wait()
            if self._detached:
                return
            self._chunks.append(data)
            self._buffered += len(data)
            self._changed.notify_all()

    async def finish(self) -> None:
        """No more bytes will come; the consumer sees the end after the
        chunks still queued."""
        async with self._changed:
            self._eof = True
            self._changed.notify_all()

    async def detach(self) -> None:
        """Drop the branch: queued bytes are discarded, a waiting producer
        goes on, a waiting consumer gets an error."""
        async with self._changed:
            self._detached = True
            self._chunks.clear()
            self._buffered = 0
            self._changed.notify_all()

    async def stream(self) -> AsyncGenerator[bytes, None]:
        while True:
            async with self._changed:
                while not self._chunks and not self._eof and not self._detached:
                    await self._changed.wait()
                if self._detached:
                    raise TechnicalError(f"tee branch {self.key} was detached")
                if not self._chunks:
                    return
                data = self._chunks.popleft()
                self._buffered -= len(data)
                self._changed.notify_all()
            yield data


@dataclass
class TeeFileMessage(UploadableFileMessage):
    """The source upload message, feeding the branches as it is read."""

    inner: UploadableFileMessage = field(init=False)
    branches: List[TeeBranch] = field(init=False)
    _total: int = field(init=False)
    _fed: int = field(init=False, default=0)

    @classmethod
    def wrap(
        cls, inner: UploadableFileMessage, branches: List[TeeBranch]
    ) -> "TeeFileMessage":
        obj = cls(
            name=inner.name,
            size=inner.get_size(),
            caption=inner.caption,
            tags=inner.tags,
            _offset=0,
            _read_size=0,
            task_tracker=inner.task_tracker,
            version_id=inner.version_id,
        )
        obj.inner = inner
        obj.branches = list(branches)
        obj._total = inner.get_size()
        obj._fed = 0
        return obj

    @property
    def fed(self) -> int:
        return self._fed

    async def open(self) -> None:
        await self.inner.open()

    async def read(self, length: int) -> bytes:
        data = await self.inner.read(length)
        if data:
            self._fed += len(data)
            for branch in self.branches:
                await branch.feed(data)
        if not data or (self._total >= 0 and self._fed >= self._total):
            await self.finish()
        return data

    async def finish(self) -> None:
        for branch in self.branches:
            await branch.finish()

    async def close(self) -> None:
        # Not the end of the stream: the Telegram uploader closes the
        # message after every part and reads on for the next one.
        await self.inner.close()

    def file_name(self) -> str:
        return self.name or self.inner.file_name()

    def next_part(self, part_size: int) -> None:
        self.inner.next_part(part_size)
        self._offset += part_size


MirrorUpload = Callable[[TeeBranch], Awaitable[Replica]]
PrimaryUpload = Callable[[UploadableFileMessage], Awaitable[List[SentFileMessage]]]


@dataclass
class TeeResult:
    primary: List[SentFileMessage] | Exception | None
    mirrors: Dict[str, Replica | Exception]
    size: int | None


class TeeUpload:
    """One tee'd upload: the wrapped source and its store tasks."""

    def __init__(
        self, message: TeeFileMessage, uploads: Dict[str, MirrorUpload]
    ) -> None:
        self.message = message
        self._branches = {branch.key: branch for branch in message.branches}
        self._tasks: Dict[str, asyncio.Task[Replica]] = {
            key: asyncio.create_task(
                self._run(key, upload), name=f"tee-{key}-{message.file_name()}"
            )
            for key, upload in uploads.items()
        }

    async def _run(self, key: str, upload: MirrorUpload) -> Replica:
        branch = self._branches[key]
        try:
            return await upload(branch)
        finally:
            # Whatever happened, the producer must never wait for this
            # branch again.
            await branch.detach()

    @property
    def store_keys(self) -> List[str]:
        return list(self._tasks)

    async def run(
        self, primary_key: str, upload_primary: PrimaryUpload | None, capacity: int
    ) -> TeeResult:
        """Feed all stores independently from a single, forward-only source."""
        primary_branch = TeeBranch(primary_key, capacity)
        size = self.message.get_size()
        source_finished = False
        if upload_primary is not None:
            self.message.branches.insert(0, primary_branch)
        # The mirror tasks start on the next event-loop turn. Tell each
        # backend whether this upload has another potential data copy.
        for branch in self.message.branches:
            branch.has_alternative = len(self.message.branches) > 1

        async def primary() -> List[SentFileMessage] | None:
            if upload_primary is None:
                return None
            stream = primary_branch.stream()
            msg = FileMessageFromStream.new(stream, size, self.message.file_name())
            msg.caption = self.message.caption
            msg.tags = self.message.tags
            msg.task_tracker = self.message.task_tracker
            msg.version_id = self.message.version_id
            msg.failover = primary_branch.has_alternative
            try:
                return await upload_primary(msg)
            finally:
                await primary_branch.detach()
                await stream.aclose()

        async def produce() -> int:
            nonlocal source_finished
            await self.message.open()
            try:
                # This task alone reads and encrypts the source. Cancelling
                # a failed store's reader cannot drop an in-progress chunk
                # or close the client's one-shot iterator.
                while size < 0 or self.message.fed < size:
                    limit = min(
                        capacity,
                        LIVE_FEED_CHUNK_BYTES if size < 0 else 1024 * 1024,
                    )
                    if size >= 0:
                        limit = min(limit, size - self.message.fed)
                    if not await self.message.read(max(1, limit)):
                        break
                if size >= 0 and self.message.fed != size:
                    raise TechnicalError("Upload stream ended before its declared size")
                await self.message.finish()
                source_finished = True
                return self.message.fed
            finally:
                await self.message.close()

        producer = asyncio.create_task(produce())
        primary_task = asyncio.create_task(primary())
        consumers = asyncio.gather(
            primary_task, *self._tasks.values(), return_exceptions=True
        )
        in_flight: set[asyncio.Future[Any]] = {producer, consumers}
        try:
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            # If every store stopped, don't leave the producer blocked
            # waiting for the client to send more data.
            total = await producer if producer in done or source_finished else None
            await consumers
            if primary_task.cancelled():
                outcome: BaseException | None = TechnicalError(
                    "Primary upload cancelled"
                )
            else:
                outcome = primary_task.exception()
            if outcome is not None and not isinstance(outcome, Exception):
                raise outcome
            return TeeResult(
                outcome if outcome is not None else primary_task.result(),
                self._outcomes(),
                total,
            )
        finally:
            producer.cancel()
            consumers.cancel()
            await asyncio.gather(producer, consumers, return_exceptions=True)
            await self.abort()

    async def collect(self) -> Dict[str, Replica | Exception]:
        """Wait for every mirror; a failed one is returned as its error."""
        try:
            await self.message.finish()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        except asyncio.CancelledError:
            await self.abort()
            raise
        return self._outcomes()

    def _outcomes(self) -> Dict[str, Replica | Exception]:
        outcomes: Dict[str, Replica | Exception] = {}
        for key, task in self._tasks.items():
            if task.cancelled():
                outcomes[key] = TechnicalError(f"tee upload into {key} was cancelled")
            elif (error := task.exception()) is None:
                outcomes[key] = task.result()
            elif isinstance(error, Exception):
                outcomes[key] = error
            else:
                raise error
        return outcomes

    async def abort(self) -> None:
        """Stop the mirror branches and wait for their uploads to unwind."""
        for branch in self._branches.values():
            await branch.detach()
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
