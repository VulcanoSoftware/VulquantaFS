import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, List, Optional

from telethon.helpers import generate_random_long

from tgdcfs.config import get_config
from tgdcfs.errors import RpcTimeoutError, TechnicalError
from tgdcfs.reqres import (
    SaveBigFilePartReq,
    SaveFilePartReq,
    SaveFilePartResp,
    SendFileReq,
    SendMessageResp,
    UploadableFileMessage,
    UploadedFile,
)
from tgdcfs.utils.others import flood_wait_seconds, is_big_file

from .interface import ITDLibClient, bounded_part

logger = logging.getLogger(__name__)

# A part that keeps failing has to give up eventually: the old unbounded,
# sleepless retry turned one broken part into a busy loop against Telegram,
# which is how a slow upload becomes a rate-limited one.
MAX_PART_ATTEMPTS = 5
RETRY_BASE_DELAY = 0.5
RETRY_MAX_DELAY = 30.0


@dataclass
class WorkersConfig:
    small: int = 3
    big: int = 8

    @classmethod
    def from_config(cls) -> "WorkersConfig":
        transfer = get_config().tgdcfs.transfer
        return cls(small=transfer.upload_workers_small, big=transfer.upload_workers_big)


@dataclass
class FileChunk:
    content: bytes
    file_part: int


class FileUploader:
    def __init__(
        self,
        client: ITDLibClient,
        file_msg: UploadableFileMessage,
        workers: Optional[WorkersConfig] = None,
    ):
        self.client = client
        self._file_msg = file_msg
        self._file_size = self._file_msg.get_size()
        self._file_name = self._file_msg.file_name()

        # One part size for every file. The library's heuristic drops to
        # 128 KiB below 100 MB, which quadruples the number of requests for
        # no gain -- 512 KiB is accepted for any size.
        self._chunk_size = get_config().tgdcfs.transfer.upload_part_size_bytes

        # A negative size means a live stream of unknown length; the total
        # only becomes known when the source runs dry. Those uploads follow
        # the streamed upload protocol (see ``_upload_streamed``).
        self._streamed = self._file_size < 0
        if self._streamed:
            self._total_parts = 0
        else:
            self._total_parts = (
                self._file_size + self._chunk_size - 1
            ) // self._chunk_size

        self._part_indexes: asyncio.Queue[int] = asyncio.Queue(
            maxsize=max(self._total_parts, 1)
        )

        self._workers = workers or WorkersConfig.from_config()

        self._file_id = generate_random_long()

        # Streamed uploads always go through saveBigFilePart, even when the
        # stream turns out to be smaller than 10 MB (core.telegram.org/api/files).
        self._is_big = self._streamed or is_big_file(self._file_size)
        self._read_size = 0
        self._uploaded_size = 0
        # When the last part came back, ``None`` while none has; see
        # :meth:`_store_alive`.
        self._last_part_answered: Optional[float] = None

        self._num_workers = self._workers.big if self._is_big else self._workers.small
        self._lock = asyncio.Lock()

    async def _close(self) -> None:
        await self._file_msg.close()

    async def _read(self, length: int) -> bytes:
        return await self._file_msg.read(length)

    def _store_alive(self) -> bool:
        """Whether the store answered a part, and recently enough to matter.

        A part bound that expires is evidence of a dead store only when no
        other part came back either. The workers share one connection, and
        at this level a single dropped response is indistinguishable from a
        service that went away; failing the whole upload for it throws away
        a transfer the client cannot resume, while the parts that did come
        back prove the store is still there. The part bound is the window:
        anything answered within the last one keeps the retry budget in
        play, and a store that answered nothing at all -- or nothing for a
        whole bound -- still ends the upload immediately.
        """
        return self._silence_remaining() > 0

    def _silence_remaining(self) -> float:
        bound = get_config().tgdcfs.transfer.upload_part_timeout_s
        if bound <= 0 or self._last_part_answered is None:
            return 0.0
        return self._last_part_answered + bound - time.monotonic()

    async def _retry_while_answering(
        self, what: str, call: Awaitable[SaveFilePartResp]
    ) -> SaveFilePartResp:
        """A timed-out part may retry only while other parts make progress.

        Its next RPC can start before the store-wide silence bound expires.
        Don't let that restart the clock: the first gap with no answers
        for a whole bound ends the upload, even during a retry.
        """
        bound = get_config().tgdcfs.transfer.upload_part_timeout_s
        rpc = asyncio.ensure_future(call)
        try:
            while (remaining := self._silence_remaining()) > 0:
                done, _ = await asyncio.wait({rpc}, timeout=remaining)
                if done:
                    return rpc.result()
                # Another worker may have extended the deadline.
            raise RpcTimeoutError(
                f"Telegram did not answer {what} within {bound}s of its last response"
            )
        finally:
            rpc.cancel()
            await asyncio.gather(rpc, return_exceptions=True)

    async def _upload_chunk(
        self, chunk: FileChunk, file_total_parts: Optional[int] = None
    ) -> None:
        what = f"part {chunk.file_part} of {self._file_name}"
        retrying_timeout = False
        for attempt in range(1, MAX_PART_ATTEMPTS + 1):
            if self._is_big:
                call = bounded_part(
                    what,
                    self.client.save_big_file_part(
                        SaveBigFilePartReq(
                            file_id=self._file_id,
                            bytes=chunk.content,
                            file_part=chunk.file_part,
                            file_total_parts=(
                                self._total_parts
                                if file_total_parts is None
                                else file_total_parts
                            ),
                        )
                    ),
                )
            else:
                call = bounded_part(
                    what,
                    self.client.save_file_part(
                        SaveFilePartReq(
                            file_id=self._file_id,
                            bytes=chunk.content,
                            file_part=chunk.file_part,
                        )
                    ),
                )
            try:
                rsp = (
                    await self._retry_while_answering(what, call)
                    if retrying_timeout
                    else await call
                )

                if not rsp.success:
                    raise TechnicalError(f"Unexpected response: {rsp}")

                self._last_part_answered = time.monotonic()
                self._uploaded_size += len(chunk.content)
                return

            except Exception as e:
                retrying_timeout |= isinstance(e, RpcTimeoutError)
                if retrying_timeout and not self._store_alive():
                    # The store stayed silent for the whole part bound and
                    # nothing came back to say otherwise: it is
                    # unresponsive, not flaky. Retrying against the same
                    # dead connection burns the SFTP client's patience --
                    # its write acknowledgements are frozen while every
                    # worker is stuck -- before the write can fail over to a
                    # mirror store, so let the failure travel now. A store
                    # that is still answering other parts is covered by the
                    # retry budget below instead.
                    raise

                if attempt == MAX_PART_ATTEMPTS:
                    logger.error(
                        f"Giving up on part {chunk.file_part} for {self._file_name} "
                        f"after {attempt} attempts: {e}"
                    )
                    raise

                if (wait := flood_wait_seconds(e)) is not None:
                    delay = float(wait)
                else:
                    delay = min(RETRY_BASE_DELAY * 2 ** (attempt - 1), RETRY_MAX_DELAY)

                logger.warning(
                    f"Error uploading part {chunk.file_part} for {self._file_name}: "
                    f"{e}, attempt={attempt}/{MAX_PART_ATTEMPTS}, "
                    f"retrying in {delay:.1f}s"
                )
                while delay > 0:
                    pause = delay
                    if retrying_timeout:
                        pause = min(delay, self._silence_remaining())
                        if pause <= 0:
                            raise
                    await asyncio.sleep(pause)
                    delay -= pause

    def _done_reading(self) -> bool:
        return self._read_size >= self._file_size

    async def _upload_next_part(self, part: int) -> int:
        async with self._lock:
            if self._done_reading():
                return 0
            size_to_read = min(self._file_size - self._read_size, self._chunk_size)
            content = await self._read(size_to_read)
            self._read_size += size_to_read

        await self._upload_chunk(FileChunk(content=content, file_part=part))
        return size_to_read

    async def _cancelled(self) -> bool:
        if tt := self._file_msg.task_tracker:
            return await tt.cancelled()
        return False

    async def upload(self) -> int:
        await self._file_msg.open()

        if self._streamed:
            size = await self._upload_streamed()
            await self._close()
            return size

        for i in range(self._total_parts):
            await self._part_indexes.put(i)

        async def create_worker(worker_id: int) -> bool:
            while True:
                if await self._cancelled():
                    logger.warning(
                        f"Task uploading for {self._file_name} was cancelled. Worker {worker_id} exiting."
                    )
                    return False

                try:
                    part_size = await self._upload_next_part(
                        self._part_indexes.get_nowait()
                    )
                except asyncio.QueueEmpty:
                    logger.debug(
                        f"[Worker {worker_id}] No more parts to upload for {self._file_name}. Worker exiting."
                    )
                    return True
                logger.debug(
                    f"[Worker {worker_id}] {self._uploaded_size * 100 / self._file_size}% uploaded. file_id={self._file_id} file_name={self._file_name}"
                )
                if (tt := self._file_msg.task_tracker) and part_size > 0:
                    await tt.update_progress(size_delta=part_size)

        workers = [
            asyncio.create_task(create_worker(worker_id))
            for worker_id in range(self._num_workers)
        ]
        try:
            await asyncio.gather(*workers)
        except Exception:
            # One dead part makes the whole file_id unusable, so the other
            # workers should stop feeding it instead of uploading into a
            # file that can never be sent.
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        await self._close()
        return self._file_size

    async def _upload_streamed(self) -> int:
        """Upload a stream whose length is unknown until it ends.

        This follows the streamed upload protocol from the official API
        documentation (core.telegram.org/api/files, "Streamed uploads"):
        buffer ``part_size`` bytes -- or less when the stream ends -- and
        upload each part immediately, always through
        ``upload.saveBigFilePart``. Every part but the last is saved with
        ``file_total_parts = -1``; the last one carries
        ``file_total_parts = ceil(total_stream_size / part_size)`` and is
        saved only after every other part is in, so the server can
        finalize the file.

        One part is always held back until the next read proves that more
        data follows, so the stream ending exactly on a part boundary
        makes the held part the last one instead of sending an empty
        part: FILE_PART_EMPTY is on Telegram's error list, and the docs
        do not promise that an empty part finalizes a file. Only a
        completely empty stream saves nothing, exactly like a known-size
        empty file.
        """
        queue: asyncio.Queue[Optional[FileChunk]] = asyncio.Queue(
            maxsize=self._num_workers
        )
        worker_failed = asyncio.Event()
        # What took the first worker down. It is the upload's real failure,
        # so it is what the caller has to see -- a bail-out message of our
        # own would hide the part bound or the store's error from the log
        # and from the write's failover path.
        worker_failure: List[BaseException] = []

        async def worker() -> None:
            try:
                while True:
                    if await self._cancelled():
                        logger.warning(
                            f"Task uploading for {self._file_name} was cancelled. "
                            f"Streamed worker exiting."
                        )
                        return
                    chunk = await queue.get()
                    if chunk is None:
                        return
                    await self._upload_chunk(chunk, file_total_parts=-1)
                    if tt := self._file_msg.task_tracker:
                        await tt.update_progress(size_delta=len(chunk.content))
            except Exception as ex:
                if not worker_failure:
                    worker_failure.append(ex)
                worker_failed.set()
                raise

        workers = [
            asyncio.create_task(worker(), name=f"streamed-upload-{self._file_name}")
            for _ in range(self._num_workers)
        ]

        async def feed(chunk: Optional[FileChunk]) -> None:
            # A full queue waits for a worker to take from it; workers that
            # all stopped (cancelled or failed) never will, so bail out
            # instead of blocking on the queue forever.
            while True:
                if worker_failure:
                    raise worker_failure[0]
                if all(w.done() for w in workers):
                    if chunk is None:
                        return
                    raise TechnicalError(
                        f"Upload of {self._file_name} stopped before the "
                        f"stream was consumed"
                    )
                try:
                    queue.put_nowait(chunk)
                    return
                except asyncio.QueueFull:
                    await asyncio.sleep(0.05)

        total = 0
        index = 0
        held: Optional[bytes] = None
        tail: Optional[bytes] = None
        reader: Optional[asyncio.Task] = None
        # A worker that dies while this loop waits in ``_read`` must end
        # the upload, not sit unretrieved until the source produces
        # something again: the SFTP client's writes stay unacknowledged,
        # so nothing here proves the failure on its own. The next read
        # races the worker-failure event, and the failing side decides.
        failure_watcher = asyncio.create_task(worker_failed.wait())
        try:
            while True:
                if await self._cancelled():
                    # Nothing more goes to the server after a cancel, not
                    # even the part held back for the boundary check.
                    held = None
                    break
                reader = asyncio.create_task(self._read(self._chunk_size))
                done, _ = await asyncio.wait(
                    {reader, failure_watcher},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if failure_watcher in done:
                    raise worker_failure[0]
                content = reader.result()
                if not content:
                    break
                total += len(content)
                if held is not None:
                    # The read that just came back proves the held part
                    # has a successor, so it can go to the workers now.
                    await feed(FileChunk(content=held, file_part=index))
                    index += 1
                    held = None
                if len(content) < self._chunk_size:
                    # The stream ran dry mid-part: this short part is the
                    # last one and carries the real total.
                    tail = content
                    break
                held = content
            if not await self._cancelled():
                for _ in workers:
                    await feed(None)
                await asyncio.gather(*workers)
            if await self._cancelled():
                held = tail = None
        finally:
            # An aborted uploader must never wait for space in a dead
            # worker's queue, and must reap every worker exception.
            children = [failure_watcher, *workers]
            if reader is not None:
                children.append(reader)
            for child in children:
                if not child.done() and not child.cancelling():
                    child.cancel()
            await asyncio.gather(*children, return_exceptions=True)

        last = tail if tail is not None else held
        if last is not None:
            await self._upload_chunk(
                FileChunk(content=last, file_part=index), file_total_parts=index + 1
            )
            if tt := self._file_msg.task_tracker:
                await tt.update_progress(size_delta=len(last))
            index += 1

        self._file_size = total
        self._total_parts = index
        # Nothing was saved for an empty stream, so the send must use the
        # small-file constructor, exactly as a known-size empty file does.
        self._is_big = total > 0
        return total

    def get_uploaded_file(self) -> UploadedFile:
        return UploadedFile(
            id=self._file_id,
            parts=self._total_parts,
            name=self._file_name,
        )

    async def send(self, chat_id: int, caption: str = "") -> SendMessageResp:
        logger.debug(
            f"Sending file {self._file_name} ({self._file_id}) to chat {chat_id}"
        )

        req = SendFileReq(
            chat=chat_id,
            file=self.get_uploaded_file(),
            name=self._file_name,
            caption=caption,
        )

        if self._is_big:
            return await self.client.send_big_file(req)
        return await self.client.send_small_file(req)
