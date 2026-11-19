import asyncio
import os
import tempfile
import time
from typing import AsyncIterator
from unittest.mock import Mock

import pytest

from tgdcfs.backends.telegram.interface import ITDLibClient
from tgdcfs.backends.telegram.uploader import (
    MAX_PART_ATTEMPTS,
    FileChunk,
    FileUploader,
    WorkersConfig,
)
from tgdcfs.config import TransferConfig
from tgdcfs.errors import RpcTimeoutError, TechnicalError
from tgdcfs.reqres import (
    FileMessageFromBuffer,
    FileMessageFromPath,
    FileMessageFromStream,
    SaveFilePartResp,
    SendFileReq,
    SendMessageResp,
    UploadedFile,
)
from tgdcfs.tasks.integrations import TaskTracker


@pytest.fixture
def no_backoff(mocker):
    """Skip the retry backoff so the retry tests do not sit in real sleeps."""
    return mocker.patch(
        "tgdcfs.backends.telegram.uploader.asyncio.sleep",
        new=mocker.AsyncMock(),
    )


class TestWorkersConfig:
    def test_default_values(self):
        config = WorkersConfig()
        assert config.small == 3
        assert config.big == 8

    def test_custom_values(self):
        config = WorkersConfig(small=2, big=8)
        assert config.small == 2
        assert config.big == 8


class TestFileChunk:
    def test_creation(self):
        content = b"test data"
        file_part = 5
        chunk = FileChunk(content=content, file_part=file_part)

        assert chunk.content == content
        assert chunk.file_part == file_part


class TestUploaderFromPath:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.send_small_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=123)
        )
        client.send_big_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=124)
        )
        return client

    @pytest.fixture
    def test_file(self):
        """Create a temporary test file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_content = b"Hello, World!" * 100  # 1300 bytes
            f.write(test_content)
            f.flush()
            yield f.name, test_content
        os.unlink(f.name)

    @pytest.fixture
    def mock_task_tracker(self, mocker):
        tracker = mocker.AsyncMock(spec=TaskTracker)
        tracker.cancelled = mocker.AsyncMock(return_value=False)
        tracker.update_progress = mocker.AsyncMock()
        return tracker

    @pytest.mark.asyncio
    async def test_small_file_upload(self, mock_client, test_file, mock_task_tracker):
        file_path, expected_content = test_file
        file_size = len(expected_content)

        file_msg = FileMessageFromPath.new(path=file_path, name="test.txt")
        file_msg.task_tracker = mock_task_tracker

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == file_size
        assert uploader.get_uploaded_file().name == "test.txt"
        mock_client.save_file_part.assert_called()
        mock_task_tracker.update_progress.assert_called()

    @pytest.mark.asyncio
    async def test_big_file_upload(self, mock_client, mock_task_tracker):
        """Test big file upload (>10MB)"""
        big_content = b"x" * (11 * 1024 * 1024)  # 11MB

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(big_content)
            f.flush()

            try:
                file_msg = FileMessageFromPath.new(path=f.name, name="big_file.bin")
                file_msg.task_tracker = mock_task_tracker

                uploader = FileUploader(client=mock_client, file_msg=file_msg)

                uploaded_size = await uploader.upload()

                assert uploaded_size == len(big_content)
                mock_client.save_big_file_part.assert_called()
            finally:
                os.unlink(f.name)

    @pytest.mark.asyncio
    async def test_upload_with_offset(self, mock_client, test_file):
        file_path, original_content = test_file
        offset = 5
        expected_content = original_content[offset:]

        file_msg = FileMessageFromPath.new(path=file_path, name="test_offset.txt")
        file_msg._offset = offset
        file_msg.size = len(expected_content)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_content)

    @pytest.mark.asyncio
    async def test_default_file_name(self, mock_client, test_file):
        file_path, _ = test_file

        file_msg = FileMessageFromPath.new(path=file_path)
        file_msg.name = ""  # Clear the name to test default

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        expected_name = os.path.basename(file_path)
        assert uploader.get_uploaded_file().name == expected_name

    @pytest.mark.asyncio
    async def test_upload_failure_and_retry(self, mock_client, test_file):
        file_path, expected_content = test_file

        # First call fails, second succeeds
        mock_client.save_file_part.side_effect = [
            Exception("Network error"),
            SaveFilePartResp(success=True),
        ]

        file_msg = FileMessageFromPath.new(path=file_path, name="retry_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_content)
        assert mock_client.save_file_part.call_count >= 1

    @pytest.mark.asyncio
    async def test_task_cancellation(
        self, mock_client, test_file, mock_task_tracker, mocker
    ):
        file_path, expected_content = test_file

        # Simulate cancellation
        mock_task_tracker.cancelled = mocker.AsyncMock(return_value=True)

        file_msg = FileMessageFromPath.new(path=file_path, name="cancelled.txt")
        file_msg.task_tracker = mock_task_tracker

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        # Note: Current implementation doesn't raise TaskCancelled, it just returns early
        uploaded_size = await uploader.upload()
        assert uploaded_size == len(
            expected_content
        )  # Should still complete the file size calculation

    @pytest.mark.asyncio
    async def test_send_file(self, mock_client, test_file):
        file_path, expected_content = test_file
        chat_id = 123
        caption = "Test caption"

        file_msg = FileMessageFromPath.new(path=file_path, name="send_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()
        response = await uploader.send(chat_id, caption)

        assert isinstance(response, SendMessageResp)
        mock_client.send_small_file.assert_called_once()


class TestUploaderFromBuffer:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @pytest.mark.asyncio
    async def test_buffer_upload(self, mock_client):
        test_data = b"Buffer test data" * 50  # 800 bytes

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="buffer_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)
        assert uploader.get_uploaded_file().name == "buffer_test.txt"
        mock_client.save_file_part.assert_called()

    @pytest.mark.asyncio
    async def test_buffer_with_offset(self, mock_client):
        test_data = b"0123456789" * 10  # 100 bytes
        offset = 10
        expected_data = test_data[offset:]

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="offset_buffer.txt")
        file_msg._offset = offset
        file_msg.size = len(expected_data)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(expected_data)

    @pytest.mark.asyncio
    async def test_default_buffer_name(self, mock_client):
        test_data = b"unnamed buffer data"

        file_msg = FileMessageFromBuffer.new(buffer=test_data)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        assert uploader.get_uploaded_file().name == "unnamed"


class TestUploaderFromStream:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @staticmethod
    async def create_test_stream(chunks: list[bytes]) -> AsyncIterator[bytes]:
        """Create a test async stream from chunks"""
        for chunk in chunks:
            yield chunk

    @pytest.mark.asyncio
    async def test_stream_upload(self, mock_client):
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(
            stream=stream, size=total_size, name="stream_test.txt"
        )

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == total_size
        assert uploader.get_uploaded_file().name == "stream_test.txt"
        mock_client.save_file_part.assert_called()

    @pytest.mark.asyncio
    async def test_stream_default_name(self, mock_client):
        chunks = [b"data"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(stream=stream, size=total_size)

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        assert uploader.get_uploaded_file().name == "unnamed"

    @pytest.mark.asyncio
    async def test_stream_chunked_reading(self, mock_client):
        """Test that stream reading handles chunks correctly"""
        # Create chunks that don't align with read boundaries
        chunks = [b"abc", b"defghij", b"klm"]
        total_size = sum(len(chunk) for chunk in chunks)

        stream = self.create_test_stream(chunks)
        file_msg = FileMessageFromStream.new(
            stream=stream, size=total_size, name="chunked.txt"
        )

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == total_size


class TestErrorHandling:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        return client

    @pytest.mark.asyncio
    async def test_upload_response_failure(self, mock_client, mocker, no_backoff):
        """A part that fails within the retry budget still goes through."""
        failure_count = 0

        def side_effect(*_args, **_kwargs):
            nonlocal failure_count
            if failure_count < MAX_PART_ATTEMPTS - 1:
                failure_count += 1
                return SaveFilePartResp(success=False)
            return SaveFilePartResp(success=True)

        mock_client.save_file_part = mocker.AsyncMock(side_effect=side_effect)

        test_data = b"test data"
        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="fail_test.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()
        assert uploaded_size == len(test_data)
        assert mock_client.save_file_part.call_count == MAX_PART_ATTEMPTS

    @pytest.mark.asyncio
    async def test_upload_gives_up_after_the_retry_budget(
        self, mock_client, mocker, no_backoff
    ):
        """A part that never succeeds fails the upload instead of looping."""
        mock_client.save_file_part = mocker.AsyncMock(
            side_effect=Exception("Network error")
        )

        file_msg = FileMessageFromBuffer.new(buffer=b"test data", name="doomed.txt")
        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        with pytest.raises(Exception, match="Network error"):
            await uploader.upload()

        assert mock_client.save_file_part.call_count == MAX_PART_ATTEMPTS

    @pytest.mark.asyncio
    async def test_upload_waits_as_long_as_telegram_asks(self, mock_client, mocker):
        """A flood error is retried after the delay Telegram named."""

        class FloodWaitError(Exception):
            seconds = 17

        sleep = mocker.patch(
            "tgdcfs.backends.telegram.uploader.asyncio.sleep",
            new=mocker.AsyncMock(),
        )
        mock_client.save_file_part = mocker.AsyncMock(
            side_effect=[FloodWaitError(), SaveFilePartResp(success=True)]
        )

        file_msg = FileMessageFromBuffer.new(buffer=b"test data", name="flood.txt")
        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        await uploader.upload()

        # The patch replaces asyncio.sleep for the whole process, so any other
        # sleep that happens to run meanwhile (a polling loop of a task another
        # test left behind) is recorded too. Only the flood wait is asserted.
        flood_waits = [c for c in sleep.await_args_list if c == mocker.call(17.0)]
        assert len(flood_waits) == 1


class TestConcurrencyAndWorkers:
    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        return client

    @pytest.mark.asyncio
    async def test_custom_workers_config(self, mock_client):
        """Test upload with custom worker configuration"""
        test_data = b"concurrent test data" * 10
        workers_config = WorkersConfig(small=2, big=4)

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="concurrent.txt")

        uploader = FileUploader(
            client=mock_client,
            file_msg=file_msg,
            workers=workers_config,
        )

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)

    @pytest.mark.asyncio
    async def test_concurrent_uploads(self, mock_client):
        """Test that multiple parts can be uploaded concurrently"""
        # Comfortably more than one 512 KiB part
        test_data = b"x" * (1024 * 1024 * 2)  # 2 MiB

        file_msg = FileMessageFromBuffer.new(buffer=test_data, name="large.txt")

        uploader = FileUploader(client=mock_client, file_msg=file_msg)

        uploaded_size = await uploader.upload()

        assert uploaded_size == len(test_data)
        # Should have made multiple calls due to chunking
        assert mock_client.save_file_part.call_count > 1


class TestStreamedUpload:
    """Uploads of unknown length, per core.telegram.org/api/files.

    The protocol: buffer part_size bytes -- or less when the stream ends --
    and upload immediately, always through upload.saveBigFilePart, with
    ``file_total_parts = -1`` everywhere except the last part, which
    carries ``ceil(total/part_size)`` and always holds data: one part is
    kept back until the next read proves a successor, so a stream ending
    exactly on a part boundary never sends an empty part.
    """

    PART = 512 * 1024

    @pytest.fixture
    def mock_client(self, mocker):
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )
        client.send_small_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=1)
        )
        client.send_big_file = mocker.AsyncMock(
            return_value=SendMessageResp(message_id=2)
        )
        return client

    @staticmethod
    async def stream_of(chunks: list[bytes]) -> AsyncIterator[bytes]:
        for chunk in chunks:
            yield chunk

    def reqs(self, mock_client):
        return [call.args[0] for call in mock_client.save_big_file_part.call_args_list]

    def totals(self, mock_client):
        return [req.file_total_parts for req in self.reqs(mock_client)]

    @pytest.mark.asyncio
    async def test_small_stream_still_uses_big_file_parts(self, mock_client):
        content = b"small stream"

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content]), size=-1, name="small.bin"
            ),
        )
        assert await uploader.upload() == len(content)

        assert mock_client.save_file_part.await_count == 0
        assert self.totals(mock_client) == [1]
        assert uploader.get_uploaded_file().parts == 1

    @pytest.mark.asyncio
    async def test_parts_stream_out_with_minus_one_total(self, mock_client):
        content = b"x" * (2 * self.PART + 100)

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content[: self.PART], content[self.PART :]]),
                size=-1,
            ),
        )
        assert await uploader.upload() == len(content)

        assert self.totals(mock_client) == [-1, -1, 3]
        assert [len(req.bytes) for req in self.reqs(mock_client)] == [
            self.PART,
            self.PART,
            100,
        ]
        assert uploader.get_uploaded_file().parts == 3

    @pytest.mark.asyncio
    async def test_a_stream_ending_on_a_part_boundary_keeps_the_last_part_full(
        self, mock_client
    ):
        content = b"x" * (2 * self.PART)

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content]), size=-1
            ),
        )
        assert await uploader.upload() == len(content)

        assert self.totals(mock_client) == [-1, 2]
        assert [len(req.bytes) for req in self.reqs(mock_client)] == [
            self.PART,
            self.PART,
        ]
        last = self.reqs(mock_client)[-1]
        assert last.bytes == content[self.PART :]
        assert last.file_part == 1
        assert uploader.get_uploaded_file().parts == 2

    @pytest.mark.asyncio
    async def test_one_byte_over_a_part_boundary_gets_a_one_byte_last_part(
        self, mock_client
    ):
        content = b"x" * (2 * self.PART + 1)

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content]), size=-1
            ),
        )
        assert await uploader.upload() == len(content)

        assert self.totals(mock_client) == [-1, -1, 3]
        assert [len(req.bytes) for req in self.reqs(mock_client)] == [
            self.PART,
            self.PART,
            1,
        ]
        assert uploader.get_uploaded_file().parts == 3

    @pytest.mark.asyncio
    async def test_empty_stream_uploads_nothing(self, mock_client):
        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(stream=self.stream_of([]), size=-1),
        )
        assert await uploader.upload() == 0

        assert mock_client.save_big_file_part.await_count == 0
        assert mock_client.save_file_part.await_count == 0

    @pytest.mark.asyncio
    async def test_send_uses_the_big_file_constructor(self, mock_client):
        content = b"x" * (self.PART + 1)

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content]), size=-1
            ),
        )
        await uploader.upload()
        await uploader.send(12345)

        mock_client.send_big_file.assert_awaited_once_with(
            SendFileReq(
                chat=12345,
                file=UploadedFile(
                    id=uploader.get_uploaded_file().id, parts=2, name="unnamed"
                ),
                name="unnamed",
                caption="",
            )
        )
        mock_client.send_small_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failing_part_aborts_the_upload(
        self, mock_client, no_backoff, mocker
    ):
        content = b"x" * (2 * self.PART)
        mock_client.save_big_file_part = mocker.AsyncMock(
            side_effect=TechnicalError("part refused")
        )

        uploader = FileUploader(
            client=mock_client,
            file_msg=FileMessageFromStream.new(
                stream=self.stream_of([content]), size=-1
            ),
        )

        with pytest.raises(TechnicalError):
            await uploader.upload()


class TestPartTimeout:
    """A silent part upload must not burn the SFTP client's patience.

    The part uploads pace the SFTP write acknowledgements: while one
    hangs, nothing is answered, and a client that sees no activity for
    its inactivity timeout (FileZilla: 20 s) drops the session and the
    transfer looks stalled. The part bound turns the silence into an
    error long before that, and without the retry budget of a flaky
    network: a store that never answered at all is not going to start
    answering mid-retry, so a timeout travels straight to the write's
    failover instead.
    """

    PART = 512 * 1024

    @staticmethod
    def _config(mocker, **transfer):
        cfg = Mock()
        cfg.tgdcfs.transfer = TransferConfig.from_dict(transfer)
        mocker.patch("tgdcfs.backends.telegram.uploader.get_config", return_value=cfg)
        mocker.patch("tgdcfs.backends.telegram.interface.get_config", return_value=cfg)

    @staticmethod
    def _stream(size: int):
        async def stream():
            left = size
            while left > 0:
                chunk = b"x" * min(left, TestPartTimeout.PART)
                left -= len(chunk)
                yield chunk

        return stream()

    @staticmethod
    def _hanging(mocker):
        async def hang(req):
            await asyncio.sleep(60)

        return mocker.AsyncMock(side_effect=hang)

    @pytest.mark.asyncio
    async def test_a_silent_part_upload_raises_within_the_part_bound(self, mocker):
        self._config(mocker, upload_part_timeout_s=1)
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = self._hanging(mocker)

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=self._stream(2 * self.PART), size=-1
            ),
        )

        start = time.monotonic()
        with pytest.raises(RpcTimeoutError):
            await uploader.upload()
        assert time.monotonic() - start < 5
        # Eight workers, four parts: every part was tried exactly once,
        # none of them was re-fed to the dead connection five times over.
        assert client.save_big_file_part.await_count <= 4

    @pytest.mark.asyncio
    async def test_a_part_nobody_answered_is_retried_while_others_come_back(
        self, mocker
    ):
        """One dropped response is not a dead store.

        The workers share one connection, so a part whose response never
        arrives looks exactly like a store that went away -- but the parts
        that keep coming back say it did not. Failing the upload there
        discards a transfer the client cannot resume, one dropped response
        into a 1 GB file; the retry budget is what that case is for. Only a
        store that answers nothing at all gives up inside the bound.
        """
        self._config(mocker, upload_part_timeout_s=1, upload_workers_big=2)
        client = mocker.AsyncMock(spec=ITDLibClient)
        saved = []
        hung = []

        async def save_big_file_part(req):
            if req.file_part == 2 and not hung:
                hung.append(req.file_part)
                await asyncio.sleep(60)  # the response that never comes
            await asyncio.sleep(0.05)  # slow enough to outlast the bound
            saved.append(req.file_part)
            return SaveFilePartResp(success=True)

        client.save_big_file_part = save_big_file_part
        parts = 64

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=self._stream(parts * self.PART), size=-1
            ),
        )

        assert (
            await asyncio.wait_for(uploader.upload(), timeout=20) == parts * self.PART
        )
        # The unanswered part was sent again and landed once, and nothing
        # else was disturbed by its retry.
        assert hung == [2]
        assert sorted(saved) == list(range(parts))
        assert uploader.get_uploaded_file().parts == parts

    @pytest.mark.asyncio
    async def test_the_retry_budget_survives_for_errors_that_can_recover(
        self, no_backoff, mocker
    ):
        self._config(mocker, upload_part_timeout_s=1)

        class TelegramFloodWaitError(Exception):
            seconds = 30

        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = mocker.AsyncMock(
            side_effect=TelegramFloodWaitError("flood")
        )

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(stream=self._stream(self.PART), size=-1),
        )

        with pytest.raises(TelegramFloodWaitError):
            await uploader.upload()
        assert client.save_big_file_part.await_count == MAX_PART_ATTEMPTS

    @pytest.mark.asyncio
    async def test_a_zero_part_bound_leaves_the_call_unbounded(self, mocker):
        self._config(mocker, upload_part_timeout_s=0)
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = self._hanging(mocker)

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(stream=self._stream(self.PART), size=-1),
        )

        task = asyncio.create_task(uploader.upload())
        await asyncio.sleep(0.3)
        assert not task.done()
        assert client.save_big_file_part.await_count >= 1
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestStreamedUploadFailurePropagation:
    """A dead worker must end a streamed upload the client is still feeding.

    The part bound kills every worker about fifteen seconds after the
    store went silent, but the main loop is not looking at them: it sits
    in ``_read`` for the next chunk, and a stopped client sends nothing
    anymore. Its reads therefore never return, the workers' exceptions
    stay unretrieved, and ``save()``'s failover path -- aborting the tee,
    marking the primary dead -- is never reached, so the retries pool up
    behind the stuck upload. The wait in ``_read`` must break on the
    worker failure and surface it as the upload's error.
    """

    PART = 512 * 1024

    @staticmethod
    def _config(mocker, **transfer):
        cfg = Mock()
        cfg.tgdcfs.transfer = TransferConfig.from_dict(transfer)
        mocker.patch("tgdcfs.backends.telegram.uploader.get_config", return_value=cfg)
        mocker.patch("tgdcfs.backends.telegram.interface.get_config", return_value=cfg)

    @staticmethod
    def _hanging(mocker):
        async def hang(req):
            await asyncio.sleep(60)

        return mocker.AsyncMock(side_effect=hang)

    @staticmethod
    def _stream(size: int):
        async def stream():
            left = size
            while left > 0:
                chunk = b"x" * min(left, TestStreamedUploadFailurePropagation.PART)
                left -= len(chunk)
                yield chunk

        return stream()

    @pytest.mark.asyncio
    async def test_a_worker_timeout_ends_a_stream_that_falls_silent(self, mocker):
        self._config(mocker, upload_part_timeout_s=1)
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = self._hanging(mocker)

        async def silent_after_two_chunks():
            for _ in range(2):
                yield b"x" * self.PART
            await asyncio.sleep(60)

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=silent_after_two_chunks(), size=-1
            ),
        )

        start = time.monotonic()
        with pytest.raises(RpcTimeoutError):
            await asyncio.wait_for(uploader.upload(), timeout=10)
        assert time.monotonic() - start < 5

    @pytest.mark.asyncio
    async def test_an_ended_stream_still_raises_the_worker_error(self, mocker):
        self._config(mocker, upload_part_timeout_s=1)
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = self._hanging(mocker)

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=self._stream(2 * self.PART), size=-1
            ),
        )

        start = time.monotonic()
        with pytest.raises(RpcTimeoutError):
            await asyncio.wait_for(uploader.upload(), timeout=10)
        assert time.monotonic() - start < 5

    @pytest.mark.asyncio
    async def test_a_dead_worker_ends_the_upload_while_one_lives_behind(self, mocker):
        """The cleanup must not wait for the queue when a worker died.

        The workers hold one queued chunk each; the main loop only exits
        when the first failure lands, and the queue behind it is full of
        chunks the dead workers will never take. The old cleanup fed
        end-of-stream sentinels into that full queue and hung there for
        as long as it took every worker to exhaust its retries -- with a
        silent store, forever: nobody drains the queue anymore. The
        exception stayed unretrieved and ``save()``'s failover path was
        never reached, so a retry against a mirror could never start.
        """
        self._config(mocker, upload_part_timeout_s=10, upload_workers_big=2)
        client = mocker.AsyncMock(spec=ITDLibClient)

        calls = 0

        async def save_big_file_part(req):
            nonlocal calls
            calls += 1
            # One part gives up within a second; the other is still inside
            # its own attempt when that failure lands.
            await asyncio.sleep(1 if calls == 1 else 2)
            raise RpcTimeoutError("Telegram did not answer the part")

        client.save_big_file_part = save_big_file_part

        async def silent_after_six_chunks():
            for _ in range(6):
                yield b"x" * self.PART
            await asyncio.sleep(60)

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=silent_after_six_chunks(), size=-1
            ),
        )

        start = time.monotonic()
        # Whichever side notices first -- the cleanup gathering the dead
        # worker, or the feed bailing out on a queue nobody drains -- the
        # upload fails with the worker's own error, never with a bail-out
        # message that hides the store's.
        with pytest.raises(RpcTimeoutError):
            await asyncio.wait_for(uploader.upload(), timeout=10)
        assert time.monotonic() - start < 5

    @pytest.mark.asyncio
    async def test_a_healthy_upload_survives_the_failure_watcher(self, mocker):
        self._config(mocker, upload_part_timeout_s=1)
        client = mocker.AsyncMock(spec=ITDLibClient)
        client.save_big_file_part = mocker.AsyncMock(
            return_value=SaveFilePartResp(success=True)
        )

        uploader = FileUploader(
            client=client,
            file_msg=FileMessageFromStream.new(
                stream=self._stream(3 * self.PART + 10), size=-1
            ),
        )

        assert await uploader.upload() == 3 * self.PART + 10
        assert client.save_big_file_part.await_count == 4
