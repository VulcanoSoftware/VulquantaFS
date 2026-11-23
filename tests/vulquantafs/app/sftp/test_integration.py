"""End-to-end checks driving the server with a real SSH/SFTP client.

These exercise the pieces the unit tests deliberately skip: asyncssh's own
protocol handlers, the handshake, authentication and the handle bookkeeping
between OPEN, READ/WRITE and CLOSE.
"""

import asyncio
import datetime
import logging
import stat
import threading
from types import SimpleNamespace
from unittest.mock import call

import asyncssh
import pytest

from tests.vulquantafs.app.sftp.mocked_stats import mocked_stats
from vulquantafs.app.sftp.server import make_server_factory, make_sftp_factory
from vulquantafs.config import GithubRepoConfig, SFTPConfig
from vulquantafs.core import Client
from vulquantafs.core.api import DirectoryApi, FileApi, MetaDataApi
from vulquantafs.core.api.removal_journal import RemovalJournal
from vulquantafs.core.model import (
    TGFSDirectory,
    TGFSFileDesc,
    TGFSFileVersion,
    TGFSMetadata,
)
from vulquantafs.core.repository.impl.metadata.github_repo import (
    GithubRepoMetadataRepository,
)
from vulquantafs.core.repository.interface import IMetaDataRepository
from vulquantafs.reqres import UploadableFileMessage

CONTENT = bytes(range(256)) * 400  # 100 KiB
CREATED_AT = datetime.datetime(2024, 1, 1, 12, 0, 0)
UPDATED_AT = datetime.datetime(2024, 6, 1, 12, 0, 0)


class FakeChannel:
    """A minimal stand-in for the Telegram-backed storage of one client."""

    def __init__(self):
        self.root = TGFSDirectory.root_dir()
        self.contents: dict[str, bytes] = {}

    def add_file(self, directory: TGFSDirectory, name: str, data: bytes) -> None:
        directory.create_file_ref(name, fd_message_id=len(self.contents) + 1)
        self.contents[name] = data


def build_client(mocker, name: str, channel: FakeChannel) -> Client:
    client = mocker.Mock(spec=Client)
    client.name = name

    client.dir_api = mocker.Mock()
    client.dir_api.root = channel.root
    client.dir_api.get_fr = mocker.Mock(
        side_effect=lambda directory, file_name: directory.find_file(file_name)
    )
    client.dir_api.create = mocker.AsyncMock(
        side_effect=lambda n, under: under.create_dir(n)
    )
    client.dir_api.rm_empty = mocker.AsyncMock(
        side_effect=lambda d: d.parent.children.remove(d)
    )
    client.dir_api.rm_dangerously = mocker.AsyncMock()
    client.dir_api.move = mocker.AsyncMock()

    def desc(fr):
        fd = TGFSFileDesc(name=fr.name, created_at=CREATED_AT)
        fd.add_version(
            TGFSFileVersion(
                id="v1",
                updated_at=UPDATED_AT,
                _size=len(channel.contents.get(fr.name, b"")),
                message_ids=[fr.message_id],
            )
        )
        return fd

    async def retrieve(fr, begin, end, as_name):
        data = channel.contents.get(fr.name, b"")
        stop = len(data) if end < 0 else min(len(data), end + 1)

        async def stream():
            for start in range(begin, stop, 8192):
                yield data[start : min(start + 8192, stop)]

        return stream()

    async def upload(directory, file_msg, version_id=None, defer_file_ref=False):
        # `touch` sends a FileMessageEmpty, which carries no readable body;
        # a live upload (SFTP) has an unknown size and is drained to EOF.
        data = bytearray()
        if isinstance(file_msg, UploadableFileMessage):
            while chunk := await file_msg.read(8192):
                data.extend(chunk)
        payload = bytes(data)
        if not directory.find_files([file_msg.name]):
            channel.add_file(directory, file_msg.name, payload)
        else:
            channel.contents[file_msg.name] = payload
        return desc(directory.find_file(file_msg.name))

    async def rm(fr, defer_metadata=False):
        fr.delete()
        channel.contents.pop(fr.name, None)

    client.file_api = mocker.Mock()
    client.file_api.desc = mocker.AsyncMock(side_effect=desc)
    client.file_api.retrieve = mocker.AsyncMock(side_effect=retrieve)
    client.file_api.upload = mocker.AsyncMock(side_effect=upload)
    client.file_api.rm = mocker.AsyncMock(side_effect=rm)
    client.file_api.move = mocker.AsyncMock()

    client.fc_repo = mocker.Mock()
    client.fc_repo.content_length = mocker.AsyncMock(side_effect=lambda fv: fv.size)
    client.file_api.stats = mocked_stats(client)
    return client


def build_config(mocker, users):
    config = mocker.Mock()
    config.vulquantafs.users = users
    config.vulquantafs.sftp = SFTPConfig.from_dict({"upload_buffer_size_mb": 0})
    return config


@pytest.fixture
def channel():
    channel = FakeChannel()
    channel.add_file(channel.root, "note.txt", CONTENT)
    channel.root.create_dir("sub")
    return channel


@pytest.fixture
def users(mocker):
    return {
        "admin": mocker.Mock(password="secret", readonly=False),
        "viewer": mocker.Mock(password="secret", readonly=True),
    }


@pytest.fixture
async def server(mocker, channel, users):
    mocker.patch(
        "vulquantafs.core.ops.create_upload_task", new_callable=mocker.AsyncMock
    )
    mocker.patch(
        "vulquantafs.app.sftp.server.authenticate",
        side_effect=lambda username, password: _authenticate(users, username, password),
    )

    clients = {"notes": build_client(mocker, "notes", channel)}
    config = build_config(mocker, users)

    acceptor = await asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=make_server_factory(config),
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        sftp_factory=make_sftp_factory(clients, config),
    )
    try:
        yield acceptor
    finally:
        acceptor.close()
        await acceptor.wait_closed()


async def test_cold_listing_and_sequential_remove_do_not_wait_for_backend(
    mocker, users, tmp_path
):
    root = TGFSDirectory.root_dir()
    folder = root.create_dir("batch")
    for i in range(2640):
        folder.create_file_ref(f"file-{i}", i + 1)

    mirror_started = asyncio.Event()
    mirror_available = asyncio.Event()

    class Repo(IMetaDataRepository):
        pushes = 0

        async def get(self):
            return TGFSMetadata(root)

        async def push(self):
            self.pushes += 1
            mirror_started.set()
            await mirror_available.wait()

    repo = Repo()
    journal = RemovalJournal(str(tmp_path / "removes.sqlite3"), None)
    api = MetaDataApi(repo, push_mode="coalesced", removal_journal=journal)
    await api.init()
    store = mocker.Mock(deletes_on_remove=False)

    async def stalled_descriptor(*args, **kwargs):
        # Outlast the client's timeout without leaving the server stuck
        # closing the SFTP session if the listing regresses.
        await asyncio.sleep(4)
        return TGFSFileDesc.empty("unavailable.txt")

    descriptors = mocker.Mock(
        get_file_desc=mocker.AsyncMock(side_effect=stalled_descriptor),
        content_length=mocker.AsyncMock(return_value=len(CONTENT)),
    )
    client = mocker.Mock(spec=Client, name="notes")
    client.file_api = FileApi(
        api, descriptors, store, has_cached_versions=lambda: False
    )
    client.dir_api = DirectoryApi(api, client.file_api, store)
    config = build_config(mocker, users)
    mocker.patch(
        "vulquantafs.app.sftp.server.authenticate",
        side_effect=lambda username, password: _authenticate(users, username, password),
    )
    acceptor = await asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=make_server_factory(config),
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        sftp_factory=make_sftp_factory({"notes": client}, config),
    )
    try:
        async with await connect(acceptor) as conn:
            async with conn.start_sftp_client() as sftp:
                entries = await asyncio.wait_for(
                    sftp.readdir("/notes/batch"), timeout=3
                )
                files = [
                    entry for entry in entries if entry.filename.startswith("file-")
                ]
                assert len(files) == 2640
                assert all(entry.attrs.size is None for entry in files)
                assert all(stat.S_ISREG(entry.attrs.permissions) for entry in files)
                assert all(
                    entry.longname == f"type=file; {entry.filename}" for entry in files
                )
                descriptors.get_file_desc.assert_not_called()
                await sftp.remove("/notes/batch/file-0")
                flush = asyncio.create_task(api.flush_removals())
                try:
                    await asyncio.wait_for(mirror_started.wait(), timeout=2)
                    async with asyncio.timeout(15):
                        for i in range(1, 2640):
                            await sftp.remove(f"/notes/batch/file-{i}")
                    assert not folder.find_files()
                    assert repo.pushes == 1
                    assert len(journal.load()[1]) == 2640
                finally:
                    mirror_available.set()
                    await flush
        assert repo.pushes == 2
        assert journal.load() == (0, set())
        descriptors.get_file_desc.assert_not_called()
    finally:
        acceptor.close()
        await acceptor.wait_closed()
        mirror_available.set()
        await api.close()


async def test_rmdir_stays_responsive_and_does_not_wait_for_history(mocker, users):
    loop = asyncio.get_running_loop()
    history_started, commit_started = asyncio.Event(), asyncio.Event()
    release_history, release_commit = threading.Event(), threading.Event()
    github = mocker.Mock()
    ref = github.get_git_ref.return_value
    ref.object.sha = "head"
    base = github.get_git_commit.return_value
    base.tree.sha = "root-tree"
    github.create_git_commit.return_value.sha = "next"

    def blocked(started, release, result=None):
        loop.call_soon_threadsafe(started.set)
        assert release.wait(10), "test did not release blocked GitHub request"
        return result

    github.get_commits.return_value.get_page.side_effect = lambda _: blocked(
        history_started, release_history, []
    )
    ref.edit.side_effect = lambda _: blocked(commit_started, release_commit)
    github.get_git_tree.side_effect = [
        SimpleNamespace(
            truncated=False,
            tree=[
                SimpleNamespace(path="parent", type="tree", sha="parent-tree"),
                SimpleNamespace(path="other", type="tree", sha="other-tree"),
            ],
        ),
        SimpleNamespace(
            truncated=False,
            tree=[SimpleNamespace(path="batch", type="tree", sha="huge-tree")],
        ),
    ]
    repo = GithubRepoMetadataRepository(
        GithubRepoConfig(
            access_token="unused",
            repo="test/metadata",
            commit="main",
            squash_history_max_commits=100_000,
        ),
        repo=github,
    )
    root = TGFSDirectory.root_dir()
    root.create_dir("parent").create_dir("batch")
    other = root.create_dir("other")
    other.create_file_ref("live", 1)
    repo.metadata = TGFSMetadata(root)
    api = MetaDataApi(repo, push_mode="coalesced", push_window=0)
    client = build_client(mocker, "notes", FakeChannel())
    media = mocker.Mock()
    client.dir_api = DirectoryApi(api, client.file_api, media)
    config = build_config(mocker, users)
    mocker.patch(
        "vulquantafs.app.sftp.server.authenticate",
        side_effect=lambda username, password: _authenticate(users, username, password),
    )
    server = await asyncssh.listen(
        host="127.0.0.1",
        port=0,
        server_factory=make_server_factory(config),
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        sftp_factory=make_sftp_factory({"notes": client}, config),
    )
    heal = asyncio.create_task(api.heal())
    try:
        async with asyncio.timeout(5):
            await history_started.wait()
            async with await connect(server) as conn:
                async with conn.start_sftp_client() as sftp:
                    async with conn.start_sftp_client() as browser:
                        with pytest.raises(asyncssh.SFTPFailure):
                            await sftp.rmdir("/notes/other")
                        removal = asyncio.create_task(sftp.rmdir("/notes/parent/batch"))
                        try:
                            await commit_started.wait()
                            assert "other" in await browser.listdir("/notes")
                            assert "batch" in await browser.listdir("/notes/parent")
                        finally:
                            release_commit.set()
                            await removal
                        assert "batch" not in await browser.listdir("/notes/parent")
                        assert "other" in await browser.listdir("/notes")
            assert not heal.done()
    finally:
        release_history.set()
        release_commit.set()
        await heal
        server.close()
        await server.wait_closed()
        await api.close()
        repo._ghc.writer.shutdown()

    assert github.get_git_tree.call_args_list == [
        call("root-tree"),
        call("parent-tree"),
    ]
    args, kwargs = github.create_git_tree.call_args
    assert kwargs == {"base_tree": base.tree}
    assert [element._identity for element in args[0]] == [
        {"path": "parent/batch", "mode": "040000", "type": "tree", "sha": None},
        {"path": "parent/.gitkeep", "mode": "100644", "type": "blob", "content": ""},
    ]
    ref.edit.assert_called_once_with("next")
    assert not media.mock_calls


def _authenticate(users, username, password):
    from vulquantafs.auth.user import AdminUser, ReadonlyUser
    from vulquantafs.errors.vulquantafs import LoginFailed

    user = users.get(username)
    if user is None or user.password != password:
        raise LoginFailed("No such user or password is incorrect.")
    return ReadonlyUser(username) if user.readonly else AdminUser(username)


async def connect(acceptor, username="admin", password="secret"):
    return await asyncssh.connect(
        host="127.0.0.1",
        port=acceptor.get_port(),
        username=username,
        password=password,
        known_hosts=None,
    )


class TestAuthentication:
    async def test_valid_credentials_are_accepted(self, server):
        async with await connect(server) as conn:
            async with conn.start_sftp_client() as sftp:
                assert await sftp.listdir("/") == [".", "..", "notes"]

    async def test_a_wrong_password_is_rejected(self, server):
        with pytest.raises(asyncssh.PermissionDenied):
            await connect(server, password="wrong")

    async def test_an_unknown_user_is_rejected(self, server):
        with pytest.raises(asyncssh.PermissionDenied):
            await connect(server, username="ghost")


class TestBrowsing:
    async def test_root_lists_the_clients(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            assert await sftp.listdir("/") == [".", "..", "notes"]

    async def test_client_root_lists_its_entries(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            assert sorted(await sftp.listdir("/notes")) == [
                ".",
                "..",
                "note.txt",
                "sub",
            ]

    async def test_stat_reports_the_file_size(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            assert (await sftp.stat("/notes/note.txt")).size == len(CONTENT)

    async def test_isdir_and_isfile(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            assert await sftp.isdir("/notes/sub")
            assert await sftp.isfile("/notes/note.txt")

    async def test_missing_paths_are_reported(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            with pytest.raises(asyncssh.SFTPNoSuchFile):
                await sftp.stat("/notes/nope.txt")


class TestTransfers:
    async def test_download_returns_the_whole_file(self, server, tmp_path):
        target = tmp_path / "note.txt"
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.get("/notes/note.txt", str(target))

        assert target.read_bytes() == CONTENT

    async def test_random_access_read(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            async with await sftp.open("/notes/note.txt", "rb") as fh:
                await fh.seek(70_000)
                tail = await fh.read(64)
                await fh.seek(10)
                head = await fh.read(64)

        assert tail == CONTENT[70_000:70_064]
        assert head == CONTENT[10:74]

    async def test_upload_stores_the_payload(self, server, channel, tmp_path):
        source = tmp_path / "upload.bin"
        payload = bytes(range(256)) * 200
        source.write_bytes(payload)

        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/upload.bin")

        assert channel.contents["upload.bin"] == payload

    async def test_upload_20k_files_from_two_sessions(self, server, channel, caplog):
        caplog.set_level(logging.WARNING)
        expected = {f"file-{i:05d}.bin": f"payload {i}".encode() for i in range(20_000)}

        async def upload_batch(items):
            async with await connect(server) as conn, conn.start_sftp_client() as sftp:
                for name, payload in items:
                    async with sftp.open(f"/notes/sub/{name}", "wb") as fh:
                        await fh.write(payload)

        items = list(expected.items())
        async with asyncio.timeout(120), asyncio.TaskGroup() as sessions:
            sessions.create_task(upload_batch(items[::2]))
            sessions.create_task(upload_batch(items[1::2]))

        # Every acknowledged CLOSE must have committed the full payload.
        assert channel.contents == {"note.txt": CONTENT, **expected}
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            listed = await asyncio.wait_for(sftp.listdir("/notes/sub"), timeout=20)
        assert sorted(listed) == [".", "..", *sorted(expected)]

    async def test_round_trip(self, server, tmp_path):
        source = tmp_path / "round.bin"
        target = tmp_path / "round.out"
        source.write_bytes(CONTENT)

        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/round.bin")
            await sftp.get("/notes/round.bin", str(target))

        assert target.read_bytes() == CONTENT

    async def test_an_empty_upload_creates_an_empty_file(
        self, server, channel, tmp_path
    ):
        source = tmp_path / "empty.bin"
        source.write_bytes(b"")

        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.put(str(source), "/notes/empty.bin")

        assert channel.contents["empty.bin"] == b""


class TestMutations:
    async def test_mkdir_and_rmdir(self, server, channel):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.mkdir("/notes/fresh")
            assert "fresh" in [d.name for d in channel.root.find_dirs()]

            await sftp.rmdir("/notes/fresh")
            assert "fresh" not in [d.name for d in channel.root.find_dirs()]

    async def test_remove(self, server, channel):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            await sftp.remove("/notes/note.txt")

        assert channel.root.find_files(["note.txt"]) == []

    async def test_mkdir_at_the_root_is_refused(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            with pytest.raises(asyncssh.SFTPPermissionDenied):
                await sftp.mkdir("/fresh")


class TestReadonlyUser:
    async def test_may_read(self, server, tmp_path):
        target = tmp_path / "note.txt"
        async with await connect(server, username="viewer") as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.get("/notes/note.txt", str(target))

        assert target.read_bytes() == CONTENT

    async def test_may_not_upload(self, server, tmp_path):
        source = tmp_path / "upload.bin"
        source.write_bytes(b"nope")

        async with await connect(server, username="viewer") as conn:
            async with conn.start_sftp_client() as sftp:
                with pytest.raises(asyncssh.SFTPPermissionDenied):
                    await sftp.put(str(source), "/notes/upload.bin")

    async def test_may_not_remove(self, server):
        async with await connect(server, username="viewer") as conn:
            async with conn.start_sftp_client() as sftp:
                with pytest.raises(asyncssh.SFTPPermissionDenied):
                    await sftp.remove("/notes/note.txt")
                with pytest.raises(asyncssh.SFTPPermissionDenied):
                    await sftp.rmdir("/notes/sub")


class TestConcurrency:
    async def test_parallel_reads_of_the_same_file(self, server):
        async with await connect(server) as conn, conn.start_sftp_client() as sftp:
            async with await sftp.open("/notes/note.txt", "rb") as fh:
                chunks = await asyncio.gather(
                    *(fh.read(1024, offset) for offset in range(0, 8192, 1024))
                )

        assert b"".join(chunks) == CONTENT[:8192]
