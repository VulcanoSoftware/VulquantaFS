"""The tree stores carry a file reference's descriptor mirrors in the blob
path, so a restart reads the trust record back instead of queueing every
descriptor for replication again."""

from unittest.mock import Mock, patch

import pytest
from github import Github
from github.Repository import Repository

from vulquantafs.config import GithubRepoConfig
from vulquantafs.core.backfill import _sync_ref
from vulquantafs.core.repository.interface import FDRepositoryResp
from vulquantafs.core.model import (
    TGFSDirectory,
    TGFSFileDesc,
    TGFSFileRef,
    TGFSMetadata,
)
from vulquantafs.core.repository.impl.metadata.github_repo import (
    GithubRepoMetadataRepository,
)
from vulquantafs.core.repository.impl.metadata.github_repo.gh_directory import (
    GithubDirectory,
    file_ref_segment,
    parse_ref_segment,
)


def mirror_segment(name: str, message_id: int, mirrors: dict) -> str:
    return file_ref_segment(name, message_id, mirrors)


@pytest.fixture
def mock_github_config():
    return GithubRepoConfig(
        access_token="test_token", repo="owner/test-repo", commit="main"
    )


@pytest.fixture
def mock_ghc():
    from vulquantafs.core.repository.impl.metadata.github_repo.gh_directory import (
        GithubConfig,
    )

    return GithubConfig(
        gh=Mock(spec=Github),
        repo_name="owner/test-repo",
        repo=Mock(spec=Repository),
        commit="main",
    )


def mock_tree(repo, paths):
    """Make ``get_git_tree`` answer with ``paths`` (a trailing ``/`` is a
    subtree), like the structure tests of ``test_github_repo.py``."""
    from github.GitTreeElement import GitTreeElement

    entries = []
    for path in paths:
        entry = Mock(spec=GitTreeElement)
        entry.path = path.rstrip("/")
        entry.type = "tree" if path.endswith("/") else "blob"
        entry.sha = path + "-sha"
        entry.mode = "100644"
        entries.append(entry)
    tree = Mock()
    tree.tree = entries
    tree.truncated = False
    repo.get_git_tree.return_value = tree
    return tree


@pytest.fixture(autouse=True)
def stub_timestamp_store():
    with patch(
        "vulquantafs.core.repository.impl.metadata.github_repo.DirTimestampStore"
    ) as store_cls:
        store_cls.return_value.get.return_value = None
        yield store_cls


class TestRefSegmentCodec:
    def test_mirrors_round_trip_through_the_segment(self):
        mirrors = {"tg:123456": 5, "dc:1544335068208832542": 77}
        segment = file_ref_segment("report.pdf", 123, mirrors)
        assert segment.startswith("report.pdf.123.m")

        stored, message_id, read_back = parse_ref_segment(segment)
        assert (stored, message_id) == ("report.pdf", 123)
        assert read_back == mirrors

    def test_a_ref_without_mirrors_stays_a_legacy_segment(self):
        assert file_ref_segment("report", 123, {}) == "report.123"
        assert file_ref_segment("report", 123, {"tg:1": 0}) == "report.123"

        stored, message_id, mirrors = parse_ref_segment("report.123")
        assert (stored, message_id, mirrors) == ("report", 123, {})

    def test_dotted_names_keep_parsing(self):
        segment = file_ref_segment("my.report.v2", 9, {"tg:1": 2})
        assert parse_ref_segment(segment) == ("my.report.v2", 9, {"tg:1": 2})

    @pytest.mark.parametrize(
        "segment",
        ["readme", "readme.", "readme.mpayload", "readme.123.m", "readme.123.mx=y"],
    )
    def test_unreadable_segments_are_rejected(self, segment):
        with pytest.raises(ValueError):
            parse_ref_segment(segment)


class TestLoaderRestoresMirrors:
    @patch("vulquantafs.core.repository.impl.metadata.github_repo.Github")
    def test_a_payload_segment_restores_the_mirrors(
        self, mock_github_class, mock_github_config
    ):
        mock_repo = Mock(spec=Repository)
        mock_github_class.return_value.get_repo.return_value = mock_repo
        mock_tree(
            mock_repo, [mirror_segment("document", 123, {"tg:123": 5}), "subdir/"]
        )

        repository = GithubRepoMetadataRepository(mock_github_config)
        root_dir = repository._build_directory_structure()

        assert len(root_dir.files) == 1
        fr = root_dir.files[0]
        assert (fr.name, fr.message_id) == ("document", 123)
        assert fr.mirrors == {"tg:123": 5}

    @patch("vulquantafs.core.repository.impl.metadata.github_repo.Github")
    def test_a_stale_legacy_segment_superseded_by_the_payload(
        self, mock_github_class, mock_github_config
    ):
        mock_repo = Mock(spec=Repository)
        mock_github_class.return_value.get_repo.return_value = mock_repo

        for paths in (
            ["document.123", mirror_segment("document", 123, {"tg:123": 5})],
            [mirror_segment("document", 123, {"tg:123": 5}), "document.123"],
        ):
            mock_tree(mock_repo, paths)
            repository = GithubRepoMetadataRepository(mock_github_config)
            root_dir = repository._build_directory_structure()

            assert len(root_dir.files) == 1
            assert root_dir.files[0].mirrors == {"tg:123": 5}

    @patch("vulquantafs.core.repository.impl.metadata.github_repo.Github")
    def test_a_newer_descriptor_wins_over_an_older_payload(
        self, mock_github_class, mock_github_config
    ):
        mock_repo = Mock(spec=Repository)
        mock_github_class.return_value.get_repo.return_value = mock_repo
        mock_tree(
            mock_repo,
            [mirror_segment("document", 123, {"tg:123": 5}), "document.200"],
        )

        repository = GithubRepoMetadataRepository(mock_github_config)
        root_dir = repository._build_directory_structure()

        assert len(root_dir.files) == 1
        assert (root_dir.files[0].message_id, root_dir.files[0].mirrors) == (200, {})


class TestManagedBlobs:
    @pytest.mark.parametrize(
        "path,managed",
        [
            ("docs/readme.42", True),
            ("docs/readme.42.mdGc6MTIz=5", True),
            ("docs/.gitkeep", True),
            ("docs/.vulquantafsstats.json", False),
            ("docs/foreign.txt", False),
            ("docs/readme", False),
        ],
    )
    def test_the_sync_only_deletes_reference_blobs(self, path, managed):
        assert GithubRepoMetadataRepository._is_managed_blob(path) is managed


def build_repository(mock_github_config, mock_ghc):
    with patch("vulquantafs.core.repository.impl.metadata.github_repo.Github"):
        repository = GithubRepoMetadataRepository(mock_github_config)
    repository._ghc = mock_ghc
    return repository


def plain_tree() -> TGFSMetadata:
    root = TGFSDirectory.root_dir()
    docs = root.create_dir("docs")
    docs.files.append(
        TGFSFileRef(message_id=42, name="readme", location=docs, mirrors={"tg:1": 5})
    )
    return TGFSMetadata(root)


class TestFullSyncMigratesTheSegments:
    @patch("vulquantafs.core.repository.impl.metadata.github_repo.Github")
    @pytest.mark.asyncio
    async def test_push_full_writes_payload_segments_and_drops_legacy(
        self, mock_github_class, mock_github_config, mock_ghc
    ):
        repository = build_repository(mock_github_config, mock_ghc)
        repository.metadata = plain_tree()
        mock_tree(mock_ghc.repo, ["docs/readme.42"])

        ref = Mock()
        ref.object.sha = "commitsha"
        base_commit = Mock()
        base_commit.tree.sha = "basetreesha"
        mock_ghc.repo.get_git_ref.return_value = ref
        mock_ghc.repo.get_git_commit.return_value = base_commit
        mock_ghc.repo.create_git_tree.return_value = Mock()

        await repository.push_full()

        elements = mock_ghc.repo.create_git_tree.call_args[0][0]
        created = {
            e._InputGitTreeElement__path
            for e in elements
            if e._InputGitTreeElement__content == ""
        }
        dropped = {
            e._InputGitTreeElement__path
            for e in elements
            if e._InputGitTreeElement__content != ""
            and e._InputGitTreeElement__sha is None
        }
        assert created == {f"docs/{mirror_segment('readme', 42, {'tg:1': 5})}"}
        assert dropped == {"docs/readme.42"}


class TestIncrementalRefUpdates:
    @patch("vulquantafs.core.repository.impl.metadata.github_repo.commit_tree_overlay")
    @pytest.mark.asyncio
    async def test_a_noted_ref_update_rewrites_the_segment_on_push(
        self, overlay, mock_github_config, mock_ghc
    ):
        repository = build_repository(mock_github_config, mock_ghc)
        root = GithubDirectory(mock_ghc, name="root", parent=None)
        fr = root.attach_file_ref("readme", 42)
        fr.mirrors = {"tg:123": 5}

        fr.location.note_ref_update(fr, (42, {}, None))
        await repository.push()

        overlay.assert_called_once()
        elements = overlay.call_args[0][2]
        created = {
            e._InputGitTreeElement__path
            for e in elements
            if e._InputGitTreeElement__content == ""
        }
        dropped = {
            e._InputGitTreeElement__path
            for e in elements
            if e._InputGitTreeElement__content != ""
            and e._InputGitTreeElement__sha is None
        }
        assert created == {mirror_segment("readme", 42, {"tg:123": 5})}
        assert dropped == {"readme.42"}
        assert mock_ghc.pending_ref_updates == []

    @patch("vulquantafs.core.repository.impl.metadata.github_repo.commit_tree_overlay")
    @pytest.mark.asyncio
    async def test_a_ref_that_vanished_only_drops_its_old_blob(
        self, overlay, mock_github_config, mock_ghc
    ):
        repository = build_repository(mock_github_config, mock_ghc)
        root = GithubDirectory(mock_ghc, name="root", parent=None)
        fr = root.attach_file_ref("readme", 42)
        fr.mirrors = {"tg:123": 5}
        fr.location.note_ref_update(fr, (42, {}, None))
        root.files.remove(fr)

        await repository.push()

        elements = overlay.call_args[0][2]
        assert {e._InputGitTreeElement__path for e in elements} == {"readme.42"}
        assert all(e._InputGitTreeElement__sha is None for e in elements)

    @patch("vulquantafs.core.repository.impl.metadata.github_repo.commit_tree_overlay")
    @pytest.mark.asyncio
    async def test_a_failed_update_is_queued_for_the_next_push(
        self, overlay, mock_github_config, mock_ghc
    ):
        repository = build_repository(mock_github_config, mock_ghc)
        root = GithubDirectory(mock_ghc, name="root", parent=None)
        fr = root.attach_file_ref("readme", 42)
        fr.mirrors = {"tg:123": 5}
        fr.location.note_ref_update(fr, (42, {}, None))
        overlay.side_effect = Exception("GitHub is down")

        await repository.push()
        assert len(mock_ghc.pending_ref_updates) == 1

        overlay.side_effect = None
        await repository.push()
        assert mock_ghc.pending_ref_updates == []
        retried = overlay.call_args[0][2]
        assert {e._InputGitTreeElement__path for e in retried} == {
            mirror_segment("readme", 42, {"tg:123": 5}),
            "readme.42",
        }


class TestRefMutationsReportThemselves:
    async def test_sync_ref_hands_the_old_state_to_the_location(self):
        location = Mock()
        fr = TGFSFileRef(message_id=7, name="a", location=location, mirrors={"tg:1": 2})
        client = Mock()
        client.metadata_api = None
        resp = FDRepositoryResp(
            7, Mock(spec=TGFSFileDesc), mirrors={"tg:1": 2, "dc:9": 8}, store=None
        )

        await _sync_ref(client, fr, resp)

        location.note_ref_update.assert_called_once_with(fr, (7, {"tg:1": 2}, None))
        assert fr.mirrors == {"tg:1": 2, "dc:9": 8}

    async def test_sync_ref_stays_silent_without_a_change(self):
        location = Mock()
        fr = TGFSFileRef(message_id=7, name="a", location=location)
        client = Mock()
        client.metadata_api = None
        resp = FDRepositoryResp(7, Mock(spec=TGFSFileDesc))

        await _sync_ref(client, fr, resp)

        location.note_ref_update.assert_not_called()

    def test_relocating_a_ref_reports_its_blob_state(self):
        location = Mock()
        fr = TGFSFileRef(
            message_id=7,
            name="a",
            location=location,
            mirrors={"dc:9": 8},
            store="tg:1",
        )

        fr.relocate("dc:9")

        assert (fr.message_id, fr.store, fr.mirrors) == (8, "dc:9", {"tg:1": 7})
        location.note_ref_update.assert_called_once_with(fr, (7, {"dc:9": 8}, "tg:1"))

    def test_a_plain_directory_ignores_the_report(self):
        root = TGFSDirectory.root_dir()
        fr = root.attach_file_ref("a", 1)
        fr.location.note_ref_update(fr, (1, {}, None))
