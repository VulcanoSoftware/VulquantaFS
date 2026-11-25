import pytest

from vulquantafs.config import (
    BotConfig,
    CacheConfig,
    Config,
    GithubRepoConfig,
    JWTConfig,
    ListingConfig,
    ManagerConfig,
    MetadataConfig,
    MetadataConfigDict,
    MetadataType,
    ServerConfig,
    SFTPConfig,
    TGFSConfig,
    TransferConfig,
    UserConfig,
    WebDAVConfig,
    _default_data_dir,
    _env_with_legacy_fallback,
)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
def test_telegram_bot_login_timeout_must_be_finite_and_positive(value):
    with pytest.raises(ValueError, match="login_timeout_s"):
        BotConfig.from_dict({"session_file": "bot.session", "login_timeout_s": value})


def test_telegram_bot_login_timeout_defaults_to_twenty_and_can_be_changed():
    assert BotConfig.from_dict({"session_file": "bot.session"}).login_timeout_s == 20
    assert (
        BotConfig.from_dict(
            {"session_file": "bot.session", "login_timeout_s": 5.5}
        ).login_timeout_s
        == 5.5
    )


class TestWebDAVConfig:
    def test_from_dict(self):
        data = {"host": "localhost", "port": 8080, "path": "/webdav"}
        config = WebDAVConfig.from_dict(data)

        assert config.host == "localhost"
        assert config.port == 8080
        assert config.path == "/webdav"


class TestManagerConfig:
    def test_from_dict(self):
        data = {"host": "0.0.0.0", "port": 9000}
        config = ManagerConfig.from_dict(data)

        assert config.host == "0.0.0.0"
        assert config.port == 9000


class TestUserConfig:
    def test_from_dict_readonly_false(self):
        data = {"password": "secret", "readonly": False}
        config = UserConfig.from_dict(data)

        assert config.password == "secret"
        assert config.readonly is False

    def test_from_dict_readonly_default_false(self):
        data = {"password": "secret"}
        config = UserConfig.from_dict(data)

        assert config.password == "secret"
        assert config.readonly is False

    def test_from_dict_readonly_true(self):
        data = {"password": "secret", "readonly": True}
        config = UserConfig.from_dict(data)

        assert config.password == "secret"
        assert config.readonly is True


class TestJWTConfig:
    def test_from_dict(self):
        data = {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600}
        config = JWTConfig.from_dict(data)

        assert config.secret == "jwt_secret"
        assert config.algorithm == "HS256"
        assert config.life == 3600


class TestServerConfig:
    def test_from_dict(self):
        data = {"host": "127.0.0.1", "port": 8000}
        config = ServerConfig.from_dict(data)

        assert config.host == "127.0.0.1"
        assert config.port == 8000


class TestTGFSConfig:
    def test_from_dict_minimal(self):
        data = {
            "users": {},
            "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
            "server": {"host": "localhost", "port": 3000},
        }
        config = TGFSConfig.from_dict(data)

        assert config.users == {}
        assert config.jwt.secret == "test"
        assert config.server.host == "localhost"

    def test_from_dict_with_users(self):
        data = {
            "users": {
                "admin": {"password": "admin123", "readonly": False},
                "viewer": {"password": "view123", "readonly": True},
            },
            "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
            "server": {"host": "localhost", "port": 3000},
        }
        config = TGFSConfig.from_dict(data)

        assert "admin" in config.users
        assert "viewer" in config.users
        assert config.users["admin"].password == "admin123"
        assert config.users["admin"].readonly is False
        assert config.users["viewer"].readonly is True

    def test_from_dict_no_users(self):
        data = {
            "users": None,
            "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
            "server": {"host": "localhost", "port": 3000},
        }
        config = TGFSConfig.from_dict(data)

        assert config.users == {}

    def test_from_dict_without_sftp_block(self):
        data = {
            "users": {},
            "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
            "server": {"host": "localhost", "port": 3000},
        }
        config = TGFSConfig.from_dict(data)

        assert config.sftp.enabled is False
        assert config.sftp.port == SFTPConfig.DEFAULT_PORT

    def test_from_dict_with_sftp_block(self):
        data = {
            "users": {},
            "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
            "server": {"host": "localhost", "port": 3000},
            "sftp": {"enabled": True, "port": 2200},
        }
        config = TGFSConfig.from_dict(data)

        assert config.sftp.enabled is True
        assert config.sftp.port == 2200


class TestSFTPConfig:
    def test_defaults(self):
        config = SFTPConfig.from_dict(None)

        assert config.enabled is False
        assert config.host == "0.0.0.0"
        assert config.port == SFTPConfig.DEFAULT_PORT
        assert config.host_key_file.endswith(SFTPConfig.DEFAULT_HOST_KEY_FILE)
        assert config.authorized_keys_dir is None
        assert config.upload_buffer_dir is None
        assert config.upload_buffer_size_mb == SFTPConfig.DEFAULT_UPLOAD_BUFFER_SIZE_MB
        assert (
            config.fast_readdir_min_files == SFTPConfig.DEFAULT_FAST_READDIR_MIN_FILES
        )

    def test_full_block(self):
        config = SFTPConfig.from_dict(
            {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 2200,
                "host_key_file": "keys/host_key",
                "authorized_keys_dir": "keys/authorized",
                "upload_buffer_size_mb": 8,
                "upload_buffer_dir": "spool",
                "fast_readdir_min_files": 0,
            }
        )

        assert config.enabled is True
        assert config.host == "127.0.0.1"
        assert config.port == 2200
        assert config.host_key_file.endswith("host_key")
        assert config.authorized_keys_dir is not None
        assert config.authorized_keys_dir.endswith("authorized")
        assert config.upload_buffer_size_mb == 8
        assert config.upload_buffer_size_bytes == 8 * 1024 * 1024
        assert config.upload_buffer_dir is not None
        assert config.fast_readdir_min_files == 0

    @pytest.mark.parametrize("port", [0, 65536, -1])
    def test_rejects_invalid_port(self, port):
        with pytest.raises(ValueError):
            SFTPConfig.from_dict({"port": port})

    def test_rejects_negative_buffer_size(self):
        with pytest.raises(ValueError):
            SFTPConfig.from_dict({"upload_buffer_size_mb": -1})

    def test_rejects_negative_fast_readdir_threshold(self):
        with pytest.raises(ValueError, match="fast_readdir_min_files"):
            SFTPConfig.from_dict({"fast_readdir_min_files": -1})


class TestCacheConfig:
    def test_off_and_defaults_when_absent(self):
        config = TGFSConfig.from_dict(
            {
                "users": {},
                "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
                "server": {"host": "localhost", "port": 3000},
            }
        )

        cache = config.cache
        assert cache.enabled is False
        assert cache.dir == "cache"
        assert cache.max_size_mb == 20 * 1024
        assert cache.max_files == 0
        assert cache.max_file_size_mb == 4 * 1024
        assert cache.block_kb == 4 * 1024
        assert cache.stage_uploads is True
        assert cache.keep_for_reads is True
        assert cache.min_free_mb == 1024
        assert cache.max_age_hours == 0
        assert cache.target_fill_percent == 90
        assert cache.target_bytes == 20 * 1024 * 1024 * 1024 * 90 // 100
        assert cache.directory.endswith("cache")

    def test_sweep_settings(self):
        cache = CacheConfig.from_dict(
            {"min_free_mb": 0, "max_age_hours": 48, "target_fill_percent": 100}
        )
        assert cache.min_free_bytes == 0
        assert cache.max_age_seconds == 48 * 3600
        assert cache.target_bytes is None
        assert CacheConfig.from_dict({"max_size_mb": 0}).target_bytes is None
        for bad in (0, 101):
            with pytest.raises(ValueError, match="target_fill_percent"):
                CacheConfig.from_dict({"target_fill_percent": bad})
        with pytest.raises(ValueError, match="min_free_mb"):
            CacheConfig.from_dict({"min_free_mb": -1})

    def test_overrides(self):
        cache = CacheConfig.from_dict(
            {
                "enabled": True,
                "dir": "staging",
                "max_size_mb": 100,
                "max_files": 5,
                "max_file_size_mb": 0,
                "block_kb": 1024,
                "stage_uploads": False,
                "keep_for_reads": False,
            }
        )
        assert cache.enabled is True
        assert cache.max_size_bytes == 100 * 1024 * 1024
        assert cache.max_files == 5
        assert cache.max_file_size_bytes == 0
        assert cache.block_bytes == 1024 * 1024
        assert cache.stage_uploads is False
        assert cache.keep_for_reads is False

    def test_negative_budget_rejected(self):
        with pytest.raises(ValueError, match="max_size_mb"):
            CacheConfig.from_dict({"max_size_mb": -1})

    def test_tiny_block_rejected(self):
        with pytest.raises(ValueError, match="block_kb"):
            CacheConfig.from_dict({"block_kb": 16})


class TestListingConfig:
    def test_defaults_when_absent(self):
        listing = ListingConfig.from_dict(None)
        assert listing.stat_in_metadata is False
        assert listing.validate_on_list is True
        assert listing.message_cache_size == 1024

    def test_values_are_read(self):
        listing = ListingConfig.from_dict(
            {
                "stat_in_metadata": True,
                "validate_on_list": False,
                "message_cache_size": 50000,
            }
        )
        assert listing.stat_in_metadata is True
        assert listing.validate_on_list is False
        assert listing.message_cache_size == 50000

    @pytest.mark.parametrize("bad", [0, -1])
    def test_message_cache_needs_room(self, bad):
        with pytest.raises(ValueError, match="message_cache_size"):
            ListingConfig.from_dict({"message_cache_size": bad})


class TestTransferConfig:
    def test_defaults_when_absent(self):
        """An existing config.yaml has no transfer block and must still load."""
        config = TGFSConfig.from_dict(
            {
                "users": {},
                "jwt": {"secret": "test", "algorithm": "HS256", "life": 1800},
                "server": {"host": "localhost", "port": 3000},
            }
        )

        assert config.transfer.upload_workers_small == 3
        assert config.transfer.upload_workers_big == 8
        assert config.transfer.upload_part_size_kb == 512
        assert config.transfer.download_pieces_in_flight == 4
        assert config.transfer.connection_pool_size == 1
        assert config.transfer.chunk_cache_mb == 0
        assert config.transfer.upload_parts_in_flight == 1
        assert config.transfer.rpc_timeout_s == 120
        assert config.transfer.upload_part_timeout_s == 15

    def test_overrides(self):
        config = TransferConfig.from_dict(
            {
                "upload_workers_big": 12,
                "upload_part_size_kb": 256,
                "download_piece_size_kb": 8192,
                "download_pieces_in_flight": 6,
                "parallel_download_threshold_mb": 32,
                "connection_pool_size": 4,
                "chunk_cache_mb": 128,
                "chunk_cache_readahead": 3,
            }
        )

        assert config.upload_workers_big == 12
        assert config.upload_part_size_bytes == 256 * 1024
        assert config.download_piece_size_bytes == 8192 * 1024
        assert config.parallel_download_threshold_bytes == 32 * 1024 * 1024
        assert config.chunk_cache_bytes == 128 * 1024 * 1024

    @pytest.mark.parametrize("part_size", [700, 300, 1024])
    def test_rejects_part_sizes_telegram_would_refuse(self, part_size):
        """Telegram only accepts part sizes that divide 512 KiB."""
        with pytest.raises(ValueError, match="upload_part_size_kb"):
            TransferConfig.from_dict({"upload_part_size_kb": part_size})

    @pytest.mark.parametrize(
        "key",
        ["upload_workers_big", "download_pieces_in_flight", "connection_pool_size"],
    )
    def test_rejects_non_positive_counts(self, key):
        with pytest.raises(ValueError, match=key):
            TransferConfig.from_dict({key: 0})

    def test_rejects_a_negative_cache_budget(self):
        with pytest.raises(ValueError, match="chunk_cache_mb"):
            TransferConfig.from_dict({"chunk_cache_mb": -1})

    def test_a_disabled_cache_is_allowed(self):
        assert TransferConfig.from_dict({"chunk_cache_mb": 0}).chunk_cache_mb == 0

    def test_a_disabled_part_bound_is_allowed(self):
        config = TransferConfig.from_dict({"upload_part_timeout_s": 0})
        assert config.upload_part_timeout_s == 0

    def test_rejects_a_negative_part_bound(self):
        with pytest.raises(ValueError, match="upload_part_timeout_s"):
            TransferConfig.from_dict({"upload_part_timeout_s": -1})


class TestGithubRepoConfig:
    def test_from_dict(self):
        data = {"repo": "owner/repo", "commit": "main", "access_token": "token123"}
        config = GithubRepoConfig.from_dict(data)

        assert config.repo == "owner/repo"
        assert config.commit == "main"
        assert config.access_token == "token123"


class TestMetadataConfig:
    def test_from_dict_pinned_message(self):
        config = MetadataConfig.from_dict(
            MetadataConfigDict(type="pinned_message", name="name", github_repo=None)
        )

        assert config.type == MetadataType.PINNED_MESSAGE
        assert config.github_repo is None

    def test_from_dict_github_repo(self):
        config = MetadataConfig.from_dict(
            MetadataConfigDict(
                type="github_repo",
                name="name",
                github_repo={
                    "repo": "owner/repo",
                    "commit": "main",
                    "access_token": "token123",
                },
            )
        )

        assert config.type == MetadataType.GITHUB_REPO
        assert config.github_repo is not None
        assert config.github_repo.repo == "owner/repo"

    def test_from_dict_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown metadata type: unknown_type"):
            MetadataConfig.from_dict(
                MetadataConfigDict(type="unknown_type", name="name", github_repo=None)
            )


class TestConfig:
    def test_from_dict(self):
        data = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "account": {"session_file": "account.session"},
                "login_timeout": 30000,
                "private_file_channel": 123456,
                "public_file_channel": 654321,
            },
            "vulquantafs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 8080},
            },
        }
        config = Config.from_dict(data)

        assert config.telegram.api_id == 12345

    def test_from_dict_accepts_legacy_tgfs_block(self):
        # A config written for tgfs names the application block ``tgfs``;
        # it has to load as the ``vulquantafs`` block without any migration.
        data = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "private_file_channel": 123456,
            },
            "tgfs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 8080},
            },
        }
        config = Config.from_dict(data)

        assert config.vulquantafs.server.port == 8080

    def test_from_dict_accepts_legacy_tgdcfs_block(self):
        # A config written for tgdcfs names the application block
        # ``tgdcfs``; it has to load as the ``vulquantafs`` block.
        data = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "private_file_channel": 123456,
            },
            "tgdcfs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 8080},
            },
        }
        config = Config.from_dict(data)

        assert config.vulquantafs.server.port == 8080

    def test_from_dict_prefers_vulquantafs_block_over_legacy(self):
        data = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "private_file_channel": 123456,
            },
            "vulquantafs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 8080},
            },
            "tgfs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 9090},
            },
        }
        config = Config.from_dict(data)

        assert config.vulquantafs.server.port == 8080

    def test_from_dict_without_application_block(self):
        data = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "private_file_channel": 123456,
            },
        }
        with pytest.raises(ValueError, match="vulquantafs"):
            Config.from_dict(data)


class TestLegacyEnvironmentFallback:
    def test_new_name_wins(self, monkeypatch):
        monkeypatch.setenv("VULQUANTAFS_TEST_VAR", "new")
        monkeypatch.setenv("TGDCFS_TEST_VAR", "older")
        monkeypatch.setenv("TGFS_TEST_VAR", "old")
        assert (
            _env_with_legacy_fallback(
                "VULQUANTAFS_TEST_VAR",
                "TGDCFS_TEST_VAR",
                "TGFS_TEST_VAR",
                default="default",
            )
            == "new"
        )

    def test_the_tgdcfs_name_is_used_when_the_new_name_is_missing(self, monkeypatch):
        # A container started before the rebrand sets TGDCFS_DATA_DIR; it has
        # to keep finding the data directory it has always used.
        monkeypatch.delenv("VULQUANTAFS_TEST_VAR", raising=False)
        monkeypatch.setenv("TGDCFS_TEST_VAR", "older")
        monkeypatch.setenv("TGFS_TEST_VAR", "old")
        assert (
            _env_with_legacy_fallback(
                "VULQUANTAFS_TEST_VAR",
                "TGDCFS_TEST_VAR",
                "TGFS_TEST_VAR",
                default="default",
            )
            == "older"
        )

    def test_the_tgfs_name_is_used_when_both_newer_names_are_missing(self, monkeypatch):
        monkeypatch.delenv("VULQUANTAFS_TEST_VAR", raising=False)
        monkeypatch.delenv("TGDCFS_TEST_VAR", raising=False)
        monkeypatch.setenv("TGFS_TEST_VAR", "old")
        assert (
            _env_with_legacy_fallback(
                "VULQUANTAFS_TEST_VAR",
                "TGDCFS_TEST_VAR",
                "TGFS_TEST_VAR",
                default="default",
            )
            == "old"
        )

    def test_default_when_none_is_set(self, monkeypatch):
        for name in ("VULQUANTAFS_TEST_VAR", "TGDCFS_TEST_VAR", "TGFS_TEST_VAR"):
            monkeypatch.delenv(name, raising=False)
        assert (
            _env_with_legacy_fallback(
                "VULQUANTAFS_TEST_VAR",
                "TGDCFS_TEST_VAR",
                "TGFS_TEST_VAR",
                default="default",
            )
            == "default"
        )


class TestDefaultDataDirectory:
    """A tgdcfs install keeps its master salt in ~/.tgdcfs.

    Booting into a fresh ~/.vulquantafs would create a new salt, derive a
    different master key and leave every encrypted file unreadable, so the
    legacy directory is reused as long as the new one does not exist.
    """

    @staticmethod
    def _home(tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        return tmp_path

    def test_the_current_directory_is_used_when_it_exists(self, tmp_path, monkeypatch):
        home = self._home(tmp_path, monkeypatch)
        (home / ".vulquantafs").mkdir()
        (home / ".tgdcfs").mkdir()
        assert _default_data_dir() == str(home / ".vulquantafs")

    def test_an_existing_tgdcfs_directory_is_used_in_place(self, tmp_path, monkeypatch):
        home = self._home(tmp_path, monkeypatch)
        (home / ".tgdcfs").mkdir()
        assert _default_data_dir() == str(home / ".tgdcfs")

    def test_a_fresh_install_gets_the_current_directory(self, tmp_path, monkeypatch):
        home = self._home(tmp_path, monkeypatch)
        assert _default_data_dir() == str(home / ".vulquantafs")


class TestConfigFunctions:
    def test_get_config_loads_file(self, mocker):
        mock_open = mocker.patch("vulquantafs.config.open")
        mock_yaml_load = mocker.patch("vulquantafs.config.yaml.safe_load")
        mocker.patch("vulquantafs.config.__config", None)
        from vulquantafs.config import get_config

        mock_yaml_load.return_value = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "hash123",
                "bot": {"token": "bot_token", "session_file": "bot.session"},
                "account": {"session_file": "account.session"},
                "login_timeout": 30000,
                "private_file_channel": 123456,
                "public_file_channel": 654321,
            },
            "vulquantafs": {
                "users": {},
                "metadata": {"123456": {"name": "default", "type": "pinned_message"}},
                "jwt": {"secret": "jwt_secret", "algorithm": "HS256", "life": 3600},
                "server": {"host": "0.0.0.0", "port": 8080},
            },
        }

        config = get_config()

        mock_open.assert_called_once()
        mock_yaml_load.assert_called_once()
        assert config.telegram.api_id == 12345
