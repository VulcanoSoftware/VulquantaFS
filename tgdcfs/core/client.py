from typing import Awaitable, Callable, Dict, List, Optional

from tgdcfs.backends.base import IStore
from tgdcfs.config import (
    Config,
    EncryptionConfig,
    FilesystemConfig,
    MetadataConfig,
    MetadataLayout,
    MetadataType,
    StoreConfig,
)
from tgdcfs.core.api import (
    DirectoryApi,
    FileApi,
    FileDescApi,
    FileStatApi,
    MetaDataApi,
)
from tgdcfs.core.local_cache import LocalCache
from tgdcfs.core.mirror import MirrorGroup, MirrorStore
from tgdcfs.core.model import TGFSFileRef
from tgdcfs.core.replication import ReplicationQueue, file_path
from tgdcfs.core.repository.impl import (
    PinnedMessageMetadataRepository,
    StoreFDRepository,
    StoreFileContentRepository,
)
from tgdcfs.core.repository.interface import (
    IFDRepository,
    IFileContentRepository,
    IMetaDataRepository,
)

# Builds the live store for a store config; the backends' logins live
# behind it so ``Client.create`` never touches a backend library.
StoreFactory = Callable[[StoreConfig], Awaitable[IStore]]


class Client:
    """One virtual file system: a primary store, its mirrors and the APIs
    that serve the tree stored there."""

    def __init__(
        self,
        name: str,
        store: IStore,
        file_api: FileApi,
        dir_api: DirectoryApi,
        fc_repo: IFileContentRepository,
        fd_repo: Optional[IFDRepository] = None,
        metadata_api: Optional[MetaDataApi] = None,
        mirror_group: Optional[MirrorGroup] = None,
        filesystem: Optional[FilesystemConfig] = None,
        cache: Optional[LocalCache] = None,
    ):
        self.name = name
        self.store = store
        self.file_api = file_api
        self.dir_api = dir_api
        self.fc_repo = fc_repo
        self.fd_repo = fd_repo
        self.metadata_api = metadata_api
        self.mirror_group = mirror_group
        self.filesystem = filesystem
        # The local cache shared by every file system; ``None`` when off.
        self.cache = cache

    @property
    def message_api(self) -> IStore:
        """The primary store, under the name the app layer grew up with."""
        return self.store

    @classmethod
    async def create(
        cls,
        filesystem: FilesystemConfig,
        config: Config,
        store_factory: StoreFactory,
        encryption_cfg: Optional[EncryptionConfig] = None,
        replication: Optional[ReplicationQueue] = None,
        cache: Optional[LocalCache] = None,
    ) -> "Client":
        store = await store_factory(config.stores[filesystem.primary])
        inline = filesystem.sync == "inline" or replication is None
        read_preference = [
            config.stores[name].key for name in filesystem.read_preference
        ]
        # The live stores of this file system by config name; a metadata
        # store that pins a document in one of them reuses the instance.
        live_stores: Dict[str, IStore] = {filesystem.primary: store}

        # One store per mirror; the serialization key is the store's key
        # (backend prefix + channel id as configured), stable across
        # backend libs and config renames.
        mirror_group: Optional[MirrorGroup] = None
        if filesystem.mirrors:
            mirror_stores: List[MirrorStore] = []
            for mirror_name in filesystem.mirrors:
                mirror_cfg = config.stores[mirror_name]
                mirror_store = await store_factory(mirror_cfg)
                live_stores[mirror_name] = mirror_store
                mirror_stores.append(
                    MirrorStore(key=mirror_cfg.key, store=mirror_store)
                )
            mirror_group = MirrorGroup(
                primary=store,
                stores=mirror_stores,
                mode=filesystem.mode,
                strict=filesystem.strict,
                tee=filesystem.sync == "tee",
            )

        # ``sync: tee``: the mirrors that re-upload are fed from the upload
        # stream itself, the others copied inline; the queue only gets the
        # files a store still lacks a copy of.
        tee = filesystem.sync == "tee" and mirror_group is not None
        fc_repo: IFileContentRepository = StoreFileContentRepository(
            store,
            mirror_group=mirror_group,
            inline_mirroring=inline,
            tee_mirroring=tee,
            tee_buffer=config.tgdcfs.transfer.tee_buffer_bytes,
            read_preference=read_preference,
            cache=cache,
            cache_scope=filesystem.name,
            read_parallel=filesystem.read_parallel,
            read_sources=[config.stores[name].key for name in filesystem.read_sources],
            piece_size=config.tgdcfs.transfer.download_piece_size_bytes,
            parallel_threshold=config.tgdcfs.transfer.parallel_download_threshold_bytes,
            read_window=config.tgdcfs.transfer.read_parallel_window,
        )

        # Wrap the file-content repository in an encryption decorator if
        # encryption is enabled in the config. Everything downstream
        # (FileApi, WebDAV, etc.) is unchanged: the wrapper preserves the
        # IFileContentRepository contract.
        #
        # When ``encrypt_names`` is also set we derive a separate,
        # deterministic key for the metadata path names (directory and
        # file-reference names stored in the GitHub repo) and hand it to the
        # metadata backend below.
        path_name_key: Optional[bytes] = None
        metadata_blob_key: Optional[bytes] = None
        if encryption_cfg is not None and encryption_cfg.enabled:
            from tgdcfs.crypto.bootstrap import load_master_key
            from tgdcfs.crypto.metadata_blob import derive_metadata_blob_key
            from tgdcfs.crypto.repository import EncryptingFileContentRepository

            master = load_master_key(encryption_cfg)
            fc_repo = EncryptingFileContentRepository(
                fc_repo,
                master_key=master.key,
                chunk_size=encryption_cfg.chunk_size,
                encrypt_names=encryption_cfg.encrypt_names,
            )
            if encryption_cfg.encrypt_names:
                from tgdcfs.crypto.path_names import derive_path_name_key

                path_name_key = derive_path_name_key(master.key)
            metadata_blob_key = derive_metadata_blob_key(master.key)

        fd_repo = StoreFDRepository(
            store,
            mirror_group=mirror_group,
            local_versions=cache.complete if cache is not None else None,
        )

        # -- metadata ----------------------------------------------------------
        #
        # One primary metadata store plus optional redundant ones
        # (``metadata_mirrors``): the same RAID-1 shape the file stores
        # have. Every push fans out to all of them, a store that is
        # temporarily unavailable is skipped and re-synced later, and the
        # load fails over to a mirror when the primary cannot be read.
        # Without ``metadata_mirrors`` the classic single-store behaviour
        # applies unchanged -- including the pinned copies a
        # ``pinned_message`` store keeps in the mirror stores.
        metadata_cfg = filesystem.metadata
        if filesystem.metadata_mirrors:
            metadata_repo = await cls._build_replicated_metadata(
                filesystem,
                config,
                live_stores,
                store_factory,
                path_name_key,
                metadata_blob_key,
                encryption_cfg,
            )
        elif (
            metadata_cfg.type == MetadataType.PINNED_MESSAGE
            and metadata_cfg.store is not None
            and metadata_cfg.store != filesystem.primary
        ):
            # A pinned document in a store other than the primary: its own
            # content path into that store, no mirror group (the classic
            # pinned-copies-in-the-mirrors behaviour belongs to the default
            # layout and would fight over the one pinned message).
            metadata_repo = cls._build_pinned_repo(
                live_stores[metadata_cfg.store],
                encryption_cfg,
                metadata_blob_key,
                create_if_missing=True,
            )
        elif metadata_cfg.type == MetadataType.PINNED_MESSAGE:
            metadata_repo = PinnedMessageMetadataRepository(
                store, fc_repo, mirror_group=mirror_group
            )
        else:
            metadata_repo = cls._build_repo_metadata_repo(
                metadata_cfg, path_name_key, metadata_blob_key
            )

        write_back = filesystem.write_ack == "cache" and cache is not None
        fd_api = FileDescApi(
            fd_repo, fc_repo, write_ack="cache" if write_back else "primary"
        )

        metadata_api = MetaDataApi(metadata_repo)
        await metadata_api.init()

        on_written = None
        # Mirrors filled in the background put every write on the queue;
        # inline and tee writes fill the mirrors themselves and only
        # queue the files a copy is still missing from.
        if replication is not None and (mirror_group is not None or write_back):
            queue = replication
            name = filesystem.name

            async def enqueue(fr: TGFSFileRef) -> None:
                queue.enqueue(name, file_path(fr))

            on_written = enqueue

        file_api = FileApi(
            metadata_api,
            fd_api,
            store,
            mirror_group=mirror_group,
            inline_mirroring=inline,
            on_written=on_written,
            # The write itself filled the mirrors (inline or tee), so the
            # queue is only told about a file some store still lacks a
            # copy of; in background mode every write is queued.
            written_gaps_only=inline or tee,
            stat_api=FileStatApi(
                fd_api,
                validate=config.tgdcfs.listing.validate_on_list,
                metadata_api=metadata_api,
                persist=config.tgdcfs.listing.stat_in_metadata,
            ),
        )
        dir_api = DirectoryApi(metadata_api, file_api, store)

        return cls(
            name=filesystem.name,
            store=store,
            file_api=file_api,
            dir_api=dir_api,
            fc_repo=fc_repo,
            fd_repo=fd_repo,
            metadata_api=metadata_api,
            mirror_group=mirror_group,
            filesystem=filesystem,
            cache=cache,
        )

    @staticmethod
    def _build_pinned_repo(
        target: IStore,
        encryption_cfg: Optional[EncryptionConfig],
        metadata_blob_key: Optional[bytes],
        create_if_missing: bool,
    ) -> PinnedMessageMetadataRepository:
        """A pinned metadata document in one specific store.

        The content path is a private single-store repository (wrapped in
        the encryption decorator when at-rest encryption is on, exactly
        like the file content), so the document never leaks into the file
        stores' mirror groups and every pinned copy stays an independent
        metadata store.
        """
        pinned_fc: IFileContentRepository = StoreFileContentRepository(target)
        if encryption_cfg is not None and encryption_cfg.enabled:
            # The pinned document travels through the content layer, so its
            # at-rest protection comes from the content encryption; the
            # metadata blob key is for the git-based stores.
            from tgdcfs.crypto.bootstrap import load_master_key
            from tgdcfs.crypto.repository import EncryptingFileContentRepository

            pinned_fc = EncryptingFileContentRepository(
                pinned_fc,
                master_key=load_master_key(encryption_cfg).key,
                chunk_size=encryption_cfg.chunk_size,
                encrypt_names=encryption_cfg.encrypt_names,
            )
        return PinnedMessageMetadataRepository(
            target, pinned_fc, create_if_missing=create_if_missing
        )

    @staticmethod
    def _build_repo_metadata_repo(
        cfg: MetadataConfig,
        path_name_key: Optional[bytes],
        metadata_blob_key: Optional[bytes],
    ) -> IMetaDataRepository:
        """A git repository metadata store in its configured layout.

        ``tree`` (the default for both GitHub and GitLab) mirrors the tree
        into the repository's own directory structure and writes
        incrementally; ``document`` keeps the whole tree in one JSON file
        that is rewritten on every push, the pinned-message shape.
        """
        if cfg.type == MetadataType.GITHUB_REPO:
            if cfg.github_repo is None:
                raise ValueError(
                    "configuration filesystems -> metadata -> github_repo "
                    "is required."
                )
            if cfg.github_repo.layout == MetadataLayout.DOCUMENT:
                from tgdcfs.core.repository.impl.metadata.github_document import (
                    GithubDocumentMetadataRepository,
                )

                return GithubDocumentMetadataRepository(
                    cfg.github_repo, blob_key=metadata_blob_key
                )
            from tgdcfs.core.repository.impl.metadata.github_repo import (
                GithubRepoMetadataRepository,
            )

            return GithubRepoMetadataRepository(cfg.github_repo, name_key=path_name_key)
        if cfg.gitlab_repo is None:
            raise ValueError(
                "configuration filesystems -> metadata -> gitlab_repo " "is required."
            )
        if cfg.gitlab_repo.layout == MetadataLayout.DOCUMENT:
            from tgdcfs.core.repository.impl.metadata.gitlab_repo import (
                GitlabRepoMetadataRepository,
            )

            return GitlabRepoMetadataRepository(
                cfg.gitlab_repo, blob_key=metadata_blob_key
            )
        from tgdcfs.core.repository.impl.metadata.gitlab_tree import (
            GitlabTreeRepoMetadataRepository,
        )

        return GitlabTreeRepoMetadataRepository(
            cfg.gitlab_repo,
            name_key=path_name_key,
            blob_key=metadata_blob_key,
        )

    @staticmethod
    async def _build_replicated_metadata(
        filesystem: FilesystemConfig,
        config: Config,
        live_stores: Dict[str, IStore],
        store_factory: StoreFactory,
        path_name_key: Optional[bytes],
        metadata_blob_key: Optional[bytes],
        encryption_cfg: Optional[EncryptionConfig],
    ) -> IMetaDataRepository:
        """One primary plus the redundant metadata stores of a file system."""
        from tgdcfs.core.repository.impl.metadata.replicated import (
            MetadataStore,
            ReplicatedMetadataRepository,
        )

        stores: List[MetadataStore] = []
        for i, cfg in enumerate(filesystem.metadata_stores):
            if cfg.type == MetadataType.PINNED_MESSAGE:
                target_name = cfg.store or filesystem.primary
                target = live_stores.get(target_name)
                if target is None:
                    target = await store_factory(config.stores[target_name])
                repo: IMetaDataRepository = Client._build_pinned_repo(
                    target,
                    encryption_cfg,
                    metadata_blob_key,
                    # Inside the replicated layer nothing bootstraps its own
                    # tree: emptiness must be observable, or a briefly
                    # unreachable primary would look like a fresh install.
                    create_if_missing=False,
                )
            else:
                repo = Client._build_repo_metadata_repo(
                    cfg, path_name_key, metadata_blob_key
                )
            stores.append(MetadataStore(key=cfg.key, repo=repo, primary=i == 0))
        return ReplicatedMetadataRepository(stores)


Clients = Dict[str, Client]
