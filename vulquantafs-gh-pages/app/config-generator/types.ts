// Shared shapes of the config generator's form state.

import { EncryptionConfig } from "./components/EncryptionField";

export type Backend = "telegram" | "discord";

export interface StoreConfig {
  // Name used inside the config only; never reaches the metadata.
  name: string;
  backend: Backend;
  channel: string;
}

export type MetadataType = "pinned_message" | "github_repo" | "gitlab_repo";
// How a repository metadata store (GitHub or GitLab) keeps the tree:
// mirrored into the repository's own directory structure, or as one JSON
// document that is rewritten on every push (the pinned-message shape).
export type MetadataLayout = "tree" | "document";
export type MirrorMode = "auto" | "forward" | "reupload";
export type SyncMode = "inline" | "background" | "tee";
// When a write is answered: once the primary store has it, or once the
// local cache has it and a worker moves it into the stores (write-back).
export type WriteAck = "primary" | "cache";
// How a mutation reaches the metadata stores: coalesced merges the push
// requests of a burst (a folder deleted file by file) into one store
// write per window; inline hands every push straight to the stores.
export type MetadataPushMode = "coalesced" | "inline";

// How metadata mutations reach the metadata stores.
export interface MetadataPushConfig {
  mode: MetadataPushMode;
  // coalesced only: pushes that arrive within this many milliseconds
  // share one store write. 0 pushes at once, still merging concurrents.
  window_ms: number;
  // Write the primary and the mirrors at the same time instead of one
  // store after another: the write lasts as long as the slowest store.
  parallel_stores: boolean;
}

// The loader's defaults; a value equal to its default is left out of the
// YAML so the server's default keeps applying.
export const METADATA_PUSH_DEFAULTS: MetadataPushConfig = {
  mode: "coalesced",
  window_ms: 300,
  parallel_stores: true,
};

// One metadata store: the primary one or an entry of metadata_mirrors.
// Every field is kept on the form so switching the type never loses what
// was typed; only the fields of the chosen type reach the YAML.
export interface MetadataEntry {
  type: MetadataType;
  // pinned_message only: the store whose channel holds the pinned
  // document; empty means the file system's primary store. A pinned copy
  // in a Telegram store and one in a Discord store are two separate
  // metadata stores this way.
  store: string;
  github_repo: {
    repo: string;
    commit: string;
    access_token: string;
    layout: MetadataLayout;
    // document layout only: where the JSON document lives. The tree
    // layout uses it to find (and remove) a document left by an older
    // release or by the document layout.
    file_path: string;
    // Tree layout only: compact the commit history once it grows past
    // this many commits; 0 keeps it forever.
    squash_history_max_commits: number;
  };
  gitlab_repo: {
    repo: string;
    branch: string;
    access_token: string;
    base_url: string;
    file_path: string;
    layout: MetadataLayout;
    // Tree layout only: how many blobs a full-tree sync reads in
    // parallel; more is faster but bursts harder against the rate limit.
    blob_fetch_concurrency: number;
    // Tree layout only: compact the commit history once it grows past
    // this many commits; 0 keeps it forever.
    squash_history_max_commits: number;
  };
}

// The loader's defaults; base_url and file_path are left out of the YAML
// when they equal them.
export const GITLAB_DEFAULTS = {
  base_url: "https://gitlab.com",
  file_path: "metadata.json",
  blob_fetch_concurrency: 8,
};

export const newMetadataEntry = (): MetadataEntry => ({
  type: "pinned_message",
  store: "",
  github_repo: {
    repo: "",
    commit: "master",
    access_token: "",
    layout: "tree",
    file_path: GITLAB_DEFAULTS.file_path,
    squash_history_max_commits: 0,
  },
  gitlab_repo: {
    repo: "",
    branch: "master",
    access_token: "",
    base_url: GITLAB_DEFAULTS.base_url,
    file_path: GITLAB_DEFAULTS.file_path,
    layout: "tree",
    blob_fetch_concurrency: GITLAB_DEFAULTS.blob_fetch_concurrency,
    squash_history_max_commits: 0,
  },
});

export interface FilesystemConfig {
  name: string;
  primary: string;
  mirrors: string[];
  mode: MirrorMode;
  sync: SyncMode;
  strict: boolean;
  write_ack: WriteAck;
  // Multi-source reads: spread one download over every store that holds
  // the version; read_sources limits the stores that take part.
  read_parallel: boolean;
  read_sources: string[];
  allow_shared_store: boolean;
  // The metadata stores form a redundancy set of their own: the primary
  // metadata store plus every mirror holds a full copy of the directory
  // tree, and a read fails over to the next one when a store fails.
  metadata: MetadataEntry;
  metadata_mirrors: MetadataEntry[];
  // How mutations reach those stores: coalesced (default), inline, and
  // whether the stores are written in parallel.
  metadata_push: MetadataPushConfig;
}

export interface DiscordConfig {
  enabled: boolean;
  bot_tokens: string[];
  max_file_size_bytes: number;
  delete_messages_on_remove: boolean;
}

export interface UserConfig {
  username: string;
  password: string;
  readonly: boolean;
}

export interface SftpConfig {
  enabled: boolean;
  host: string;
  port: number;
  host_key_file: string;
  authorized_keys_dir: string;
  upload_buffer_size_mb: number;
}

export interface CacheConfig {
  enabled: boolean;
  dir: string;
  max_size_mb: number;
  max_files: number;
  max_file_size_mb: number;
  block_kb: number;
  stage_uploads: boolean;
  keep_for_reads: boolean;
  min_free_mb: number;
  max_age_hours: number;
  target_fill_percent: number;
}

// The loader's defaults; a value equal to its default is left out of the
// YAML so the server's default keeps applying.
export const CACHE_DEFAULTS: CacheConfig = {
  enabled: false,
  dir: "cache",
  max_size_mb: 20480,
  max_files: 0,
  max_file_size_mb: 4096,
  block_kb: 4096,
  stage_uploads: true,
  keep_for_reads: true,
  min_free_mb: 1024,
  max_age_hours: 0,
  target_fill_percent: 90,
};

export interface ListingConfig {
  stat_in_metadata: boolean;
  validate_on_list: boolean;
  message_cache_size: number;
}

// The loader's defaults; the block is written only with the values that
// differ from them.
export const LISTING_DEFAULTS: ListingConfig = {
  stat_in_metadata: false,
  validate_on_list: true,
  message_cache_size: 1024,
};

export interface TransferConfig {
  // UI only: when off, no transfer block is written at all and the
  // application falls back to its own defaults.
  enabled: boolean;
  upload_workers_small: number;
  upload_workers_big: number;
  upload_part_size_kb: number;
  download_piece_size_kb: number;
  download_pieces_in_flight: number;
  parallel_download_threshold_mb: number;
  connection_pool_size: number;
  chunk_cache_mb: number;
  chunk_cache_readahead: number;
  chunk_cache_block_kb: number;
  // sync: tee buffers this much per mirror between the primary's reads
  // of the upload stream and the mirror's upload.
  tee_buffer_mb: number;
}

export interface ConfigData {
  telegram: {
    // UI only: off leaves the whole backends.telegram block out.
    enabled: boolean;
    api_id: string;
    api_hash: string;
    lib: "pyrogram" | "telethon";
    account: {
      session_file: string;
    };
    bot: {
      session_file: string;
      tokens: string[];
    };
  };
  discord: DiscordConfig;
  stores: StoreConfig[];
  filesystems: FilesystemConfig[];
  vulquantafs: {
    users: UserConfig[];
    jwt: {
      secret: string;
      algorithm: string;
      life: number;
    };
    server: {
      host: string;
      port: number;
    };
    sftp: SftpConfig;
    transfer: TransferConfig;
    encryption: EncryptionConfig;
    cache: CacheConfig;
    listing: ListingConfig;
  };
}

export const newFilesystem = (
  name: string,
  primary: string
): FilesystemConfig => ({
  name,
  primary,
  mirrors: [],
  mode: "auto",
  sync: "inline",
  strict: false,
  write_ack: "primary",
  read_parallel: false,
  read_sources: [],
  allow_shared_store: false,
  metadata: newMetadataEntry(),
  metadata_mirrors: [],
  metadata_push: { ...METADATA_PUSH_DEFAULTS },
});

// The form as it opens: one Telegram store, one file system on it, and
// the loader's defaults everywhere else. A fresh object every time, so a
// loaded config never shares nested state with the initial one.
export const defaultConfig = (): ConfigData => ({
  telegram: {
    enabled: true,
    api_id: "",
    api_hash: "",
    lib: "telethon",
    account: {
      session_file: "account.session",
    },
    bot: {
      session_file: "bot.session",
      tokens: [""],
    },
  },
  discord: {
    enabled: false,
    bot_tokens: [""],
    max_file_size_bytes: 10000000,
    delete_messages_on_remove: false,
  },
  stores: [{ name: "tg-main", backend: "telegram", channel: "" }],
  filesystems: [newFilesystem("default", "tg-main")],
  vulquantafs: {
    users: [
      {
        username: "user",
        password: "password",
        readonly: false,
      },
    ],
    jwt: {
      secret: "",
      algorithm: "HS256",
      life: 604800,
    },
    server: {
      host: "0.0.0.0",
      port: 1900,
    },
    sftp: {
      enabled: false,
      host: "0.0.0.0",
      port: 2222,
      host_key_file: "sftp_host_key",
      authorized_keys_dir: "",
      upload_buffer_size_mb: 64,
    },
    cache: { ...CACHE_DEFAULTS },
    listing: { ...LISTING_DEFAULTS },
    transfer: {
      enabled: false,
      upload_workers_small: 3,
      upload_workers_big: 8,
      upload_part_size_kb: 512,
      download_piece_size_kb: 4096,
      download_pieces_in_flight: 4,
      parallel_download_threshold_mb: 10,
      connection_pool_size: 1,
      chunk_cache_mb: 0,
      chunk_cache_readahead: 2,
      chunk_cache_block_kb: 1024,
      tee_buffer_mb: 64,
    },
    encryption: {
      enabled: false,
      encrypt_names: false,
      passphrase_source: "passphrase_env",
      passphrase: "",
      passphrase_env: "VULQUANTAFS_MASTER_PASSPHRASE",
      passphrase_file: "secrets/master.passphrase",
      master_salt_file: "master.salt",
      chunk_size: 65536,
    },
  },
});

export const isValidDirectoryName = (name: string): boolean => {
  // Valid directory name: no / \ : * ? " < > | and not . or ..
  const invalidChars = /[\/\\:*?"<>|]/;
  return (
    !invalidChars.test(name) &&
    name !== "." &&
    name !== ".." &&
    name.trim().length > 0
  );
};

export const isValidStoreName = (name: string): boolean =>
  /^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(name);

// Whether some mirror of the file system can only be filled by
// re-uploading the bytes: another backend, Discord on either side (no
// server-side copy) or an explicit re-upload mode. Mirrors the loader's
// rule for the default sync mode.
export const needsReupload = (
  filesystem: FilesystemConfig,
  stores: StoreConfig[]
): boolean => {
  if (filesystem.mirrors.length === 0) return false;
  if (filesystem.mode === "reupload") return true;
  const primary = stores.find((s) => s.name === filesystem.primary);
  if (!primary) return false;
  return filesystem.mirrors.some((name) => {
    const store = stores.find((s) => s.name === name);
    return (
      store !== undefined &&
      (store.backend === "discord" || store.backend !== primary.backend)
    );
  });
};
