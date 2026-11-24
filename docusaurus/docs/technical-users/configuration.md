---
sidebar_position: 3
title: Complete Configuration Reference
description: Detailed schema and reference guide for config.yaml.
---

# ⚙️ Complete Configuration Reference (`config.yaml`)

VulquantaFS is configured using a single YAML file located at `$VULQUANTAFS_DATA_DIR/config.yaml`. The `VULQUANTAFS_CONFIG_FILE` environment variable overrides the file *name*; it is resolved relative to `VULQUANTAFS_DATA_DIR`, not the working directory.

---

## Complete Example `config.yaml`

```yaml
backends:
  telegram:
    api_id: 123456
    api_hash: "0123456789abcdef0123456789abcdef"
    bot:
      session_file: bot.session
      login_timeout_s: 20  # Telethon: time limit for each bot at startup
      tokens:
        - "123456789:ABCdefGHIjklMNOpqrsTUVwxyZ"
    delete_messages_on_remove: false

  discord:
    bot_tokens:
      - "BotTokenString1"
      - "BotTokenString2"
    max_file_size_bytes: 20000000  # 20 MB attachment limit
    delete_messages_on_remove: false

stores:
  tg-primary:
    backend: telegram
    channel: "-1001234567890"

  dc-mirror:
    backend: discord
    channel: "123456789012345678"

filesystems:
  media:
    primary: tg-primary
    mirrors:
      - dc-mirror
    mode: auto           # auto | forward | reupload
    sync: background     # inline | background | tee
    strict: false
    write_ack: primary   # primary | cache
    read_parallel: true
    read_sources:
      - tg-primary
    metadata:
      type: github_repo
      github_repo:
        repo: "your-username/vulquantafs-metadata"
        commit: "master"
        access_token: "ghp_yourPersonalAccessToken"
        tree_fetch_concurrency: 8  # parallel directory reads when rebuilding a big tree
    metadata_push:
      mode: coalesced       # coalesced (default) | inline
      window_ms: 300        # how long a coalesced push groups pushes before it writes
      parallel_stores: true # write the metadata stores at the same time

vulquantafs:
  users:
    admin:
      password: "SuperSecretPassword123"
      readonly: false
    guest:
      password: "GuestPassword123"
      readonly: true

  jwt:
    secret: "change-me-to-a-long-random-string"
    algorithm: "HS256"
    life: 604800

  server:
    host: "0.0.0.0"
    port: 1900

  sftp:
    enabled: true
    host: "0.0.0.0"
    port: 2222
    host_key_file: "sftp_host_key"
    # authorized_keys_dir: "sftp_authorized_keys"
    # upload_buffer_size_mb: 64
    # upload_buffer_dir: ""
    # max_ahead_gap_mb: 1024
    # fast_readdir_min_files: 256
    # close_timeout_s: 18

  encryption:
    enabled: true
    encrypt_names: true
    passphrase_env: "VULQUANTAFS_MASTER_PASSPHRASE"
    master_salt_file: "master.salt"
    chunk_size: 65536

  cache:
    enabled: true
    dir: "cache"
    max_size_mb: 20480
    stage_uploads: true
    keep_for_reads: true
    min_free_mb: 1024

  transfer:
    upload_workers_small: 3
    upload_workers_big: 8
    upload_part_size_kb: 512
    download_piece_size_kb: 4096
    download_pieces_in_flight: 4
```

---

## Configuration Section Details

### 1. `backends` Section
Configures authentication tokens and connection parameters for Telegram and Discord:

The `telegram` block can be left out for a Discord-only setup (no store may then use the `telegram` backend). Singular `bot.token` and `discord.bot_token` are accepted next to the lists.

- **`telegram`**:
  - `api_id` *(int)*: Telegram API App ID from [my.telegram.org](https://my.telegram.org).
  - `api_hash` *(string)*: Telegram API App Hash.
  - `bot.tokens` *(list of strings)*: Telegram Bot tokens from [@BotFather](https://t.me/BotFather). Providing multiple tokens distributes download/upload operations across tokens.
  - `bot.login_timeout_s` *(number, default `20`)*: Telethon login deadline for each bot, including the first Telegram RPC. A bot that times out or cannot connect is logged and left out until restart; at least one bot must log in. This setting does not apply to Pyrogram or interactive user logins.
  - `bot.session_file` *(string)*: Telethon session file for the bot, relative to `VULQUANTAFS_DATA_DIR` (default: `bot.session`).
  - `lib` *(pyrogram | telethon)*: MTProto client library used by the Telegram backend (default: `telethon`).
  - `account` *(optional object)*: A regular Telegram user account in addition to the bots, with `session_file` (relative to `VULQUANTAFS_DATA_DIR`), `used_to_upload` and `used_to_download` flags. Parts above the bot limit (up to 4,194,304,000 bytes) are only uploaded when this account has `used_to_upload: true` and Telegram Premium.
  - `delete_messages_on_remove` *(bool, default `false`)*: Delete the channel messages when a file is removed from the file system; without it messages are kept (best-effort deletion is gated on this flag).
- **`discord`**:
  - `bot_tokens` *(list of strings)*: Discord Bot tokens from the Discord Developer Portal.
  - `max_file_size_bytes` *(int)*: Maximum attachment size per uploaded message (default: `10000000` = 10 MB). Adjust based on your Discord server boost tier (Level 0: 20 MB, Level 2: 50 MB, Level 3: 100 MB).
  - `delete_messages_on_remove` *(bool, default `false`)*: Delete the channel messages when a file is removed from the file system.
  - `upload_max_retries` *(int, default `10`)*: Retries per failed attachment upload.
  - `upload_retry_interval` *(float, default `5.0`)*: Seconds between upload retries.
  - `max_concurrent_uploads` *(int, default `max(3, number of bot tokens)`)*: Concurrent attachment uploads across the bot pool. An upload whose size is not known in advance (such as SFTP) uses at most two slots per file, leaving fewer attachments to confirm after the client closes it.
  - `max_concurrent_downloads` *(int, default `3`)*: Concurrent attachment downloads across the bot pool.

---

### 2. `stores` Section
Defines logical store names mapped to channels:
- `backend` *(telegram | discord)*: Backend driver name.
- `channel` *(string)*: Channel ID (e.g. `-1001234567890` for Telegram or `123456789012345678` for Discord).

---

### 3. `filesystems` Section
Defines top-level virtual directories exposed via WebDAV/SFTP:
- `primary` *(string)*: Name of primary store defined in `stores`.
- `mirrors` *(list of strings)*: Names of mirror stores.
- `mode` *(auto | forward | reupload)*:
  - `auto`: Uses zero-bandwidth server-side copy (forwarding) when possible, falls back to reuploading.
  - `forward`: Requires server-side forwarding (Telegram to Telegram only).
  - `reupload`: Re-downloads and streams uploaded bytes to mirror store.
- `sync` *(inline | background | tee)*: when omitted it is `background` if some mirror has to re-upload (another backend, or any Discord store involved) and `inline` otherwise (Telegram-to-Telegram forwarding).
  - `inline`: Writes wait for primary and mirror operations to finish before responding to client.
  - `background`: Responds after primary write completes; queues mirror operations to background queue.
  - `tee`: Uploads to primary and reuploading mirrors simultaneously from incoming byte stream; mirrors that can be filled by server-side forwarding are still copied inline.
- `strict` *(bool, default `false`)*: If `true`, fails the client write if any mirror write fails. Requires `sync: inline`; the loader refuses it with any other `sync`.
- `write_ack` *(primary | cache)*:
  - `primary`: Client receives 201 Created after primary channel write finishes.
  - `cache`: Client receives 201 Created as soon as upload lands in local cache disk directory. Requires `vulquantafs.cache.enabled: true` and cannot be combined with `sync: tee` or `strict: true`.
- `read_preference` *(list of strings)*: Order in which stores are tried for reads when `read_parallel` is off. Every name must be the file system's primary or one of its mirrors.
- `allow_shared_store` *(bool, default `false`)*: Allow a store that is the primary of one file system to also be a mirror of another. A store can never be the primary of two file systems, with or without this flag.
- `metadata` *(object)*: `type` is `pinned_message` (default) or `github_repo` (which then requires a `github_repo` block). A `github_repo` block takes `tree_fetch_concurrency` (int, default `8`), a `gitlab_repo` block `blob_fetch_concurrency` (int, default `8`): how many blobs a full-tree sync reads in parallel. A git-backed store (tree layout) also takes `squash_history_max_commits` (int, default `0`): once the branch's commit history grows past this many commits, the periodic heal rewrites it into a single commit that carries the branch's whole current state. Git backends keep every deleted blob alive for as long as a commit references it, so an unbounded history grows the repository's storage use forever; the squash drops those old references so the backend's garbage collection can reclaim them. `0` keeps every commit. Works for both a GitHub and a GitLab store, primary or mirror, and with a single primary store as only the metadata store.
- `metadata_push` *(object)*: How a mutation reaches the metadata stores.
  - `mode` *(coalesced, default | inline)*: `coalesced` groups overlapping metadata writes during `window_ms`; missed writes are retried by the heal worker. For SFTP file removal with Git-backed or explicitly replicated metadata, when no file backend physically deletes messages, VulquantaFS instead acknowledges each removal once it is journaled in the local data directory. It syncs those removals to the metadata stores in batches and replays unflushed removals after restart. Keep that data directory on a persistent volume. Deletions have not reached another server or volume until the stores sync. `inline` waits for a remote metadata push and disables new journaled acknowledgements. Physical deletion settings are unchanged.
  - `window_ms` *(int, default `300`)*: How long a coalesced push groups requests before it writes; `0` pushes right away, still merging operations that run at the same moment.
  - `parallel_stores` *(bool, default `true`)*: Write the primary and the mirrors at the same time; the write lasts as long as the slowest store instead of the sum of all of them.

---

### 4. `vulquantafs` Section
Main application parameters:
- **`users`** and **`server`** are required keys (like `jwt`); `users` may be an empty mapping.
- **`users`**: User accounts for WebDAV and SFTP authentication.
  - `readonly: true`: Users can list and download, but upload/delete operations return permission denied.
  - When `users` is empty, every login is served as a read-only `anonymous` user. SFTP needs no credentials then; HTTP/WebDAV still requires an `Authorization` header, but accepts any credentials in it.
- **`jwt`** *(required)*: Bearer-token settings used by the Manager API, the Telegram Mini App and WebDAV clients that log in via `POST /login` (see [Management API](./management-api.md)). This section has no default; the config loader refuses to start without it.
  - `secret` *(string)*: Secret used to sign the tokens. Use a long random string.
  - `algorithm` *(string)*: JWT signing algorithm (e.g. `HS256`).
  - `life` *(int)*: Token lifetime in seconds (`604800` = 7 days in `demo-config.yaml`).
- **`server`**: Host and port of the HTTP server that serves WebDAV (`/webdav`), the Manager API (`/api`) and `POST /login`.
- **`sftp`**: Optional SFTP interface configuration.
  - `enabled` *(bool, default `false`)*, `host` *(default `0.0.0.0`)*, `port` *(default `2222`)*, `host_key_file` *(default `sftp_host_key`; ed25519, mode `0600`, created on first start)*.
  - `authorized_keys_dir` *(optional)*: Directory with one `<username>` file per user, in the usual `authorized_keys` format, enabling public key login. The user must still exist under `users`, so the `readonly` flag keeps applying.
  - `upload_buffer_size_mb` *(int, default `64`)*: How much of an incoming upload may sit ahead of the streamed position in memory before it spills over to `upload_buffer_dir`. Uploads stream straight into the cloud channel; the buffer only absorbs writes that run ahead.
  - `upload_buffer_dir` *(optional)*: Spill directory for the upload buffer (the system temp directory when empty).
  - `max_ahead_gap_mb` *(int, default `1024`)*: How far ahead of the streamed position a single write may jump before the upload fails. Within that bound writes may arrive in any order; a gap no write ever fills is zero-filled when the client closes the file.
  - `fast_readdir_min_files` *(int, default `256`)*: In directories with at least this many files, SFTP lists names and types without fetching each file's missing size and timestamps from storage. Known values are still sent; `STAT`/`LSTAT` can fetch the exact values. Set to `0` for clients that require exact attributes in every listing.
  - `close_timeout_s` *(number, default `18`)*: Maximum wait for an upload's SFTP `CLOSE` response, including storage and metadata confirmation. A timeout returns an SFTP error rather than withholding a response until the client disconnects. Set to `0` to disable the limit; if you raise it, set the client's inactivity timeout higher.
- **`encryption`**: Client-side AES-256-GCM settings (see [Security & At-Rest Encryption](./encryption.md)).
- **`cache`** (off unless `enabled: true`): Local disk cache configuration (see [Local Cache & Performance Tuning](./cache-and-performance.md)).
- **`listing`**: Folder listing behaviour (see [Local Cache & Performance Tuning](./cache-and-performance.md)).
- **`transfer`**: Fine-tuning piece size, concurrency workers, and buffers. Every value has a default, so the section can be left out entirely:
  - `upload_workers_small` *(int, default `3`)* / `upload_workers_big` *(int, default `8`)*: Upload workers for files below/above 10 MB.
  - `upload_part_size_kb` *(int, default `512`)*: Telegram part size; must divide 512 and not exceed it.
  - `download_piece_size_kb` *(int, default `4096`)* and `download_pieces_in_flight` *(int, default `4`)*: Read piece size and concurrency; peak buffering per download is their product.
  - `parallel_download_threshold_mb` *(int, default `10`)*: Minimum file size to trigger multi-source reads.
  - `upload_parts_in_flight` *(int, default `1`)*: Parallel part uploads when the source is seekable (a version in the local cache).
  - `read_parallel_window` *(int, default `8`)*: Piece reorder window for multi-source reads.
  - `connection_pool_size` *(int, default `1`)*: MTProto connections each bot opens.
  - `chunk_cache_mb` *(int, default `0` = off)*, `chunk_cache_readahead` *(int, default `2`)*, `chunk_cache_block_kb` *(int, default `1024`)*: In-memory cache for downloaded bytes.
  - `tee_buffer_mb` *(int, default `64`)*: MiB buffered per store during `sync: tee` writes. Uploads whose size is not known in advance use at most 1 MiB per branch, so less of the transfer is still queued after the last client write.

---

### Config written for tgfs or tgdcfs

A `config.yaml` in the tgfs layout is translated on load: the top-level `tgfs:` block is read as `vulquantafs:` (deprecation warning), `telegram:` as `backends.telegram`, `telegram.private_file_channel` becomes the primary stores, `tgfs.metadata.<channel>` names each file system and `telegram.redundancy.mirrors` lists its mirrors. This layout cannot be combined with `stores`/`filesystems`.

A config written for tgdcfs keeps its `tgdcfs:` block (deprecation warning) and loads exactly like one named `vulquantafs:`, in both layouts. Encrypted metadata documents (blobs) written by tgdcfs keep opening after the upgrade; new blobs are written with the current derivation.
