---
sidebar_position: 2
title: Deployment & Self-Hosting Guide
description: How to deploy VulquantaFS using Docker, Docker Compose, systemd, or bare metal.
---

# 🚀 Deployment & Self-Hosting Guide

VulquantaFS can be deployed using **Docker**, **Docker Compose**, **systemd**, or directly on bare-metal Linux.

---

## Environment Variables

VulquantaFS reads configuration and environment settings from standard environment variables:

| Variable | Default Value | Description |
|---|---|---|
| `VULQUANTAFS_DATA_DIR` | `~/.vulquantafs` (`/home/vulquantafs/.vulquantafs` inside Docker) | Path to persistent data directory (holds sessions, host keys, queue files, cache). |
| `VULQUANTAFS_CONFIG_FILE` | `config.yaml` (resolved relative to `VULQUANTAFS_DATA_DIR`) | Name of the YAML configuration file. |
| `VULQUANTAFS_EVENT_LOOP` | `asyncio` | Event loop shared by HTTP, SFTP and the storage clients. The default avoids the uvloop TLS transport path seen in bulk-upload stalls. Set to `uvloop` only to opt back into the previous loop; if uvloop is unavailable, startup falls back to `asyncio` with a warning. |

The encryption passphrase is not read from a fixed environment variable; it is resolved through the `encryption` section of `config.yaml`, in this priority order: `passphrase_env` (the *name* of an environment variable to read, e.g. `VULQUANTAFS_MASTER_PASSPHRASE`), `passphrase_file` (a file containing the passphrase, useful for systemd `LoadCredential`), or inline `passphrase` (development only). The examples below assume `passphrase_env: "VULQUANTAFS_MASTER_PASSPHRASE"` as set in the bundled `demo-config.yaml`.

*(Note: Legacy `TGFS_DATA_DIR` and `TGFS_CONFIG_FILE` environment variables are supported with a deprecation warning).*

---

## 1. Docker Deployment (Recommended)

Official Docker images are published to two registries under the publishing namespace of the repository that built them (for this repository: the `DOCKERHUB_NAMESPACE` repository variable, or the GitHub owner by default):

- **Docker Hub** (needs the `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` secrets):
  - Backend image: `<namespace>/vulquantafs:latest`
  - Frontend image: `<namespace>/vulquantafs-fe:latest`
- **GHCR** (always, with the repository's own `GITHUB_TOKEN`):
  - Backend image: `ghcr.io/<owner>/<repo>/vulquantafs:latest`
  - Frontend image: `ghcr.io/<owner>/<repo>/vulquantafs-fe:latest`

The images are identical; pull from whichever registry you prefer. The config
generator (linked under **Project Links** in the page footer) lets you pick the
registry and the container engine (Docker or Podman) in its command panel. The `docker run` and `docker compose` commands below work the same under Podman: replace `docker` with `podman` and run the compose file with `podman-compose`.

> **Note**: a GHCR package created by the workflow starts **private**. To let anyone pull it without a `docker login ghcr.io`, open the package's *Package settings* on GitHub and change its visibility to public.

### Running with `docker run`

The image ships no configuration, so mount your `config.yaml` into the data directory (see the [configuration reference](./configuration.md)):

```bash
docker run -d \
  --name vulquantafs \
  --restart unless-stopped \
  -p 1900:1900 \
  -p 2222:2222 \
  -e VULQUANTAFS_MASTER_PASSPHRASE="your-long-secret-passphrase" \
  -v ~/.vulquantafs:/home/vulquantafs/.vulquantafs \
  -v "$PWD/config.yaml:/home/vulquantafs/.vulquantafs/config.yaml:ro" \
  <namespace>/vulquantafs:latest
```

---

## 2. Docker Compose Deployment

Here is a production-ready `docker-compose.yml` including both the backend server and frontend nginx container:

```yaml
version: "3.8"

services:
  vulquantafs:
    image: <namespace>/vulquantafs:latest
    container_name: vulquantafs
    restart: unless-stopped
    ports:
      - "1900:1900"   # WebDAV & Manager API
      - "2222:2222"   # SFTP
    environment:
      - VULQUANTAFS_MASTER_PASSPHRASE=${VULQUANTAFS_MASTER_PASSPHRASE}
    volumes:
      - ./data:/home/vulquantafs/.vulquantafs
      - ./config.yaml:/home/vulquantafs/.vulquantafs/config.yaml:ro

  frontend:
    image: <namespace>/vulquantafs-fe:latest
    container_name: vulquantafs-fe
    restart: unless-stopped
    ports:
      - "8080:80"
    depends_on:
      - vulquantafs
```

Store your passphrase in a `.env` file alongside `docker-compose.yml`:
```env
VULQUANTAFS_MASTER_PASSPHRASE=your-secure-random-passphrase
```

---

## 3. systemd Service Deployment

For native Linux host deployments, use systemd credentials for safe passphrase handling.

1. Create a service user and directory:
```bash
sudo useradd -m -s /bin/bash vulquantafs
sudo mkdir -p /etc/vulquantafs ~/.vulquantafs
```

2. Store passphrase securely in `/etc/vulquantafs/master.passphrase`:
```bash
echo "your-secure-passphrase" | sudo tee /etc/vulquantafs/master.passphrase
sudo chmod 0400 /etc/vulquantafs/master.passphrase
```

3. Create systemd unit file `/etc/systemd/system/vulquantafs.service`:
```ini
[Unit]
Description=VulquantaFS Cloud Storage Bridge
After=network.target

[Service]
Type=simple
User=vulquantafs
WorkingDirectory=/home/vulquantafs
LoadCredential=master_passphrase:/etc/vulquantafs/master.passphrase
ExecStart=/home/vulquantafs/.venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

4. Point the passphrase at the credential in `$VULQUANTAFS_DATA_DIR/config.yaml`:
```yaml
encryption:
  enabled: true
  passphrase_file: /run/credentials/vulquantafs.service/master_passphrase
```

5. Enable and start service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vulquantafs
```

---

## TLS

The HTTP server (WebDAV, Manager API, `/login`) has no TLS settings. Terminate TLS in a reverse proxy in front of port `1900` if clients connect over untrusted networks. SFTP is encrypted by SSH itself.

---

## Data Directory Structure

Inside `$VULQUANTAFS_DATA_DIR` (e.g. `/home/vulquantafs/.vulquantafs`), VulquantaFS maintains:

```
.vulquantafs/
├── config.yaml               # Server configuration
├── bot.session               # Telethon Telegram bot session
├── master.salt               # 16-byte Argon2id salt (CRITICAL: DO NOT DELETE)
├── sftp_host_key             # Generated ed25519 SSH host key
├── sftp_authorized_keys/     # User public key files (when authorized_keys_dir is configured)
├── replication_queue.json    # Persistent pending replication queue
├── cache/                    # Local cache files & block maps
└── cache/index.json          # Cache index database
```

> ⚠️ **CRITICAL BACKUP NOTICE**: Always back up `config.yaml`, `master.salt`, and `sftp_host_key`. Losing `master.salt` renders encrypted files permanently unrecoverable even with the correct passphrase!
