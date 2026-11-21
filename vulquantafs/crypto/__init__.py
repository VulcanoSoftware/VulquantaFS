"""Transparent at-rest encryption layer for VulquantaFS.

This package provides a chunked, authenticated stream cipher that sits between
the high-level file API and the underlying Telegram storage backend. When
enabled, all file content is encrypted client-side before being uploaded to
Telegram channels, so that compromise of the channel or the metadata store
does not expose plaintext data.

Design summary:
  * Cipher:         AES-256-GCM (authenticated, hardware accelerated via AES-NI)
  * Chunk size:     64 KiB (configurable) -- enables random-access decryption
  * Per-file key:   HKDF-SHA256(master_key, salt=file_salt, info="tgfs-file-v1")
  * Master key:     Argon2id(passphrase, salt=master_salt) -- derived once on startup
  * File header:    60 bytes, inline at the start of the first Telegram part,
                    so a file is self-describing and can be recovered even if the
                    VulquantaFS metadata store is lost.

The encryption layer is implemented as a decorator around
``IFileContentRepository`` -- the rest of VulquantaFS does not need to know whether
encryption is active.

Note: the heavyweight decorator class
``EncryptingFileContentRepository`` is *not* eagerly imported here, because
pulling it in would transitively load the Telegram backend modules. Import
it explicitly when needed:

    from vulquantafs.crypto.repository import EncryptingFileContentRepository
"""

from vulquantafs.crypto.cipher import ChunkedAESGCM
from vulquantafs.crypto.header import FileHeader
from vulquantafs.crypto.kdf import derive_file_key, derive_master_key
from vulquantafs.crypto.metadata_blob import (
    decrypt_metadata_blob,
    derive_metadata_blob_key,
    encrypt_metadata_blob,
)
from vulquantafs.crypto.names import decrypt_name, derive_name_key, encrypt_name

__all__ = [
    "ChunkedAESGCM",
    "FileHeader",
    "decrypt_metadata_blob",
    "decrypt_name",
    "derive_file_key",
    "derive_master_key",
    "derive_metadata_blob_key",
    "derive_name_key",
    "encrypt_metadata_blob",
    "encrypt_name",
]
