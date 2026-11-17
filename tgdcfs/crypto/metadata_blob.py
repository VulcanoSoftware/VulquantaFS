"""Encryption for a metadata document stored outside the file stores.

The pinned-message store keeps ``metadata.json`` as an ordinary file
message, so at-rest encryption comes for free from the content layer.
A git-based store (``gitlab_repo``) writes the document itself, and a
plain JSON tree in a repository would expose every file and directory
name to anyone who can read the repo. When encryption is enabled, the
blob is therefore sealed with AES-256-GCM under a key derived from the
master key, domain-separated from the other sub-keys.

Wire format::

    "TGDCFSMETAV1_" || base64url( nonce(12) || AES-GCM ciphertext )

The prefix lets a reader tell an encrypted blob from a plaintext one, so
a store written before encryption was switched on keeps loading.
"""

from __future__ import annotations

import base64

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

# Distinct HKDF info so this key cannot collide with the per-file content
# keys, the path-name key or the document-name key.
_HKDF_INFO = b"tgdcfs-metadata-blob-key-v1"

# Tags a git blob as an encrypted TGDCFS metadata document.
ENCRYPTED_BLOB_PREFIX = "TGDCFSMETAV1_"

_NONCE_BYTES = 12


class MetadataBlobError(ValueError):
    """Raised when an encrypted metadata blob cannot be decoded."""


def derive_metadata_blob_key(master_key: bytes) -> bytes:
    """Derive the 32-byte AES-256-GCM key for metadata blobs."""
    if len(master_key) != 32:
        raise ValueError(f"master key must be 32 bytes, got {len(master_key)}")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"",
        info=_HKDF_INFO,
    )
    return hkdf.derive(master_key)


def is_encrypted_metadata_blob(text: str) -> bool:
    return text.startswith(ENCRYPTED_BLOB_PREFIX)


def encrypt_metadata_blob(key: bytes, plaintext: bytes) -> str:
    """Seal ``plaintext``; returns the text a git blob carries."""
    import os

    nonce = os.urandom(_NONCE_BYTES)
    sealed = nonce + AESGCM(key).encrypt(nonce, plaintext, None)
    return ENCRYPTED_BLOB_PREFIX + base64.urlsafe_b64encode(sealed).decode()


def decrypt_metadata_blob(key: bytes, text: str) -> bytes:
    """Open a blob produced by :func:`encrypt_metadata_blob`."""
    if not is_encrypted_metadata_blob(text):
        # A plaintext document written while encryption was off.
        return text.encode()
    try:
        sealed = base64.urlsafe_b64decode(text[len(ENCRYPTED_BLOB_PREFIX) :].encode())
        if len(sealed) < _NONCE_BYTES + 16:
            raise ValueError("blob too short")
        nonce, ciphertext = sealed[:_NONCE_BYTES], sealed[_NONCE_BYTES:]
        return AESGCM(key).decrypt(nonce, ciphertext, None)
    except (ValueError, InvalidTag) as ex:
        raise MetadataBlobError(f"Could not decrypt the metadata blob: {ex}") from ex
