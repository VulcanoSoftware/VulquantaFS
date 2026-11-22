"""Opening metadata blobs written before the renames keeps working.

tgdcfs sealed its ``TGDCFSMETAV1_`` blobs with a key derived from the
pre-rebrand HKDF info string, so the prefix check alone would silently
open them with the wrong key. The decryptor therefore takes a legacy key
alongside the current one.
"""

import base64
import os

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vulquantafs.crypto.metadata_blob import (
    ENCRYPTED_BLOB_PREFIX,
    LEGACY_ENCRYPTED_BLOB_PREFIX,
    MetadataBlobError,
    decrypt_metadata_blob,
    derive_legacy_metadata_blob_key,
    derive_metadata_blob_key,
    encrypt_metadata_blob,
)

MASTER_KEY = b"k" * 32
DOCUMENT = b'{"dir": {"name": "root"}}'


def seal_with(prefix: str, key: bytes, plaintext: bytes) -> str:
    nonce = os.urandom(12)
    sealed = nonce + AESGCM(key).encrypt(nonce, plaintext, None)
    return prefix + base64.urlsafe_b64encode(sealed).decode()


def test_a_blob_from_the_current_derivation_opens_with_the_current_key():
    text = encrypt_metadata_blob(derive_metadata_blob_key(MASTER_KEY), DOCUMENT)

    assert decrypt_metadata_blob(derive_metadata_blob_key(MASTER_KEY), text) == DOCUMENT


def test_a_tgdcfs_blob_opens_with_the_legacy_key():
    text = seal_with(
        LEGACY_ENCRYPTED_BLOB_PREFIX,
        derive_legacy_metadata_blob_key(MASTER_KEY),
        DOCUMENT,
    )

    opened = decrypt_metadata_blob(
        derive_metadata_blob_key(MASTER_KEY),
        text,
        legacy_key=derive_legacy_metadata_blob_key(MASTER_KEY),
    )

    assert opened == DOCUMENT


def test_a_tgdcfs_blob_does_not_open_with_only_the_current_key():
    text = seal_with(
        LEGACY_ENCRYPTED_BLOB_PREFIX,
        derive_legacy_metadata_blob_key(MASTER_KEY),
        DOCUMENT,
    )

    with pytest.raises(MetadataBlobError, match="legacy blob key"):
        decrypt_metadata_blob(derive_metadata_blob_key(MASTER_KEY), text)


def test_a_plaintext_document_is_returned_as_it_was_written():
    assert (
        decrypt_metadata_blob(derive_metadata_blob_key(MASTER_KEY), "plain") == b"plain"
    )


def test_the_prefixes_stay_distinct():
    assert ENCRYPTED_BLOB_PREFIX != LEGACY_ENCRYPTED_BLOB_PREFIX
    assert derive_metadata_blob_key(MASTER_KEY) != derive_legacy_metadata_blob_key(
        MASTER_KEY
    )
