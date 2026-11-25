"""Durable local outbox for SFTP references whose content already exists."""

import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable

from vulquantafs.core.model import TGFSFileRef
from vulquantafs.crypto.metadata_blob import (
    decrypt_metadata_blob,
    encrypt_metadata_blob,
)


@dataclass
class UploadRecord:
    id: int
    parent: str
    name: str
    message_id: int
    mirrors: dict[str, int]
    store: str | None


class UploadJournal:
    def __init__(self, path: str, key: bytes | None):
        self._path = path
        self._key = key
        self._db: sqlite3.Connection | None = None

    def _connection(self) -> sqlite3.Connection:
        if self._db is None:
            os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
            fd = os.open(self._path, os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(fd)
            db = sqlite3.connect(self._path)
            try:
                # In rollback-journal mode EXTRA also syncs the parent
                # directory after removing the transaction journal.
                db.execute("PRAGMA synchronous=EXTRA")
                db.execute(
                    "CREATE TABLE IF NOT EXISTS uploads ("
                    "id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
                )
            except BaseException:
                db.close()
                raise
            self._db = db
        return self._db

    def add(self, ref: TGFSFileRef) -> int:
        payload = json.dumps(
            [
                ref.location.absolute_path,
                ref.name,
                ref.message_id,
                ref.mirrors,
                ref.store,
            ],
            separators=(",", ":"),
        )
        if self._key is not None:
            payload = encrypt_metadata_blob(self._key, payload.encode())
        db = self._connection()
        with db:
            conn = db.execute("INSERT INTO uploads(payload) VALUES (?)", (payload,))
            if conn.lastrowid is None:
                raise sqlite3.DatabaseError("The upload journal did not record an ID")
            return conn.lastrowid

    def load(self) -> list[UploadRecord]:
        if not os.path.exists(self._path):
            return []
        records = []
        for row_id, payload in self._connection().execute(
            "SELECT id, payload FROM uploads ORDER BY id"
        ):
            if self._key is not None:
                payload = decrypt_metadata_blob(self._key, payload)
            records.append(UploadRecord(row_id, *json.loads(payload)))
        return records

    def clear(self, ids: Iterable[int]) -> None:
        db = self._connection()
        with db:
            db.executemany("DELETE FROM uploads WHERE id = ?", ((id_,) for id_ in ids))

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
