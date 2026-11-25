"""Local write-ahead record for metadata-only SFTP REMOVE requests."""

import hashlib
import hmac
import json
import os
import sqlite3

from vulquantafs.core.model import TGFSFileRef


class RemovalJournal:
    def __init__(self, path: str, key: bytes | None, default_store: str | None = None):
        self._path = path
        self._key = key
        self._default_store = default_store
        self._db: sqlite3.Connection | None = None

    def _connection(self) -> sqlite3.Connection:
        if self._db is None:
            # The acknowledgement must survive an unclean exit. Keep the
            # default rollback journal and require synchronous commits.
            os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
            fd = os.open(self._path, os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(fd)
            db = sqlite3.connect(self._path)
            try:
                db.execute("PRAGMA synchronous=FULL")
                db.execute(
                    "CREATE TABLE IF NOT EXISTS removes ("
                    "id INTEGER PRIMARY KEY, fingerprint BLOB NOT NULL)"
                )
            except BaseException:
                db.close()
                raise
            self._db = db
        return self._db

    def fingerprint(self, ref: TGFSFileRef) -> bytes:
        data = json.dumps(
            [
                ref.location.absolute_path,
                ref.name,
                ref.message_id,
                ref.store or self._default_store,
            ],
            separators=(",", ":"),
        ).encode()
        # Do not store clear-text paths, including for encrypted metadata.
        return (
            hmac.digest(self._key, data, "sha256")
            if self._key is not None
            else hashlib.sha256(data).digest()
        )

    def add(self, ref: TGFSFileRef) -> int:
        db = self._connection()
        with db:
            row = db.execute(
                "INSERT INTO removes(fingerprint) VALUES (?)",
                (self.fingerprint(ref),),
            )
            last_id = row.lastrowid
            if last_id is None:
                raise sqlite3.DatabaseError("The removal journal did not record an ID")
        return last_id

    def load(self) -> tuple[int, set[bytes]]:
        if not os.path.exists(self._path):
            return 0, set()
        rows = list(self._connection().execute("SELECT id, fingerprint FROM removes"))
        return max((i for i, _ in rows), default=0), {key for _, key in rows}

    def clear_through(self, last_id: int) -> None:
        db = self._connection()
        with db:
            db.execute("DELETE FROM removes WHERE id <= ?", (last_id,))

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
