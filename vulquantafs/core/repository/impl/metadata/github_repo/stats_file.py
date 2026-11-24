import base64
import gzip
import io
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vulquantafs.core.model.stat import FileStatSerialized

# Per-folder file holding the size and dates of the folder's files
# (``listing.stat_in_metadata``), keyed by descriptor message id.
STATS_FILE_NAME = ".vulquantafsstats.json"

# Every commit that only writes stats files starts with this. The folder
# dates come from the git history and must not move when one is written.
STATS_COMMIT_PREFIX = "Update file stats"

# These files are caches. Leave small folders in the legacy format; on
# large ones the JSON string in a GitHub tree request can be hundreds of
# kilobytes and time out while holding the file-reference writer lock.
_COMPRESS_MIN_BYTES = 64 * 1024
_COMPRESSED_KEY = "gzip-v1"
_MAX_COMPRESSED_STATS_OUTPUT = 100 * 1024 * 1024


def encode_stats(entries: dict[str, "FileStatSerialized"]) -> str:
    plain = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    if not _COMPRESS_MIN_BYTES <= len(plain) <= _MAX_COMPRESSED_STATS_OUTPUT:
        return plain
    packed = base64.b64encode(
        gzip.compress(plain.encode(), compresslevel=1, mtime=0)
    ).decode("ascii")
    compact = json.dumps({_COMPRESSED_KEY: packed}, separators=(",", ":"))
    return compact if len(compact) < len(plain) else plain


def decode_stats(content: bytes) -> dict[str, object]:
    entries = json.loads(content)
    if isinstance(entries, dict) and _COMPRESSED_KEY in entries:
        data = base64.b64decode(entries[_COMPRESSED_KEY], validate=True)
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
            plain = stream.read(_MAX_COMPRESSED_STATS_OUTPUT + 1)
        if len(plain) > _MAX_COMPRESSED_STATS_OUTPUT:
            raise ValueError("decompressed stats exceed the size limit")
        entries = json.loads(plain)
    if not isinstance(entries, dict):
        raise ValueError("not a JSON object")
    return entries


def is_stats_file(path: str) -> bool:
    return path.rpartition("/")[2] == STATS_FILE_NAME


def is_stats_commit(message: object) -> bool:
    return isinstance(message, str) and message.startswith(STATS_COMMIT_PREFIX)
