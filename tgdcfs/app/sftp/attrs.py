import stat
from typing import Optional

from asyncssh import SFTPAttrs

DIR_MODE = 0o755
DIR_MODE_READONLY = 0o555
FILE_MODE = 0o644
FILE_MODE_READONLY = 0o444


def _seconds(timestamp_ms: int) -> int:
    """TGDCFS keeps timestamps in milliseconds, SFTP wants whole seconds."""
    return max(0, timestamp_ms // 1000)


def dir_attrs(created_ms: int, modified_ms: int, readonly: bool) -> SFTPAttrs:
    mode = DIR_MODE_READONLY if readonly else DIR_MODE
    return SFTPAttrs(
        size=0,
        permissions=stat.S_IFDIR | mode,
        uid=0,
        gid=0,
        atime=_seconds(modified_ms),
        mtime=_seconds(modified_ms),
        crtime=_seconds(created_ms),
        nlink=1,
    )


def file_attrs(
    size: Optional[int],
    created_ms: Optional[int],
    modified_ms: Optional[int],
    readonly: bool,
) -> SFTPAttrs:
    mode = FILE_MODE_READONLY if readonly else FILE_MODE
    return SFTPAttrs(
        size=max(0, size) if size is not None else None,
        permissions=stat.S_IFREG | mode,
        uid=0,
        gid=0,
        atime=_seconds(modified_ms) if modified_ms is not None else None,
        mtime=_seconds(modified_ms) if modified_ms is not None else None,
        crtime=_seconds(created_ms) if created_ms is not None else None,
        nlink=1,
    )


def root_attrs(readonly: bool) -> SFTPAttrs:
    """Attributes of the synthetic directory listing the configured clients."""
    return dir_attrs(created_ms=0, modified_ms=0, readonly=readonly)
