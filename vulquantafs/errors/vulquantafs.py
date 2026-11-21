from .base import BusinessError
from .error_code import ErrorCode


class MetadataNotFound(BusinessError):
    def __init__(self):
        super().__init__(
            message="Metadata not found",
            code=ErrorCode.METADATA_NOT_FOUND,
            cause=None,
        )


class MetadataNotInitialized(BusinessError):
    def __init__(self):
        super().__init__(
            message="Metadata not initialized",
            code=ErrorCode.METADATA_NOT_INITIALIZED,
            cause=None,
        )


class UnDownloadableMessage(BusinessError):
    def __init__(self, message_id: int):
        super().__init__(
            message=f"Message {message_id} does not contain a document",
            code=ErrorCode.UNDOWNLOADABLE_MESSAGE,
            cause=None,
        )
        self.message_id = message_id


class NoPinnedMessage(BusinessError):
    def __init__(self):
        super().__init__(
            message="No pinned message found",
            code=ErrorCode.NO_PINNED_MESSAGE,
            cause=None,
        )


class NoStoredMetadata(BusinessError):
    """A metadata store holds no metadata document yet.

    Raised instead of creating one when a store is only a redundant copy:
    the replicated layer must not bootstrap an empty tree into a mirror
    while the primary is briefly unreachable, or it would look empty and
    win the failover.
    """

    def __init__(self, store: str = ""):
        super().__init__(
            message=f"No metadata document in store {store}".strip(),
            code=ErrorCode.NO_STORED_METADATA,
            cause=None,
        )


class MetadataBlobTooLarge(BusinessError):
    """The metadata document does not fit one message of a pinned store.

    A pinned copy has to be a single message; a store with smaller
    messages cannot hold it. The replicated layer treats this as a
    permanent skip for that store (logged once) rather than an error to
    retry, and un-skips it if a later document fits again.
    """

    def __init__(self, size: int, store: str, max_part_bytes: int):
        super().__init__(
            message=(
                f"The metadata blob ({size} bytes) does not fit one message "
                f"of store {store} ({max_part_bytes} bytes)"
            ),
            code=ErrorCode.METADATA_BLOB_TOO_LARGE,
            cause=None,
        )


class PinnedMessageNotSupported(BusinessError):
    def __init__(self):
        super().__init__(
            message="Pinned message is not supported because account api is not configured. Use metadata by Github repo instead.",
            code=ErrorCode.PINNED_MESSAGE_NOT_SUPPORTED,
            cause=None,
        )


class TaskCancelled(BusinessError):
    def __init__(self, file_name: str):
        super().__init__(
            message=f"Task for {file_name} has been cancelled",
            code=ErrorCode.TASK_CANCELLED,
            cause=None,
        )


class LoginFailed(BusinessError):
    def __init__(self, message: str):
        super().__init__(
            message=message,
            code=ErrorCode.LOGIN_FAILED,
            cause=None,
        )


class DuplicatedChannelIdOrName(BusinessError):
    def __init__(self, channel_id_or_name: str):
        super().__init__(
            message=f"Channel ID or name {channel_id_or_name} is already used",
            code=ErrorCode.DUPLICATED_CHANNEL_ID_OR_NAME,
            cause=None,
        )
