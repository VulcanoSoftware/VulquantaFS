import struct
from http import HTTPStatus
from typing import Iterable, Optional

from .error_code import ErrorCode

# Errors that say one request can never work, not that a store is down: an
# id the protocol cannot encode, a malformed reply, a wrong type.
DATA_ERRORS = (
    ArithmeticError,
    LookupError,
    TypeError,
    ValueError,
    UnicodeError,
    struct.error,
)


class TechnicalError(Exception):
    def __init__(
        self,
        message: str,
        cause: Optional[str] = None,
        http_error: HTTPStatus = HTTPStatus.INTERNAL_SERVER_ERROR,
    ):
        super().__init__(message)
        self.cause = cause
        self.http_error = http_error


class RpcTimeoutError(TechnicalError):
    """A call into a store stayed silent past its deadline.

    Unlike any other technical failure the store never answered at all,
    so there is nothing worth negotiating: retry loops must stop and let
    the failure travel straight to the failover logic.
    """


class BusinessError(TechnicalError):
    def __init__(
        self,
        message: str,
        code: ErrorCode,
        cause: Optional[str] = None,
        http_error: HTTPStatus = HTTPStatus.BAD_REQUEST,
    ):
        super().__init__(message, cause, http_error)
        self.code = code


class AggregatedError(Exception):
    def __init__(self, errors: Iterable[Exception]):
        super().__init__("\n".join(str(e) for e in errors))


def is_store_outage(ex: BaseException) -> bool:
    """Whether ``ex`` says the store is unreachable, not that a request failed.

    The read path marks a store dead -- taking it away from every file for
    the next minute -- on any exception today. A request that could not
    even be built says nothing about the store's health, though: one
    message id the protocol cannot encode used to disable the primary for
    a minute, and every file read in that minute came back as an empty
    file.
    """
    seen: set = set()
    current: Optional[BaseException] = ex
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, DATA_ERRORS):
            return False
        current = current.__cause__ or current.__context__
    return True
