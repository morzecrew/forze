"""Error translation for Socket.IO acknowledgements.

Mirrors the FastAPI error boundary (:mod:`forze_fastapi.exceptions`) on the ack
channel: a dispatched event handler returns either the route's ack payload or a
structured error ack of the shape::

    {
        "error": {
            "detail": "<client-safe summary>",
            "code": "<exception code>",
            "kind": "<exception kind>",
            "context": {...}  # only when the egress policy exposes details
        }
    }

Kinds that would map to an HTTP status ``>= 500`` (internal, infrastructure,
configuration, unknown) are logged server-side and acked with a generic detail
so internal diagnostics never leak to clients. ``context`` is
included only for client-safe kinds whose egress policy exposes details, after
scrubbing via :func:`forze.base.scrubbing.sanitize` with the ``egress`` context.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from typing import Final

from forze.base.exceptions import (
    CoreException,
    ExceptionKind,
    exception_egress_policy,
    http_status_for_kind,
)
from forze.base.logging import Logger
from forze.base.primitives import JsonDict
from forze.base.scrubbing import sanitize

from ._logging import ForzeSocketIOLogger

# ----------------------- #

GENERIC_INTERNAL_DETAIL: Final[str] = "Internal server error"
"""Generic detail message for server-side errors."""

INTERNAL_ERROR_CODE: Final[str] = "core.internal"
"""Error code used for unhandled non-:class:`CoreException` errors."""

error_logger = Logger(ForzeSocketIOLogger.ERRORS)
"""Logger for Socket.IO server-side error diagnostics."""

# ....................... #


def is_server_error_kind(kind: ExceptionKind) -> bool:
    """Return whether *kind* is a server-side error.

    Shares the canonical status mapping with :mod:`forze_fastapi.exceptions`
    via :func:`http_status_for_kind`: kinds that map to a status ``>= 500`` (and
    unknown kinds, which fall back to ``500``) are server-side and must never
    expose their summary or details to clients. Deriving from the single source
    of truth keeps both transports in lock-step as kinds are added or remapped.
    """

    return http_status_for_kind(kind) >= 500


# ....................... #


def log_server_error(exc: BaseException, *, core: CoreException | None = None) -> None:
    """Log a server-side error with appropriate severity and traceback policy."""

    if core is not None and core.__cause__ is not None:
        error_logger.critical_exception(
            "Server error",
            exc=core.__cause__,
            error_code=core.code,
            error_kind=core.kind.value,
        )

    elif core is not None:
        error_logger.error(
            "Server error",
            error_code=core.code,
            error_kind=core.kind.value,
            detail=core.summary,
        )

    else:
        error_logger.critical_exception("Unhandled exception", exc=exc)


# ....................... #


def build_core_exception_ack(exc: CoreException) -> JsonDict:
    """Build the standard error acknowledgement payload for a :class:`CoreException`.

    Server-side kinds are logged and their summary is replaced with a generic
    detail message so internal diagnostics never leak to clients. The sanitized
    ``context`` field is only included when the egress policy for the exception
    kind allows exposing details.
    """

    policy = exception_egress_policy(exc.kind)
    server_error = is_server_error_kind(exc.kind)

    if server_error:
        log_server_error(exc, core=exc)

    payload: JsonDict = {
        "detail": GENERIC_INTERNAL_DETAIL if server_error else exc.summary,
        "code": exc.code,
        "kind": exc.kind.value,
    }

    if exc.details and policy.expose_details and not server_error:
        payload["context"] = sanitize(exc.details, context="egress")

    return {"error": payload}


# ....................... #


def build_unhandled_exception_ack(exc: BaseException) -> JsonDict:
    """Build the generic error acknowledgement for an unhandled exception.

    The exception is logged at CRITICAL level with its traceback; the client
    only ever receives the generic internal-error payload.
    """

    log_server_error(exc)

    return {
        "error": {
            "detail": GENERIC_INTERNAL_DETAIL,
            "code": INTERNAL_ERROR_CODE,
            "kind": ExceptionKind.INTERNAL.value,
        }
    }
