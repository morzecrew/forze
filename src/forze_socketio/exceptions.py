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

from forze.base.exceptions import (
    GENERIC_INTERNAL_DETAIL,
    INTERNAL_ERROR_CODE,
    CoreException,
    ErrorEnvelope,
    error_envelope,
    is_server_error_kind,
    unhandled_error_envelope,
)
from forze.base.logging import Logger
from forze.base.primitives import JsonDict

from ._logging import ForzeSocketIOLogger

# ----------------------- #

# ``GENERIC_INTERNAL_DETAIL``, ``INTERNAL_ERROR_CODE``, ``is_server_error_kind``,
# and the egress projection are re-exported from core
# (:mod:`forze.base.exceptions.envelope`) so the canonical mapping is shared with
# :mod:`forze_fastapi.exceptions` and any future realtime transport.
__all__ = [
    "GENERIC_INTERNAL_DETAIL",
    "INTERNAL_ERROR_CODE",
    "is_server_error_kind",
    "log_server_error",
    "render_error_ack",
    "build_core_exception_ack",
    "build_unhandled_exception_ack",
]

error_logger = Logger(ForzeSocketIOLogger.ERRORS)
"""Logger for Socket.IO server-side error diagnostics."""

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


def render_error_ack(envelope: ErrorEnvelope) -> JsonDict:
    """Render a core :class:`ErrorEnvelope` into the Socket.IO ack error shape.

    Pure: the envelope has already masked server-side details and sanitized any
    exposable ``context``, so this only maps fields onto the wire shape. Logging
    is the caller's responsibility (see :func:`forze.application.transport.guard_frame`).
    """

    payload: JsonDict = {
        "detail": envelope.detail,
        "code": envelope.code,
        "kind": envelope.kind.value,
    }

    if envelope.context is not None:
        payload["context"] = envelope.context

    return {"error": payload}


# ....................... #


def build_core_exception_ack(exc: CoreException) -> JsonDict:
    """Build the standard error acknowledgement payload for a :class:`CoreException`.

    Convenience wrapper that projects *exc* via the core egress mapping, logs
    server-side errors, and renders the ack. The dispatch path logs through
    :func:`guard_frame` instead and calls :func:`render_error_ack` directly.
    """

    envelope = error_envelope(exc)

    if envelope.server_error:
        log_server_error(exc, core=exc)

    return render_error_ack(envelope)


# ....................... #


def build_unhandled_exception_ack(exc: BaseException) -> JsonDict:
    """Build the generic error acknowledgement for an unhandled exception.

    The exception is logged at CRITICAL level with its traceback; the client
    only ever receives the generic internal-error payload.
    """

    log_server_error(exc)

    return render_error_ack(unhandled_error_envelope())
