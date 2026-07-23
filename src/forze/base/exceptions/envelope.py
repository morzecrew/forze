"""Transport-neutral egress projection of a :class:`CoreException`.

A :class:`CoreException` carries everything a transport needs to render a
client-facing error, but the rules for *what* to expose are identical across
HTTP, Socket.IO, and any future realtime transport: a generic detail for
server-side errors, sanitized context only when the kind's egress policy allows
it, and a status code per kind. This module computes those rules once into an
:class:`ErrorEnvelope`; each transport renders the envelope into its own wire
shape (a JSON response, a Socket.IO ack, a websocket close frame).

The projection is **pure** — no logging, no I/O. A transport decides whether to
log a server-side error from :attr:`ErrorEnvelope.server_error`, keeping the
projection free of any logger dependency.
"""

import json
from typing import Any, Final, cast

import attrs

from forze.base.scrubbing import sanitize

from .egress import exception_egress_policy, http_status_for_kind
from .model import CoreException, ExceptionKind

# ----------------------- #

# `dict[str, Any]` inline rather than ``forze.base.primitives.JsonDict``:
# ``forze.base.primitives`` imports ``forze.base.exceptions`` (for ``exc``), so
# importing it here would form an init-time cycle.
type JsonDict = dict[str, Any]

# ....................... #

GENERIC_INTERNAL_DETAIL: Final[str] = "Internal server error"
"""Client-facing detail substituted for any server-side (``>= 500``) error."""

INTERNAL_ERROR_CODE: Final[str] = "core.internal"
"""Error code for an unhandled, non-:class:`CoreException` error."""

# ....................... #


def is_server_error_kind(kind: ExceptionKind) -> bool:
    """Return whether *kind* maps to a server-side (HTTP ``>= 500``) status.

    Derived from the single status mapping (:func:`http_status_for_kind`) so
    every transport agrees on which errors must never expose their summary or
    details to clients. Unknown kinds fall back to ``500`` and are server-side.
    """

    return http_status_for_kind(kind) >= 500


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class ErrorEnvelope:
    """Transport-neutral, client-safe projection of an error.

    Already accounts for server-error masking and egress sanitization, so a
    transport renderer only maps fields onto its wire shape — it makes no
    exposure decisions of its own.
    """

    code: str
    """Stable error code (for example ``core.not_found``)."""

    kind: ExceptionKind
    """Originating exception kind."""

    detail: str
    """Client-safe summary; generic for server-side errors."""

    status: int
    """Conventional HTTP status for the kind; a status hint for non-HTTP transports."""

    retryable: bool
    """Whether the originating kind is retryable, per its egress policy."""

    server_error: bool
    """Whether this is a server-side (``>= 500``) error.

    Transports use it to decide server-side logging and to confirm the detail
    has already been masked.
    """

    context: JsonDict | None = None
    """Sanitized, client-safe error context, or :obj:`None` when not exposed."""


# ....................... #


def _jsonable(context: JsonDict) -> JsonDict:
    """Coerce sanitized context to values ``json.dumps`` can actually encode.

    ``CoreException.details`` is typed as JSON but not enforced — handlers idiomatically
    pass UUIDs, datetimes or Decimals — and every transport renders this context with a
    plain ``json.dumps`` (an HTTP response, a Socket.IO ack, a WebSocket frame). There a
    TypeError escalates far past one error: a WS control frame that fails to serialize
    matches neither ``except*`` clause and unwinds the connection's whole task group.
    Coercing here keeps the envelope's contract true for every renderer at once:
    whatever it exposes IS renderable. Non-JSON leaves render as their ``str`` form.
    """

    try:
        return cast(JsonDict, json.loads(json.dumps(context, default=str)))

    except (TypeError, ValueError):
        # circular or otherwise hostile details: keep the error, drop the context
        return {"context_unrenderable": True}


def error_envelope(exc: CoreException) -> ErrorEnvelope:
    """Project a :class:`CoreException` into a client-safe :class:`ErrorEnvelope`.

    Pure: server-side errors get a generic detail and no context; client-safe
    kinds keep their summary and expose sanitized context only when the kind's
    egress policy allows it.
    """

    policy = exception_egress_policy(exc.kind)
    status = http_status_for_kind(exc.kind)
    server_error = status >= 500

    context: JsonDict | None = None
    if exc.details and policy.expose_details and not server_error:
        context = _jsonable(sanitize(exc.details, context="egress"))

    return ErrorEnvelope(
        code=exc.code,
        kind=exc.kind,
        detail=GENERIC_INTERNAL_DETAIL if server_error else exc.summary,
        status=status,
        retryable=policy.retryable,
        server_error=server_error,
        context=context,
    )


# ....................... #


def unhandled_error_envelope() -> ErrorEnvelope:
    """Build the generic envelope for an unhandled, non-:class:`CoreException` error.

    The client only ever sees the generic internal-error payload; the transport
    is responsible for logging the original exception with its traceback.
    """

    return ErrorEnvelope(
        code=INTERNAL_ERROR_CODE,
        kind=ExceptionKind.INTERNAL,
        detail=GENERIC_INTERNAL_DETAIL,
        status=500,
        retryable=exception_egress_policy(ExceptionKind.INTERNAL).retryable,
        server_error=True,
        context=None,
    )
