"""JSON frame encoding for realtime transports — one boundary, and it never raises.

Every realtime transport ultimately hands a payload to ``json.dumps`` — a WebSocket
control frame, an SSE ``data:`` line, a Socket.IO ack — and each used to do so at its
own call site with its own (or no) guard. A payload that cannot serialize then costs
far more than one frame: in a WebSocket task group the ``TypeError`` matches neither
``except*`` clause and cancels every in-flight command on the socket. This module is
the single boundary those call sites share: :func:`encode_frame` (and its dict-shaped
twin :func:`jsonable_frame` for transports whose library serializes for them) returns
an encoded frame or a **masked fallback error frame** — never an exception.

The fallback preserves the payload's correlation keys (``type``, ``cid``, ``id``,
``event`` — when they are plain strings) and carries a context-free internal error
envelope, so it is provably serializable regardless of what broke the original.

Frames are compact JSON written by ``orjson``: non-ASCII text goes out as UTF-8 rather
than ``\\u`` escapes, a ``UUID`` or an ``Enum`` as its JSON value, and ``NaN`` or an
infinity as ``null`` (the stdlib wrote ``NaN``, which ``JSON.parse`` rejects). A
``datetime``, a dataclass, and anything else the stdlib refused still fall back. A value
only orjson refuses (an int past 64 bits, a lone surrogate) keeps the stdlib encoding.
"""

import json
from collections.abc import Mapping
from typing import Any, Final

import orjson

from forze.application._logger import logger

# ----------------------- #

FRAME_UNSERIALIZABLE_CODE: Final[str] = "realtime_frame_unserializable"
"""Fallback error code when a realtime frame's payload cannot be JSON-encoded."""

_CORRELATION_KEYS: Final[tuple[str, ...]] = ("type", "cid", "id", "event")
"""Payload keys the fallback frame preserves, so the client can still correlate it."""

_ORJSON_OPTIONS: Final[int] = orjson.OPT_PASSTHROUGH_DATETIME | orjson.OPT_PASSTHROUGH_DATACLASS
"""Refuse what the stdlib refused: orjson would otherwise encode these itself."""


# ....................... #


def _fallback_frame(payload: Mapping[str, Any], *, code: str) -> dict[str, Any]:
    """The masked replacement frame: correlation keys plus a context-free error.

    Built without :func:`~forze.base.exceptions.error_envelope` on purpose — this must
    be constructible from string literals alone, so nothing here can fail to encode.
    """

    fallback: dict[str, Any] = {
        # JSON scalars only: a client cid may be any JSON scalar (int included), and
        # every one of these is provably serializable — unlike whatever broke the frame.
        key: payload[key]
        for key in _CORRELATION_KEYS
        if key in payload and isinstance(payload[key], (str, int, float, bool, type(None)))
    }
    fallback["error"] = {
        "detail": "Internal server error",
        "code": code,
        "kind": "internal",
    }

    return fallback


# ....................... #


def _dumps(payload: Mapping[str, Any]) -> str:
    try:
        return orjson.dumps(payload, option=_ORJSON_OPTIONS).decode()

    except TypeError:
        # Ints past 64 bits, float subclasses, lone surrogates, non-str keys and nesting
        # past 254 levels: orjson refuses them, the stdlib encoded them, so it still does.
        return json.dumps(payload, separators=(",", ":"))


# ....................... #


def encode_frame(
    payload: Mapping[str, Any],
    *,
    fallback_code: str = FRAME_UNSERIALIZABLE_CODE,
) -> str:
    """Serialize a realtime frame; an unencodable payload costs THIS frame, never a raise.

    ``ErrorEnvelope`` already coerces its context to JSON-safe values, so through the
    framework's own paths this fallback should never fire — it is the backstop for
    whatever regresses that next (an application payload carrying a live object, an
    untyped ack). *fallback_code* lets a call site keep a more specific error code
    (the WebSocket command ack uses ``realtime_ack_unserializable``).
    """

    try:
        return _dumps(payload)

    except (TypeError, ValueError) as error:
        logger.critical_exception("Realtime frame is not JSON-serializable", exc=error)

        # The stdlib, not orjson: it encodes any scalar a correlation key may hold.
        return json.dumps(_fallback_frame(payload, code=fallback_code), separators=(",", ":"))


# ....................... #


def jsonable_frame(
    payload: Mapping[str, Any],
    *,
    fallback_code: str = FRAME_UNSERIALIZABLE_CODE,
) -> dict[str, Any]:
    """The dict-shaped twin of :func:`encode_frame`, for transports whose library owns
    serialization (python-socketio encodes ack dicts itself — handing it a raw dict
    would move the same ``TypeError`` into the library's emit path)."""

    encoded = encode_frame(payload, fallback_code=fallback_code)

    # The stdlib parser: it also reads the ``NaN`` and big ints a stdlib-encoded frame holds.
    return json.loads(encoded)  # type: ignore[no-any-return]
