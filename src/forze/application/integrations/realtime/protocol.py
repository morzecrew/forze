"""The realtime wire-protocol version — negotiated once per connection.

The delivery contract every transport shares — the ``{id, data}`` envelope,
cumulative ``realtime.ack {up_to}``, catalog-declared event names — is versioned by a
single integer the client sends **in the connect handshake** (the Socket.IO ``auth``
payload, an SSE query parameter), never per-frame: frames stay lean, and a connection
speaks exactly one protocol version for its lifetime.

A missing version means ``1`` (pre-versioning clients). An unsupported version is
refused at connect with ``realtime_protocol_unsupported`` naming the supported range —
never silently downgraded. Additive envelope changes (new optional fields) do not bump
the version; clients must ignore unknown envelope fields.
"""

from typing import Final

from pydantic import BaseModel, Field

from forze.base.exceptions import exc

# ----------------------- #

__all__ = [
    "REALTIME_PROTOCOL_VERSION",
    "SUPPORTED_REALTIME_PROTOCOLS",
    "RealtimeAck",
    "negotiate_realtime_protocol",
]

REALTIME_PROTOCOL_VERSION: Final[int] = 1
"""The protocol version this server speaks (and assumes when a client sends none)."""

SUPPORTED_REALTIME_PROTOCOLS: Final[frozenset[int]] = frozenset({REALTIME_PROTOCOL_VERSION})
"""Every protocol version this server accepts at connect."""


class RealtimeAck(BaseModel):
    """The cumulative-ack payload — one wire shape on every transport.

    ``realtime.ack`` over Socket.IO, ``POST …/ack`` over SSE, and the AsyncAPI
    export all derive from this model, so the ack contract cannot drift between
    transports and their documentation.
    """

    up_to: str = Field(description="Cumulative: the last delivered durable event id.")


def negotiate_realtime_protocol(raw: object) -> int:
    """Negotiate the connection's protocol version from the raw handshake value.

    ``None`` (no version sent) resolves to :data:`REALTIME_PROTOCOL_VERSION`. An
    integer or integer string is accepted when supported; anything else — an
    unsupported version, garbage, a bool — is refused with
    ``realtime_protocol_unsupported``. Client input on an unauthenticated path, so
    the refusal is caller-caused and client-safe.
    """

    supported = sorted(SUPPORTED_REALTIME_PROTOCOLS)

    match raw:
        case None:
            return REALTIME_PROTOCOL_VERSION

        case bool():  # bool is an int subclass — but True is not protocol 1
            version = None

        case int():
            version = raw

        # isdecimal, not isdigit: superscripts/circled digits ("²", "①") pass
        # isdigit but int() rejects them — that must be a clean refusal, not a crash
        case str() if raw.strip().isdecimal():
            version = int(raw)

        case _:
            version = None

    if version not in SUPPORTED_REALTIME_PROTOCOLS:
        raise exc.validation(
            f"Unsupported realtime protocol {raw!r}: this server accepts {supported}",
            code="realtime_protocol_unsupported",
            details={"supported": supported},
        )

    return version
