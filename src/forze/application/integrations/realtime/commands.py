"""Typed inbound command routes — one declaration, any duplex transport.

A command route binds a wire event name to a registry operation with typed
payload/ack validation. Declared once by the app and consumed by whichever duplex
edge carries the frames — the Socket.IO namespace router and the raw-WebSocket
route dispatch through the same declaration, so a command's contract cannot drift
between transports (or their AsyncAPI documentation, which is generated from the
same routes).
"""

from typing import Any, final

import attrs
from pydantic import TypeAdapter

from forze.base.primitives import StrKey

# ----------------------- #

__all__ = [
    "RealtimeCommandRoute",
]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RealtimeCommandRoute[Args, Ack]:
    """Typed mapping between an inbound event and a registry operation."""

    event: str
    """Wire event name (a Socket.IO event, a WebSocket ``cmd`` frame's ``event``)."""

    operation: StrKey
    """Operation key resolved by :class:`~forze.application.execution.OperationRegistry`."""

    payload_type: Any
    """Validation type consumed by :class:`pydantic.TypeAdapter` for inbound payload."""

    ack_type: Any = None
    """Optional validation type for acknowledgement payload."""

    _payload_adapter: TypeAdapter[Any] = attrs.field(
        default=attrs.Factory(
            lambda self: TypeAdapter[Any](self.payload_type),
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    """Cached :class:`~pydantic.TypeAdapter` for ``payload_type``."""

    _ack_adapter: TypeAdapter[Any] | None = attrs.field(
        default=attrs.Factory(
            lambda self: TypeAdapter[Any](self.ack_type) if self.ack_type is not None else None,
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
    )
    """Cached adapter for ``ack_type``, or :obj:`None` when acknowledgements are untyped."""

    # ....................... #

    def parse_payload(self, payload: Any) -> Args:
        """Validate and coerce the inbound payload.

        :param payload: Raw event payload from the transport.
        :returns: Parsed payload value passed to the handler.
        """
        return self._payload_adapter.validate_python(payload)

    # ....................... #

    def parse_ack(self, value: Any) -> Ack | Any:
        """Validate and normalize the handler result for acknowledgement.

        :param value: Raw handler result.
        :returns: JSON-compatible acknowledgement payload.
        """
        if self._ack_adapter is None:
            return value

        validated = self._ack_adapter.validate_python(value)

        return self._ack_adapter.dump_python(validated, mode="json")
