"""The realtime push port — server→client emit, transport-neutral.

Implemented by a transport adapter (Socket.IO today; a FastAPI-websocket or SSE
adapter could satisfy it next) and resolved as ``ctx.realtime()`` so any handler,
saga, or projector can push to live clients without importing the transport.

The surface is deliberately one method: emit *to a logical* :class:`Audience`.
*Who is currently connected*, *which connection*, and *which tenant* are ambient
concerns the transport owns — none of them appear here. Connection/room
membership lives at the transport edge, not on this port.
"""

from typing import Awaitable, Protocol, runtime_checkable

from pydantic import BaseModel

from .audience import Audience

# ----------------------- #


@runtime_checkable
class RealtimePort(Protocol):
    """Server-initiated realtime delivery to a logical audience.

    Delivery is **at-most-once / live-only**: an emit reaches the connections
    currently part of *audience* and is not stored for those offline. Durable,
    must-eventually-arrive push rides the outbox/notify pipeline instead. Tenant
    scoping is applied by the adapter from the bound invocation identity.
    """

    def emit(
        self,
        audience: Audience,
        event: str,
        payload: BaseModel,
    ) -> Awaitable[None]:
        """Emit *event* with *payload* to *audience* within the current tenant.

        :param audience: Logical target (principal / topic / tenant).
        :param event: Client-facing event name.
        :param payload: Event body; serialized by the adapter.
        """

        ...  # pragma: no cover
