"""Socket.IO implementation of the core :class:`RealtimePort`.

A logical :class:`Audience` is resolved to a Socket.IO room here, applying the
**ambient tenant** read from the invocation context — the caller never passes a
tenant. Because a tenant-scoped room name is just a string, one
``sio.emit(room=...)`` fans out cluster-wide when the server is backed by the
Redis manager (:func:`forze_socketio.build_socketio_server`), with no extra
infrastructure. Wire it with :func:`socketio_realtime_deps` so ``ctx.realtime()``
resolves this adapter from any handler or saga.

Connection/room *membership* (auto-join on connect, topic subscription) is a
transport-edge concern handled by the Socket.IO connect/event handlers, not by
this port.
"""

from ._compat import require_socketio

require_socketio()

# ....................... #

from typing import final
from uuid import UUID

import attrs
from pydantic import BaseModel
from socketio.async_server import AsyncServer

from forze.application.contracts.deps import Deps
from forze.application.contracts.realtime import (
    Audience,
    AudienceKind,
    RealtimeDepKey,
    RealtimePort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SocketIORealtimeAdapter(TenancyMixin, RealtimePort):
    """:class:`RealtimePort` over a Socket.IO async server.

    Inherits tenant scoping from :class:`TenancyMixin`: when a tenant is bound it
    prefixes every room with ``t:<tenant_id>:`` (so tenants cannot cross-talk);
    with ``tenant_aware=True`` an unbound tenant fails closed exactly like the
    other tenant-aware ports.
    """

    sio: AsyncServer
    """Socket.IO async server used for delivery."""

    namespace: str = "/"
    """Namespace this adapter emits on."""

    # ....................... #

    def _room(self, audience: Audience) -> str:
        """Resolve *audience* to a tenant-scoped Socket.IO room name."""

        tenant_id: UUID | None = self._tenant_id_for_resolve()
        prefix = f"t:{tenant_id}:" if tenant_id is not None else ""

        if audience.kind is AudienceKind.TENANT:
            # the tenant-broadcast room is the prefix root itself
            return f"t:{tenant_id}" if tenant_id is not None else "tenant"

        return f"{prefix}{audience}"

    # ....................... #

    async def emit(
        self,
        audience: Audience,
        event: str,
        payload: BaseModel,
    ) -> None:
        await self.sio.emit(
            event,
            data=payload.model_dump(mode="json"),
            room=self._room(audience),
            namespace=self.namespace,
        )


# ....................... #


def socketio_realtime_deps(
    sio: AsyncServer,
    *,
    namespace: str = "/",
    tenant_aware: bool = False,
) -> Deps:
    """Build a deps fragment registering the Socket.IO realtime port.

    Merge into your deps registry so ``ctx.realtime()`` resolves the Socket.IO
    adapter from any handler, saga, or projector::

        deps = merge_deps(app_deps, socketio_realtime_deps(sio, tenant_aware=True))

    Rooms are always tenant-scoped when a tenant is bound. ``tenant_aware=True``
    additionally fails closed (``tenant_required``) when no tenant is bound —
    set it for multi-tenant apps; leave it ``False`` for single-tenant/system
    deployments that emit without a tenant.

    :param sio: The Socket.IO async server to emit through.
    :param namespace: Namespace the adapter emits on.
    :param tenant_aware: Require a bound tenant (fail-closed) when ``True``.
    :returns: A :class:`Deps` fragment binding :data:`RealtimeDepKey`.
    """

    def _build(ctx: ExecutionContext) -> SocketIORealtimeAdapter:
        return SocketIORealtimeAdapter(
            sio=sio,
            namespace=namespace,
            tenant_aware=tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )

    return Deps.plain({RealtimeDepKey: _build})
