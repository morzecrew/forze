"""A connection's command events run under the tenant it resolved at connect.

# covers: forze_socketio.routing (_dispatch_event) with forze_socketio.connection
#         (attach_realtime_connection storing the connection on the session)

``attach_realtime_connection`` resolves the connection's principal and tenant once, at
connect, and each command event runs in a task of its own. The event binds both, as the
raw-WebSocket route does for its frames: a tenant-aware operation would otherwise fail
closed, or run with no tenant, on a connection that has one.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import attrs
import pytest
from engineio.async_socket import AsyncSocket
from pydantic import BaseModel
from socketio.async_server import AsyncServer

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze_socketio import RealtimeConnection, SocketIOConnect, attach_realtime_connection
from forze_socketio.routing import ForzeSocketIOAdapter, SocketIONamespaceRouter
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #

_PRINCIPAL = UUID("33333333-3333-3333-3333-333333333333")
_TENANT = UUID("44444444-4444-4444-4444-444444444444")


class _Empty(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Tenant(Handler[_Empty, Any]):
    ctx: ExecutionContext

    async def __call__(self, args: _Empty) -> Any:
        tenant = self.ctx.inv_ctx.get_tenant()
        authn = self.ctx.inv_ctx.get_authn()

        return {
            "tenant_id": str(tenant.tenant_id) if tenant is not None else None,
            "principal_id": str(authn.principal_id) if authn is not None else None,
        }


async def _resolve(connect: SocketIOConnect) -> RealtimeConnection:
    tenanted = (connect.auth or {}).get("tenanted")

    return RealtimeConnection(
        authn=AuthnIdentity(principal_id=_PRINCIPAL),
        tenant=_TENANT if tenanted else None,
    )


def _server() -> AsyncServer:
    server = AsyncServer()
    server.eio.sockets["eio-1"] = AsyncSocket(server.eio, "eio-1")
    server.environ["eio-1"] = {}
    attach_realtime_connection(server, resolve=_resolve)
    ForzeSocketIOAdapter(
        sio=server,
        context_factory=lambda _request: context_from_deps(Deps()),
        operation_resolver=OperationRegistry(handlers={"tenant": lambda ctx: _Tenant(ctx=ctx)})
        .freeze()
        .resolve,
    ).include_router(
        SocketIONamespaceRouter(namespace="/").command(
            event="tenant", operation="tenant", payload_type=_Empty
        )
    )

    return server


async def _connect_and_ask(auth: dict[str, Any]) -> Any:
    server = _server()

    # python-socketio runs the connect and every event in a task of its own.
    await asyncio.create_task(
        server._handle_connect("eio-1", "/", auth)  # pyright: ignore[reportPrivateUsage]
    )
    [(sid, _eio)] = list(server.manager.get_participants("/", None))

    return await asyncio.create_task(
        server._trigger_event("tenant", "/", sid, {})  # pyright: ignore[reportPrivateUsage]
    )


# ----------------------- #


async def test_an_event_runs_under_its_connections_tenant() -> None:
    ack = await _connect_and_ask({"tenanted": True})

    assert ack == {"tenant_id": str(_TENANT), "principal_id": str(_PRINCIPAL)}


async def test_an_event_on_an_untenanted_connection_binds_no_tenant() -> None:
    ack = await _connect_and_ask({})

    assert ack == {"tenant_id": None, "principal_id": str(_PRINCIPAL)}
