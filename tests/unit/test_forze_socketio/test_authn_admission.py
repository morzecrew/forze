"""A connection's commands run with the admission its credential's verification recorded.

# covers: forze_socketio.connection (_resolve, on_connect, on_reauth) and
#         forze_socketio.routing (the identity-resolver connect, _dispatch_event)

A socket verifies its credential at connect (and reauth) and runs each command in a task of its
own, so what the verification recorded must travel with the connection: an issue the command
makes is checked against the credential that authenticated the socket, never against nothing.
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

from forze.application.contracts.authn import (
    AuthnIdentity,
    current_authn_admission,
    record_authn_admission,
)
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps
from forze.application.execution.operations.registry import OperationRegistry
from forze_socketio import RealtimeConnection, SocketIOConnect, attach_realtime_connection
from forze_socketio.routing import ForzeSocketIOAdapter, SocketIONamespaceRouter
from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_socketio.test_routing import StubSocketIOServer

pytestmark = pytest.mark.unit

# ----------------------- #

_PRINCIPAL = UUID("22222222-2222-2222-2222-222222222222")


class _Empty(BaseModel):
    pass


@attrs.define(slots=True, frozen=True)
class _Admission(Handler[_Empty, Any]):
    async def __call__(self, args: _Empty) -> Any:
        return {"admission": current_authn_admission()}


def _token(auth: Any) -> str:
    return str((auth or {}).get("token"))


def _record(token: str) -> None:
    # A verification records what it admitted; "plain" stands for a credential with none.
    if token != "plain":
        record_authn_admission(f"admitted:{token}")


async def _resolve(connect: SocketIOConnect) -> RealtimeConnection:
    _record(_token(connect.auth))
    return RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))


def _commands(sio: Any, **kwargs: Any) -> None:
    ForzeSocketIOAdapter(
        sio=sio,
        context_factory=lambda _request: context_from_deps(Deps()),
        operation_resolver=OperationRegistry(handlers={"admission": lambda _ctx: _Admission()})
        .freeze()
        .resolve,
        **kwargs,
    ).include_router(
        SocketIONamespaceRouter(namespace="/").command(
            event="admission", operation="admission", payload_type=_Empty
        )
    )


def _server() -> AsyncServer:
    server = AsyncServer()
    server.eio.sockets["eio-1"] = AsyncSocket(server.eio, "eio-1")
    server.environ["eio-1"] = {}

    return server


async def _event(server: AsyncServer, sid: str, event: str, data: Any) -> Any:
    # python-socketio runs every event in a task of its own.
    return await asyncio.create_task(
        server._trigger_event(event, "/", sid, data)  # pyright: ignore[reportPrivateUsage]
    )


# ----------------------- #


async def test_commands_run_with_the_admission_of_the_connect_and_then_of_the_reauth() -> None:
    server = _server()
    attach_realtime_connection(server, resolve=_resolve)
    _commands(server)

    await asyncio.create_task(
        server._handle_connect("eio-1", "/", {"token": "first"})  # pyright: ignore[reportPrivateUsage]
    )
    [(sid, _eio)] = list(server.manager.get_participants("/", None))

    assert await _event(server, sid, "admission", {}) == {"admission": "admitted:first"}

    assert await _event(server, sid, "realtime.reauth", {"token": "second"}) == {"ok": True}
    assert await _event(server, sid, "admission", {}) == {"admission": "admitted:second"}

    # A reauth whose verification recorded nothing replaces the admission; it never keeps
    # the one before.
    assert await _event(server, sid, "realtime.reauth", {"token": "plain"}) == {"ok": True}
    assert await _event(server, sid, "admission", {}) == {"admission": None}


async def test_shared_resolutions_carry_the_admission_to_every_namespace() -> None:
    server = _server()
    attach_realtime_connection(server, resolve=_resolve, share_identity=True)
    _commands(server)

    await asyncio.create_task(
        server._handle_connect("eio-1", "/", {"token": "shared"})  # pyright: ignore[reportPrivateUsage]
    )
    [(sid, _eio)] = list(server.manager.get_participants("/", None))

    assert await _event(server, sid, "admission", {}) == {"admission": "admitted:shared"}


async def test_an_identity_resolver_connect_carries_its_admission() -> None:
    sio = StubSocketIOServer()

    async def resolve(connect: SocketIOConnect) -> AuthnIdentity:
        _record(_token(connect.auth))
        return AuthnIdentity(principal_id=_PRINCIPAL)

    _commands(sio, identity_resolver=resolve)

    await asyncio.create_task(sio.handlers[("/", "connect")]("sid-1", {}, {"token": "routed"}))

    ack = await asyncio.create_task(sio.handlers[("/", "admission")]("sid-1", {}))
    assert ack == {"admission": "admitted:routed"}


async def test_a_connect_that_records_nothing_carries_nothing_it_inherited() -> None:
    # The connect's task may already hold an admission (a middleware's, another
    # verification's); a resolution that verifies nothing must not hand it to the commands.
    server = _server()
    attach_realtime_connection(server, resolve=_resolve)
    _commands(server)

    async def connect() -> None:
        record_authn_admission("inherited")
        await server._handle_connect("eio-1", "/", {"token": "plain"})  # pyright: ignore[reportPrivateUsage]

    await asyncio.create_task(connect())
    [(sid, _eio)] = list(server.manager.get_participants("/", None))

    assert await _event(server, sid, "admission", {}) == {"admission": None}


async def test_an_identity_resolver_connect_carries_nothing_it_inherited() -> None:
    sio = StubSocketIOServer()

    async def resolve(connect: SocketIOConnect) -> AuthnIdentity:
        _record(_token(connect.auth))
        return AuthnIdentity(principal_id=_PRINCIPAL)

    _commands(sio, identity_resolver=resolve)

    async def connect() -> None:
        record_authn_admission("inherited")
        await sio.handlers[("/", "connect")]("sid-1", {}, {"token": "plain"})

    await asyncio.create_task(connect())

    ack = await asyncio.create_task(sio.handlers[("/", "admission")]("sid-1", {}))
    assert ack == {"admission": None}
