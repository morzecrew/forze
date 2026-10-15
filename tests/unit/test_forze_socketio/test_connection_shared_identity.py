"""``share_identity``: the namespaces of one Engine.IO connection authenticate once."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from socketio.async_server import AsyncServer
from socketio.exceptions import ConnectionRefusedError as SocketIOConnectionRefusedError

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import exc
from forze_socketio import (
    CONNECTION_SESSION_KEY,
    RealtimeConnection,
    SocketIOConnect,
    attach_realtime_connection,
)

# ----------------------- #

_PRINCIPAL = UUID("11111111-1111-1111-1111-111111111111")
_LATER = datetime(2099, 1, 1, tzinfo=UTC)


class _Resolver:
    """Counts the credentials it verifies; ``bad`` tokens are refused."""

    def __init__(self, *, expires_at: datetime = _LATER) -> None:
        self.seen: list[str] = []
        self.bad: set[str] = set()
        self.expires_at = expires_at
        self.gate: asyncio.Event | None = None

    async def __call__(self, connect: SocketIOConnect) -> RealtimeConnection:
        token = connect.auth["token"]
        self.seen.append(token)

        if self.gate is not None:
            await self.gate.wait()

        if token in self.bad:
            raise exc.authentication("Invalid access token")

        return RealtimeConnection(
            authn=AuthnIdentity(principal_id=_PRINCIPAL), expires_at=self.expires_at
        )


def _server(
    resolver: _Resolver, *, share: bool = True, namespaces: tuple[str, ...] = ("/lobby", "/chat")
) -> AsyncServer:
    server = AsyncServer()

    for namespace in namespaces:
        attach_realtime_connection(
            server, namespace=namespace, resolve=resolver, share_identity=share
        )

    return server


async def _connect(server: AsyncServer, eio_sid: str, namespace: str, token: str) -> str:
    """Connect *namespace* over Engine.IO connection *eio_sid*, as python-socketio would."""

    if eio_sid not in server.eio.sockets:
        server.eio.sockets[eio_sid] = SimpleNamespace(session={}, closed=False, closing=False)

    sid = await server.manager.connect(eio_sid, namespace)
    handler = server.handlers[namespace]["connect"]
    await handler(sid, {}, {"token": token})
    return sid


async def _identity(server: AsyncServer, sid: str, namespace: str) -> RealtimeConnection:
    session = await server.get_session(sid, namespace=namespace)
    return session[CONNECTION_SESSION_KEY]


# ....................... #


async def test_two_namespaces_with_one_token_authenticate_once() -> None:
    resolver = _Resolver()
    server = _server(resolver)

    lobby = await _connect(server, "eio-1", "/lobby", "t1")
    chat = await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1"]
    assert (await _identity(server, chat, "/chat")).principal == str(_PRINCIPAL)
    assert await _identity(server, lobby, "/lobby") is await _identity(server, chat, "/chat")


async def test_a_different_token_on_the_same_connection_authenticates_again() -> None:
    resolver = _Resolver()
    server = _server(resolver)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-1", "/chat", "t2")

    assert resolver.seen == ["t1", "t2"]


async def test_another_connection_with_the_same_token_authenticates_again() -> None:
    resolver = _Resolver()
    server = _server(resolver)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-2", "/chat", "t1")

    assert resolver.seen == ["t1", "t1"]


async def test_without_the_option_every_namespace_authenticates() -> None:
    resolver = _Resolver()
    server = _server(resolver, share=False)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1", "t1"]


async def test_namespaces_connecting_together_share_one_check() -> None:
    resolver = _Resolver()
    resolver.gate = asyncio.Event()
    server = _server(resolver)

    both = asyncio.gather(
        _connect(server, "eio-1", "/lobby", "t1"), _connect(server, "eio-1", "/chat", "t1")
    )
    await asyncio.sleep(0.01)
    resolver.gate.set()
    await both

    assert resolver.seen == ["t1"]


async def test_a_cancelled_connect_does_not_cancel_the_check_another_awaits() -> None:
    resolver = _Resolver()
    resolver.gate = asyncio.Event()
    server = _server(resolver)

    first = asyncio.create_task(_connect(server, "eio-1", "/lobby", "t1"))
    await asyncio.sleep(0.01)
    second = asyncio.create_task(_connect(server, "eio-1", "/chat", "t1"))
    await asyncio.sleep(0.01)

    first.cancel()
    resolver.gate.set()
    chat = await second

    assert first.cancelled()
    assert resolver.seen == ["t1"]
    assert (await _identity(server, chat, "/chat")).principal == str(_PRINCIPAL)


async def test_a_refused_credential_is_not_remembered() -> None:
    resolver = _Resolver()
    resolver.bad.add("t1")
    server = _server(resolver)

    with pytest.raises(SocketIOConnectionRefusedError):
        await _connect(server, "eio-1", "/lobby", "t1")

    resolver.bad.clear()
    await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1", "t1"]


async def test_an_expired_identity_is_resolved_again() -> None:
    resolver = _Resolver(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    server = _server(resolver)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1", "t1"]


async def test_namespaces_with_different_resolvers_do_not_share() -> None:
    lobby, chat = _Resolver(), _Resolver()
    server = AsyncServer()
    attach_realtime_connection(server, namespace="/lobby", resolve=lobby, share_identity=True)
    attach_realtime_connection(server, namespace="/chat", resolve=chat, share_identity=True)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-1", "/chat", "t1")

    assert (lobby.seen, chat.seen) == (["t1"], ["t1"])


async def test_an_uncomparable_credential_is_authenticated_on_its_own() -> None:
    seen: list[Any] = []

    async def _resolve(connect: SocketIOConnect) -> RealtimeConnection:
        seen.append(connect.auth)
        return RealtimeConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))

    server = AsyncServer()

    for namespace in ("/lobby", "/chat"):
        attach_realtime_connection(
            server, namespace=namespace, resolve=_resolve, share_identity=True
        )

    server.eio.sockets["eio-1"] = SimpleNamespace(session={}, closed=False, closing=False)

    for namespace in ("/lobby", "/chat"):
        sid = await server.manager.connect("eio-1", namespace)
        # The same payload twice, but one no fingerprint can be taken of (a non-string key).
        await server.handlers[namespace]["connect"](sid, {}, {1: "t1"})

    assert len(seen) == 2


async def test_an_unhashable_resolver_still_shares() -> None:
    class _Unhashable(_Resolver):
        __hash__ = None  # type: ignore[assignment]  # defines equality without a hash

        def __eq__(self, other: object) -> bool:
            return isinstance(other, _Unhashable)

    resolver = _Unhashable()
    server = _server(resolver)

    await _connect(server, "eio-1", "/lobby", "t1")
    await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1"]


async def test_a_failure_nobody_was_waiting_for_is_not_replayed() -> None:
    resolver = _Resolver()
    resolver.bad.add("t1")
    resolver.gate = asyncio.Event()
    server = _server(resolver)

    first = asyncio.create_task(_connect(server, "eio-1", "/lobby", "t1"))
    await asyncio.sleep(0.01)
    first.cancel()  # the only waiter goes away while the check is in flight
    resolver.gate.set()
    await asyncio.sleep(0.01)  # and the check then fails

    resolver.bad.clear()
    await _connect(server, "eio-1", "/chat", "t1")

    assert resolver.seen == ["t1", "t1"]
