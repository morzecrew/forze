"""Minting over a socket re-checks the token that authenticated the connection.

A socket verifies its token once, at connect, and runs each command in a task of its own. With
the session fast path, a tenant switch or an API-key issue re-reads the caller's session first;
over a socket that caller is the connection's token, carried to the command by the transport.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest
from pydantic import BaseModel

pytest.importorskip("jwt")
pytest.importorskip("redis")
pytest.importorskip("argon2")

from forze.application.contracts.authn import AccessTokenCredentials, AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.execution.operations.registry import OperationRegistry
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis import RedisClient
from forze_socketio.routing import ForzeSocketIOAdapter, SocketIOConnect, SocketIONamespaceRouter
from tests.integration.test_forze_authn.test_pg_eligibility_skip import SPEC, TX, _Stack, _stack
from tests.support import eligibility_skip_conformance as battery
from tests.unit.test_forze_socketio.test_routing import StubSocketIOServer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ----------------------- #


class _MintPayload(BaseModel):
    tenant: UUID


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Mint(Handler[_MintPayload, Any]):
    """Switch the caller's tenant: issue a session for the identity the socket bound."""

    stack: _Stack

    async def __call__(self, args: _MintPayload) -> Any:
        identity = self.stack.ctx.inv_ctx.get_authn()
        assert identity is not None
        lifecycle = self.stack.ctx.authn.token_lifecycle(SPEC)

        async with self.stack.ctx.tx_ctx.scope(TX):
            await lifecycle.issue_tokens(identity, tenant_id=args.tenant)

        return {"ok": True}


async def test_a_socket_cannot_mint_after_its_session_is_revoked(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # A socket verifies its token once, at connect, and runs each command in a task of its
    # own: the command re-checks the session that authenticated the connection.
    stack = await _stack(pg_client, redis_client)
    principal = await stack.principal()
    issued = await battery.login(stack, principal)
    sio = StubSocketIOServer()

    async def resolve(connect: SocketIOConnect) -> AuthnIdentity:
        result = await stack.ctx.authn.authn(SPEC).authenticate_with_token(
            AccessTokenCredentials(token=connect.auth["token"])  # type: ignore[index]
        )
        return result.identity

    ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=lambda _request: stack.ctx,
        operation_resolver=OperationRegistry(
            handlers={"authn.mint": lambda ctx: _Mint(stack=stack)},
        )
        .freeze()
        .resolve,
        identity_resolver=resolve,
    ).include_router(
        SocketIONamespaceRouter(namespace="/rt").command(
            event="mint", operation="authn.mint", payload_type=_MintPayload
        )
    )

    def event(name: str, *args: Any) -> Awaitable[Any]:
        # python-socketio runs every event in a task of its own.
        return asyncio.create_task(stack.run(lambda: sio.handlers[("/rt", name)](*args)))

    await event("connect", "sid-1", {}, {"token": issued.access.token.token})

    assert await event("mint", "sid-1", {"tenant": str(uuid4())}) == {"ok": True}

    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=principal)
        )
    )

    ack = await event("mint", "sid-1", {"tenant": str(uuid4())})
    assert ack["error"]["code"] == "session_revoked", ack
