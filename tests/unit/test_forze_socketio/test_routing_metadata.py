"""A Socket.IO command event runs with invocation metadata of its own, as an HTTP request does.

# covers: forze_socketio.routing (_dispatch_event)

Each event is an invocation of its own, so it mints its own execution and correlation ids, and
the ids reach the operation's logs, spans and the integration events it publishes. The adapter
keeps dispatching through its ``operation_resolver``.
"""

from typing import Any
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze_socketio.routing import (
    IDENTITY_SESSION_KEY,
    ForzeSocketIOAdapter,
    SocketIONamespaceRouter,
)
from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_socketio.test_routing import StubSocketIOServer

pytestmark = pytest.mark.unit

# ----------------------- #


class _Empty(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Seen(Handler[_Empty, Any]):
    ctx: ExecutionContext

    async def __call__(self, args: _Empty) -> Any:
        metadata = self.ctx.inv_ctx.get_metadata()
        authn = self.ctx.inv_ctx.get_authn()

        return {
            "execution_id": str(metadata.execution_id) if metadata is not None else None,
            "correlation_id": str(metadata.correlation_id) if metadata is not None else None,
            "principal_id": str(authn.principal_id) if authn is not None else None,
        }


def _seen_handler() -> Any:
    sio = StubSocketIOServer()
    ForzeSocketIOAdapter(
        sio=sio,  # pyright: ignore[reportArgumentType]
        context_factory=lambda _request: context_from_deps(Deps()),
        operation_resolver=OperationRegistry(handlers={"seen": lambda ctx: _Seen(ctx=ctx)})
        .freeze()
        .resolve,
    ).include_router(
        SocketIONamespaceRouter(namespace="/").command(
            event="seen", operation="seen", payload_type=_Empty
        )
    )

    return sio, sio.handlers[("/", "seen")]


# ----------------------- #


@pytest.mark.asyncio
async def test_each_event_runs_with_fresh_invocation_metadata() -> None:
    principal = uuid4()
    sio, seen = _seen_handler()
    sio.sessions[("/", "sid-1")] = {IDENTITY_SESSION_KEY: AuthnIdentity(principal_id=principal)}

    first = await seen("sid-1", {})
    second = await seen("sid-1", {})

    assert first["execution_id"] is not None and first["correlation_id"] is not None
    assert second["execution_id"] is not None and second["correlation_id"] is not None
    # One connection, two invocations: neither id is shared between events.
    assert first["execution_id"] != second["execution_id"]
    assert first["correlation_id"] != second["correlation_id"]
    # The session identity is bound as before.
    assert first["principal_id"] == second["principal_id"] == str(principal)


@pytest.mark.asyncio
async def test_an_anonymous_event_gets_metadata_too() -> None:
    _sio, seen = _seen_handler()

    ack = await seen("sid-1", {})

    assert ack["execution_id"] is not None and ack["correlation_id"] is not None
    assert ack["principal_id"] is None
