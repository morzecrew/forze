"""A WebSocket command runs with invocation metadata of its own, as an HTTP request does.

# covers: forze_fastapi.realtime.ws (cmd dispatch through invoke_at_ingress)

The invocation middleware passes the socket's scope through untouched, so nothing ambient
carries an execution id: each command frame mints its own, and the ids reach the operation's
logs, spans and the integration events it publishes.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationRegistry
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeCommandRoute,
)
from forze_fastapi.realtime import WsConnect, WsConnection, attach_realtime_ws_route
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #

_PRINCIPAL = uuid4()
_TENANT = uuid4()


class _Empty(BaseModel):
    pass


def _seen(ctx: ExecutionContext) -> Any:
    async def handler(_args: _Empty) -> Any:
        metadata = ctx.inv_ctx.get_metadata()
        authn = ctx.inv_ctx.get_authn()
        tenant = ctx.inv_ctx.get_tenant()

        return {
            "execution_id": str(metadata.execution_id) if metadata is not None else None,
            "correlation_id": str(metadata.correlation_id) if metadata is not None else None,
            "principal_id": str(authn.principal_id) if authn is not None else None,
            "tenant_id": str(tenant.tenant_id) if tenant is not None else None,
            "idempotency_key": ctx.inv_ctx.get_idempotency_key(),
            "has_deadline": ctx.inv_ctx.get_deadline() is not None,
        }

    return handler


async def _resolve(_connect: WsConnect) -> WsConnection:
    return WsConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL), tenant=_TENANT)


def _client() -> TestClient:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: ctx,
        resolve=_resolve,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        registry=OperationRegistry().set_handler("seen", _seen).freeze(),
        commands=(
            RealtimeCommandRoute[Any, Any](event="seen", operation="seen", payload_type=_Empty),
        ),
    )
    app = FastAPI()
    app.include_router(router)

    return TestClient(app)


def _command(ws: Any, cid: str, **frame: Any) -> Any:
    ws.send_json({"type": "cmd", "event": "seen", "cid": cid, "payload": {}, **frame})
    ack = ws.receive_json()
    assert ack["cid"] == cid, ack

    return ack["data"]


# ----------------------- #


def test_each_command_runs_with_fresh_invocation_metadata() -> None:
    with _client().websocket_connect("/realtime/ws") as ws:
        first = _command(ws, "c1")
        second = _command(ws, "c2", idempotency_key="k-1", deadline_budget=30)

    assert first["execution_id"] is not None and first["correlation_id"] is not None
    assert second["execution_id"] is not None and second["correlation_id"] is not None
    # One socket, two invocations: neither id is shared between commands.
    assert first["execution_id"] != second["execution_id"]
    assert first["correlation_id"] != second["correlation_id"]
    # The identity and the per-frame envelope are bound as before.
    assert first["principal_id"] == second["principal_id"] == str(_PRINCIPAL)
    assert first["tenant_id"] == second["tenant_id"] == str(_TENANT)
    assert first["idempotency_key"] is None
    assert second["idempotency_key"] == "k-1"
    assert first["has_deadline"] is False
    assert second["has_deadline"] is True
