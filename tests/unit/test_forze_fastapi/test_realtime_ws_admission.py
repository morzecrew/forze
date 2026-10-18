"""A WebSocket's commands run with the admission its credential's verification recorded.

# covers: forze_fastapi.realtime.ws (_resolve at the handshake and at reauth, cmd dispatch)

The socket verifies its credential at the handshake (and reauth) and runs each command in a task
of its own, so what the verification recorded travels with the connection: an issue a command
makes is checked against the credential that authenticated the socket.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.contracts.authn import (
    AuthnIdentity,
    current_authn_admission,
    record_authn_admission,
)
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


class _Empty(BaseModel):
    pass


async def _admission(_args: _Empty) -> Any:
    return {"admission": current_authn_admission()}


async def _resolve(connect: WsConnect) -> WsConnection:
    token = (connect.auth or {}).get("token") or connect.websocket.query_params.get("token")

    # A verification records what it admitted; "plain" stands for a credential with none.
    if token != "plain":
        record_authn_admission(f"admitted:{token}")

    return WsConnection(authn=AuthnIdentity(principal_id=_PRINCIPAL))


def _client() -> TestClient:
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: ctx,
        resolve=_resolve,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        registry=OperationRegistry().set_handler("admission", lambda _ctx: _admission).freeze(),
        commands=(
            RealtimeCommandRoute[Any, Any](
                event="admission", operation="admission", payload_type=_Empty
            ),
        ),
    )
    app = FastAPI()
    app.include_router(router)

    return TestClient(app)


def _command(ws: Any, cid: str) -> Any:
    ws.send_json({"type": "cmd", "event": "admission", "cid": cid, "payload": {}})
    ack = ws.receive_json()
    assert ack["cid"] == cid, ack

    return ack["data"]["admission"]


def _reauth(ws: Any, token: str) -> None:
    ws.send_json({"type": "realtime.reauth", "cid": f"r-{token}", "auth": {"token": token}})
    assert ws.receive_json()["data"] == {"ok": True}


# ----------------------- #


def test_commands_run_with_the_admission_of_the_handshake_and_then_of_the_reauth() -> None:
    with _client().websocket_connect("/realtime/ws?token=first") as ws:
        assert _command(ws, "c1") == "admitted:first"

        _reauth(ws, "second")
        assert _command(ws, "c2") == "admitted:second"

        # A reauth whose verification recorded nothing replaces the admission; it never
        # keeps the one before.
        _reauth(ws, "plain")
        assert _command(ws, "c3") is None
