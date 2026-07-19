"""Recipe: duplex realtime over raw WebSocket — one socket, the whole contract.

The raw-WebSocket transport carries the full realtime wire protocol on one
connection: mailbox replay on connect (frames are the shared envelope with the
event name in-band), the cumulative ack inline (``{"type": "realtime.ack"}``),
and typed **commands** dispatched through the same frozen-registry discipline as
HTTP — declared once as ``RealtimeCommandRoute`` s, acknowledged as
``{"type": "ack", "cid", "data" | "error"}``. Identity comes from an app-supplied
resolver reading the upgrade request; in production add the route's path to the
middlewares' ``allowed_websocket_paths`` (they refuse every other websocket scope).

Here a test client plays the strict-protocol peer: connect, drain the replay, ack,
send a command, read its typed ack — and see a failing command come back as the
shared error envelope, not a dropped socket.

Run it:  uv run python -m examples.recipes.realtime_ws.app
Exercised by tests/unit/test_examples/test_realtime_ws.py.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.operations import OperationRegistry
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
    RealtimeCommandRoute,
)
from forze.base.exceptions import exc
from forze.base.logging import configure_logging
from forze.base.primitives import HlcTimestamp, uuid7
from forze_fastapi.realtime import WsConnect, WsConnection, attach_realtime_ws_route
from forze_mock import MockDepsModule

_LOGGER_NAME = "realtime_ws"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: Any) -> None:
    # Only when run as a script — global logging stays untouched for imports/tests.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:setup]
BOB = UUID("22222222-2222-2222-2222-222222222222")  # the authenticated principal


class CreateNote(BaseModel):
    text: str


class NoteCreated(BaseModel):
    note_id: str


async def create_note(args: CreateNote) -> NoteCreated:
    """An ordinary registry operation — the transport never touches domain code."""

    if not args.text.strip():
        raise exc.validation("Note text must not be blank", code="note_blank")

    return NoteCreated(note_id=f"note:{args.text}")


async def resolve_connection(connect: WsConnect) -> WsConnection | None:
    """Authenticate the upgrade request (and any ``realtime.reauth`` payload)."""

    token = (connect.auth or {}).get("token") or connect.websocket.query_params.get("token")

    if token != "good-token":
        raise exc.authentication("Bad realtime token")

    return WsConnection(authn=AuthnIdentity(principal_id=BOB))


def build_app() -> tuple[FastAPI, InMemoryRealtimeMailbox]:
    """One attach call: egress replay + live seams + governed command ingress."""

    ctx = ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())
    mailbox = InMemoryRealtimeMailbox()

    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: ctx,
        resolve=resolve_connection,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        registry=OperationRegistry().set_handler("note.create", lambda ctx: create_note).freeze(),
        commands=(
            RealtimeCommandRoute[Any, Any](
                event="note.create",
                operation="note.create",
                payload_type=CreateNote,
                ack_type=NoteCreated,
            ),
        ),
    )

    app = FastAPI()
    app.include_router(router)

    return app, mailbox


# --8<-- [end:setup]


# --8<-- [start:client]
async def seed_mailbox(mailbox: InMemoryRealtimeMailbox, texts: list[str]) -> list[str]:
    """What the gateway does for durable principal signals: store for replay."""

    ids: list[str] = []

    for i, text in enumerate(texts):
        event_id = str(uuid7())
        ids.append(event_id)
        await mailbox.store(
            principal=str(BOB),
            event_id=event_id,
            hlc=HlcTimestamp(physical_ms=i + 1, logical=0),
            signal=RealtimeSignal.of(Audience.principal(str(BOB)), "order.shipped", {"text": text}),
        )

    return ids


def run_session(app: FastAPI, *, replay_count: int) -> dict[str, Any]:
    """The strict-protocol peer: replay → ack → command → typed/err acks."""

    client = TestClient(app)
    out: dict[str, Any] = {}

    with client.websocket_connect("/realtime/ws?token=good-token&device_id=d1") as ws:
        out["replayed"] = [ws.receive_json() for _ in range(replay_count)]

        if out["replayed"]:
            ws.send_json({"type": "realtime.ack", "up_to": out["replayed"][-1]["id"]})

        ws.send_json(
            {"type": "cmd", "event": "note.create", "cid": "c1", "payload": {"text": "ship it"}}
        )
        out["command_ack"] = ws.receive_json()

        ws.send_json(
            {"type": "cmd", "event": "note.create", "cid": "c2", "payload": {"text": "  "}}
        )
        out["error_ack"] = ws.receive_json()

    return out


# --8<-- [end:client]


async def main() -> None:
    app, mailbox = build_app()
    ids = await seed_mailbox(mailbox, ["packed", "shipped"])
    log.info("mailbox seeded while the peer was offline", count=len(ids))

    out = run_session(app, replay_count=2)
    log.info("replay drained", events=[f["data"]["text"] for f in out["replayed"]])
    log.info("command acknowledged", ack=out["command_ack"])
    log.info("failing command came back as the shared envelope", error=out["error_ack"]["error"])

    out = run_session(app, replay_count=0)
    log.info("acked backlog does not re-replay", replayed=len(out["replayed"]))


if __name__ == "__main__":
    import asyncio

    _setup_logging("info")
    asyncio.run(main())
