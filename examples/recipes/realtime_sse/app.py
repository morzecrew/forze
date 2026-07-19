"""Recipe: realtime egress over SSE — the browser-native transport, one shared contract.

The egress plane is transport-plural: handlers publish ``RealtimeSignal`` s and never
learn a transport exists. This recipe serves the same mailbox the Socket.IO gateway
fills through ``attach_realtime_sse_route`` — an authenticated ``text/event-stream``
endpoint that replays everything past the device's cursor as SSE frames (``id:`` =
durable event id, ``data:`` = the shared ``{id, data}`` envelope) and a small
``POST …/ack`` endpoint for the cumulative ack. The browser's native ``EventSource``
reconnect header, ``Last-Event-ID``, takes precedence over the stored cursor — the
browser resumes itself.

Here a test client plays the browser: connect (drain the backlog), ack, reconnect
(nothing re-replayed), resume via ``Last-Event-ID``. In production add
``SecurityContextMiddleware`` (this example binds a fixed principal in a stub
middleware) and ``realtime_sse_tail_lifecycle_step`` for live signals after replay.

Run it:  uv run python -m examples.recipes.realtime_sse.app
Exercised by tests/unit/test_examples/test_realtime_sse.py.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.base.primitives import HlcTimestamp, uuid7
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.realtime import attach_realtime_sse_route
from forze_mock import MockDepsModule

_LOGGER_NAME = "realtime_sse"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Only when run as a script — global logging stays untouched for imports/tests.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:setup]
BOB = UUID("22222222-2222-2222-2222-222222222222")  # the authenticated principal


class _BindPrincipal:
    """Stand-in for ``SecurityContextMiddleware``: bind the authenticated identity.

    The SSE route reads identity from the bound context and never parses credentials
    itself — in production the security middleware does this from real credentials.
    """

    def __init__(self, app: ASGIApp, *, ctx: ExecutionContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=BOB)):
            await self.app(scope, receive, send)


def build_app() -> tuple[FastAPI, InMemoryRealtimeMailbox]:
    """The SSE egress app: one attach call, plus the mailbox the gateway would fill."""

    ctx = ExecutionContext(deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve())
    mailbox = InMemoryRealtimeMailbox()
    cursors = InMemoryMailboxCursors()

    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda _ctx: mailbox,
        cursors_factory=lambda _ctx: cursors,
        # hub=… + realtime_sse_tail_lifecycle_step(hub, stream_spec=…) adds the live leg
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    app.add_middleware(_BindPrincipal, ctx=ctx)  # type: ignore[arg-type]

    return app, mailbox


# --8<-- [end:setup]


# --8<-- [start:client]
async def seed_mailbox(mailbox: InMemoryRealtimeMailbox, texts: list[str]) -> list[str]:
    """What the Socket.IO gateway does for durable principal signals: store for replay."""

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


def connect(client: TestClient, *, last_event_id: str | None = None) -> list[dict[str, Any]]:
    """The browser's EventSource connect: drain the replay into ``{id, event, data}`` frames."""

    headers = {"Last-Event-ID": last_event_id} if last_event_id else {}
    response = client.get("/realtime/sse", headers=headers)
    response.raise_for_status()

    frames: list[dict[str, Any]] = []

    for block in response.text.split("\n\n"):
        if not block.strip() or block.startswith(":"):
            continue

        frame: dict[str, Any] = {}

        for line in block.splitlines():
            field, _, value = line.partition(": ")
            frame[field] = json.loads(value) if field == "data" else value

        frames.append(frame)

    return frames


def ack(client: TestClient, *, up_to: str) -> bool:
    """The cumulative ack — SSE has no upstream channel, so it rides a plain POST."""

    response = client.post("/realtime/sse/ack", json={"up_to": up_to})
    response.raise_for_status()

    return bool(response.json()["acked"])


# --8<-- [end:client]


async def main() -> None:
    app, mailbox = build_app()
    client = TestClient(app)

    ids = await seed_mailbox(mailbox, ["packed", "shipped", "delivered"])
    log.info("mailbox seeded while the device was offline", count=len(ids))

    frames = connect(client)
    log.info("reconnect drained the backlog", events=[f["data"]["data"]["text"] for f in frames])

    frames = connect(client, last_event_id=ids[0])
    log.info(
        "Last-Event-ID resumes precisely, beating the stored cursor",
        events=[f["data"]["data"]["text"] for f in frames],
    )

    ack(client, up_to=ids[1])
    log.info("acked through the second event", up_to=ids[1])

    frames = connect(client)
    log.info("only the unacked tail re-replays", events=[f["data"]["data"]["text"] for f in frames])


if __name__ == "__main__":
    import asyncio

    _setup_logging("info")
    asyncio.run(main())
