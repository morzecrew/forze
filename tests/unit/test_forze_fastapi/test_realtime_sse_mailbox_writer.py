"""Offline delivery over SSE with no Socket.IO server: the mailbox writer fills what SSE replays.

# covers: forze.application.execution.realtime.writer (end to end with
#         forze_fastapi.realtime.sse — replay on reconnect, cumulative ack)

A durable signal is staged and relayed while the client is disconnected; the writer — the
only mailbox writer in the process — stores it; the client reconnects, is replayed it, acks
it, and the next reconnect replays nothing. No ``forze_socketio`` anywhere in the wiring,
and a subprocess pins that importing the SSE-only stack never pulls Socket.IO in. The same
round trip, instrumented, shows the mailbox metrics reach a per-connect factory and the
writer's own mailbox.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, FastAPI
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import BaseModel
from starlette.testclient import TestClient
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.realtime import Audience, RealtimeEvent
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.realtime import (
    GatewayDedup,
    RealtimeMailboxWriter,
    StreamGroupSignalSource,
    realtime_mailbox_writer_lifecycle_step,
)
from forze_fastapi.realtime import attach_realtime_sse_route
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    CursorDocumentSpec,
    MailboxDocumentSpec,
    build_realtime_cursors,
    build_realtime_mailbox,
    build_realtime_publisher,
    build_realtime_transport,
    instrument_realtime_mailbox,
    realtime_cursor_spec,
    realtime_group_ensure_lifecycle_step,
    realtime_mailbox_spec,
)
from forze_mock import MockDepsModule
from tests.support.realtime_retention import UNSWEPT

# ----------------------- #

_PRINCIPAL = uuid4()


class _Shipped(BaseModel):
    text: str


_ORDER_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_Shipped)


class _BindIdentity:
    """Stands in for the security middleware: binds one fixed identity per request."""

    def __init__(self, app: ASGIApp, *, ctx: ExecutionContext) -> None:
        self.app = app
        self.ctx = ctx

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # pragma: no cover - lifespan only
            await self.app(scope, receive, send)
            return

        with self.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=_PRINCIPAL)):
            await self.app(scope, receive, send)


def _frames(body: str) -> list[dict[str, Any]]:
    """Parse SSE frames into ``{id, event, data}`` dicts (comments and retry lines skipped)."""

    parsed: list[dict[str, Any]] = []

    for block in body.split("\n\n"):
        if not block.strip() or block.startswith(":"):
            continue

        frame: dict[str, Any] = {}

        for line in block.splitlines():
            field, _, value = line.partition(": ")
            frame[field] = json.loads(value) if field == "data" else value

        parsed.append(frame)

    return parsed


# ----------------------- #


class TestSseOfflineDeliveryWithoutSocketIO:
    async def test_a_signal_sent_while_disconnected_is_replayed_on_reconnect_and_acked(
        self,
    ) -> None:
        await _round_trip(realtime_mailbox_spec(), realtime_cursor_spec())

    async def test_the_writer_and_every_connect_reach_the_mailbox_metrics(self) -> None:
        # The process keeps one count per spec, so a spec of its own keeps other tests out.
        channel = f"sse-{uuid4().hex[:8]}"
        mailbox_spec, cursor_spec = realtime_mailbox_spec(channel), realtime_cursor_spec(channel)
        reader = InMemoryMetricReader()
        instrument_realtime_mailbox(meter=MeterProvider(metric_readers=[reader]).get_meter("t"))

        await _round_trip(mailbox_spec, cursor_spec)

        data = reader.get_metrics_data()
        assert data is not None
        points = {
            (metric.name, point.attributes["forze.document"]): point
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            for point in metric.data.data_points
        }
        mailbox, cursors = str(mailbox_spec.name), str(cursor_spec.name)

        assert points["forze.realtime.mailbox.stored", mailbox].value == 1  # the writer's
        assert points["forze.realtime.mailbox.replayed", mailbox].value == 1
        assert points["forze.realtime.mailbox.acked", cursors].value == 1
        assert points["forze.realtime.mailbox.trimmed", mailbox].value == 1

        # three connects, each through a mailbox its factory built: 0, then 1, then 0
        replay_size = points["forze.realtime.mailbox.replay_size", mailbox]
        assert (replay_size.count, replay_size.sum, replay_size.max) == (3, 1, 1)


def test_the_sse_only_stack_never_imports_socketio() -> None:
    code = (
        "import sys\n"
        "import forze.application.execution.realtime\n"
        "import forze_fastapi.realtime\n"
        "import forze_kits.integrations.realtime\n"
        "leaked = sorted(m for m in sys.modules if m.split('.')[0] in {'socketio', "
        "'forze_socketio'})\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr


# ----------------------- #


async def _round_trip(mailbox_spec: MailboxDocumentSpec, cursor_spec: CursorDocumentSpec) -> None:
    """Connect, stage while away, reconnect for the replay, ack, reconnect caught up."""

    transport = build_realtime_transport()
    writer = RealtimeMailboxWriter(
        source=StreamGroupSignalSource(stream_spec=transport.stream_spec),
        dedup=GatewayDedup(inbox_spec=transport.inbox_spec, tx_route="mock"),
        mailbox_factory=lambda ctx: build_realtime_mailbox(
            ctx, retention=UNSWEPT, spec=mailbox_spec
        ),
    )
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze(),
        lifecycle=LifecyclePlan.from_steps(
            realtime_group_ensure_lifecycle_step(stream_spec=transport.stream_spec),
            realtime_mailbox_writer_lifecycle_step(writer),
        ).freeze(),
    )

    async with runtime.scope():
        ctx = runtime.get_context()
        client = _sse_client(ctx, mailbox_spec, cursor_spec)

        # connected, nothing waiting — then the client goes away
        assert _frames(_connect(client).text) == []

        # published while disconnected: staged with the business write, relayed after
        pub = build_realtime_publisher(
            ctx, stream_spec=transport.stream_spec, outbox_spec=transport.outbox_spec
        )
        await pub.stage(
            Audience.principal(str(_PRINCIPAL)), _ORDER_SHIPPED, _Shipped(text="shipped")
        )
        await ctx.outbox.command(transport.outbox_spec).flush()
        await OutboxRelay(outbox_spec=transport.outbox_spec).to_stream(ctx, transport.stream_spec)
        await _until_stored(ctx, mailbox_spec)

        # reconnect: the writer's row is replayed in the shared {id, data} envelope
        [frame] = _frames(_connect(client).text)
        assert frame["event"] == "order.shipped"
        assert frame["data"] == {"id": frame["id"], "data": {"text": "shipped"}}

        acked = client.post(
            "/realtime/sse/ack", json={"up_to": frame["id"]}, params={"device_id": "phone"}
        )
        assert acked.json() == {"acked": True}

        # acked: the next reconnect of that device is caught up
        assert _frames(_connect(client).text) == []


def _sse_client(
    ctx: ExecutionContext, mailbox_spec: MailboxDocumentSpec, cursor_spec: CursorDocumentSpec
) -> TestClient:
    router = APIRouter()
    attach_realtime_sse_route(
        router,
        ctx_dep=lambda: ctx,
        mailbox_factory=lambda ctx: build_realtime_mailbox(
            ctx, retention=UNSWEPT, spec=mailbox_spec
        ),
        cursors_factory=lambda ctx: build_realtime_cursors(ctx, spec=cursor_spec),
    )

    app = FastAPI()
    app.include_router(router)
    app.add_middleware(_BindIdentity, ctx=ctx)  # type: ignore[arg-type]

    return TestClient(app)


def _connect(client: TestClient) -> Any:
    # no hub: the stream is catch-up only and ends after the replay, like a reconnect cycle
    response = client.get("/realtime/sse", params={"device_id": "phone"})
    assert response.status_code == 200

    return response


async def _until_stored(ctx: ExecutionContext, spec: MailboxDocumentSpec) -> None:
    # read the rows directly: a mailbox read would count as a replay
    query = ctx.document.query(spec)

    for _ in range(300):
        if await query.find(filters={"$values": {"principal": str(_PRINCIPAL)}}):
            return

        await asyncio.sleep(0.01)

    raise AssertionError("the writer never stored the signal")
