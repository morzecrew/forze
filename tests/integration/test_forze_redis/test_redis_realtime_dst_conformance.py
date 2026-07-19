"""The gateway crash-delivery scenario over a real Redis stream — the differential leg.

The same `forze_dst.conformance.run_gateway_crash_delivery` scenario that passes against the
mock, pointed at real Redis consumer-group semantics (XREADGROUP `>` delivery, the PEL across
a crashed consumer, XAUTOCLAIM recovery, XACK). The bridge keeps its dedup mark and mailbox on
a mock context — the production shape splits exactly there (broker on Redis, inbox/mailbox on
the database), and the store side already has its own mock↔Postgres differential. Asserting
the same `GatewayDeliveryOutcome` on both legs turns "the mock's ack-stream survives a crash"
into "the mock's ack-stream matches the engine that actually keeps the pending-entries list".
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.deps import Deps
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamSpec,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze.base.serialization import PydanticModelCodec
from forze.testing import context_from_modules
from forze_dst.conformance import (
    REALTIME_DELIVERY_PRINCIPAL,
    REALTIME_DELIVERY_SIGNALS,
    GatewayCrashPoint,
    GatewayDeliveryOutcome,
    run_gateway_crash_delivery,
)
from forze_kits.integrations.realtime import build_realtime_mailbox, realtime_inbox_spec
from forze_mock import MockDepsModule, MockState
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient
from forze_socketio import GatewayDedup, RealtimeGateway

# ----------------------- #

pytestmark = pytest.mark.asyncio

_N = len(REALTIME_DELIVERY_SIGNALS)


class _RecordingSio:
    def __init__(self) -> None:
        self.ids: list[str | None] = []

    async def emit(self, event: str, data: Any = None, **_: Any) -> None:
        self.ids.append(data["id"])


class _NullSource:
    async def run(self, ctx: Any, handler: Any, *, stop: Any = None) -> None:  # pragma: no cover
        raise NotImplementedError


def _redis_stream_runtime(redis_client: RedisClient) -> ExecutionRuntime:
    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))
    writer = RedisStreamAdapter(client=redis_client, codec=codec)
    group = RedisStreamGroupAdapter(client=redis_client, codec=codec)
    admin = RedisStreamGroupAdminAdapter(client=redis_client)

    def module() -> Deps:
        return Deps.plain(
            {
                StreamCommandDepKey: lambda _ctx, _spec: writer,
                AckStreamGroupQueryDepKey: lambda _ctx, _spec: group,
                AckStreamGroupAdminDepKey: lambda _ctx, _spec: admin,
            }
        )

    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


async def _run(
    redis_client: RedisClient, *, crash: GatewayCrashPoint
) -> GatewayDeliveryOutcome:
    # Stream ports on real Redis; the bridge's dedup mark + mailbox on a mock context —
    # the production split (broker vs database), and the store side has its own differential.
    stream_runtime = _redis_stream_runtime(redis_client)
    store_ctx = context_from_modules(MockDepsModule(state=MockState()))

    sio = _RecordingSio()
    gateway = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=_NullSource(),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
    )
    mailbox = build_realtime_mailbox(store_ctx)

    async def bridge(
        signal: RealtimeSignal, tenant: UUID | None, dedup_id: str | None, hlc: HlcTimestamp
    ) -> None:
        await gateway._handle(store_ctx, mailbox, signal, tenant, dedup_id, hlc)  # pyright: ignore[reportPrivateUsage]

    async def _rows() -> int:
        return len(await mailbox.read_since(principal=REALTIME_DELIVERY_PRINCIPAL, since=None))

    spec: StreamSpec[RealtimeSignal] = StreamSpec(
        # a fresh stream key per case — the container's keyspace is shared across tests
        name=f"dst-realtime-{uuid4().hex[:12]}",
        codec=PydanticModelCodec(model_type=RealtimeSignal),
    )

    async with stream_runtime.scope():
        return await run_gateway_crash_delivery(
            stream_runtime.get_context(),
            stream_spec=spec,
            bridge=bridge,
            crash=crash,
            emitted_ids=lambda: sio.ids,
            mailbox_rows=_rows,
        )


# ----------------------- #


class TestRedisGatewayCrashDelivery:
    """Every expected outcome below is byte-identical to the mock leg's — the differential."""

    async def test_crash_before_bridge_recovers_every_signal_once(
        self, redis_client: RedisClient
    ) -> None:
        outcome = await _run(redis_client, crash=GatewayCrashPoint.BEFORE_BRIDGE)
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=_N,
            emitted=_N,
            distinct_emitted=_N,
            mailboxed=_N,
            pending_after=0,
        )

    async def test_crash_between_commit_and_ack_dedups_the_redelivery(
        self, redis_client: RedisClient
    ) -> None:
        outcome = await _run(redis_client, crash=GatewayCrashPoint.AFTER_BRIDGE_BEFORE_ACK)
        assert outcome == GatewayDeliveryOutcome(
            appended=_N,
            deliveries=2 * _N,
            emitted=_N,
            distinct_emitted=_N,
            mailboxed=_N,
            pending_after=0,
        )
