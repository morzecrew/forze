"""The transport-neutral mailbox writer — the gateway's store half, with no live leg.

# covers: forze.application.execution.realtime.writer (RealtimeMailboxWriter,
#         realtime_mailbox_writer_lifecycle_step), forze.application.execution.realtime.delivery
#         (RealtimeDelivery, claim_dedup_inbox)

The writer runs the same delivery core as the Socket.IO gateway (the crash-delivery battery
in ``test_realtime_delivery_battery.py`` drives both through one scenario); these tests pin
what is specific to running it with no emit: only replay-safe signals are touched, and the
tenancy rules hold on the store alone.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID, HEADER_HLC, HEADER_TENANT_ID
from forze.application.contracts.realtime import (
    Audience,
    RealtimeEvent,
    RealtimeEventCatalog,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.realtime import (
    GatewayDedup,
    PubSubSignalSource,
    RealtimeGatewayStats,
    RealtimeMailboxWriter,
    RealtimeSignalSource,
    StreamGroupSignalSource,
    TenantShardedSignalSource,
    claim_dedup_inbox,
    realtime_mailbox_writer_lifecycle_step,
)
from forze.application.integrations.realtime import InMemoryRealtimeMailbox, RealtimeMailbox
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    build_realtime_mailbox,
    realtime_inbox_spec,
    realtime_mailbox_spec,
    realtime_pubsub_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule, MockRouteConfig
from tests.support.realtime_retention import UNSWEPT

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")
_FAST = timedelta(seconds=0.01)
_HLC = HlcTimestamp(physical_ms=1, logical=0)


class _MsgView(BaseModel):
    text: str
    kind: str = "note"


_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_MsgView)
_TYPING = RealtimeEvent(name="typing", payload_type=_MsgView, offline_delivery=False)


class _NullSource(RealtimeSignalSource):
    async def run(self, ctx: Any, handler: Any, *, stop: Any = None) -> None:  # pragma: no cover
        raise NotImplementedError  # the tests drive the handler directly


def _dedup() -> GatewayDedup:
    return GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock")


def _runtime(module: MockDepsModule | None = None) -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module or MockDepsModule()).freeze())


def _principal(text: str = "hi", *, event: str = "order.shipped") -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), event, {"text": text})


async def _append(
    ctx: ExecutionContext,
    signal: RealtimeSignal,
    *,
    event_id: str | None,
    headers: dict[str, str] | None = None,
) -> None:
    spec = realtime_stream_spec()
    cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
    sent = {HEADER_EVENT_ID: event_id} if event_id else {}
    await cmd.append(str(spec.name), signal, type=signal.event, headers={**sent, **(headers or {})})


async def _run_until(
    writer: RealtimeMailboxWriter,
    ctx: ExecutionContext,
    done: Callable[[], Awaitable[bool]],
    *,
    settle: float = 0.06,
) -> None:
    """Run *writer* until *done*, then a little longer so a duplicate is processed too."""

    task = asyncio.create_task(writer.run(ctx))

    try:
        for _ in range(200):
            if await done():
                break

            await asyncio.sleep(0.01)

        await asyncio.sleep(settle)

    finally:
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task


async def _is_unmarked(ctx: ExecutionContext, event_id: str) -> bool:
    """Whether *event_id* is still unseen by the dedup inbox (a fresh mark succeeds)."""

    spec = realtime_inbox_spec()

    async with ctx.tx_ctx.scope("mock"):
        return await ctx.inbox(spec).mark_if_unseen(str(spec.name), event_id)


# ----------------------- #


class TestRealtimeMailboxWriter:
    async def test_durable_principal_signal_is_stored_at_its_carried_hlc(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        stats = RealtimeGatewayStats()
        writer = RealtimeMailboxWriter(
            source=StreamGroupSignalSource(stream_spec=realtime_stream_spec(), poll_interval=_FAST),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
            stats=stats,
        )
        hlc = HlcTimestamp(physical_ms=42, logical=3)

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await _append(ctx, _principal(), event_id="evt-1", headers={HEADER_HLC: hlc.encode()})
            await _run_until(writer, ctx, lambda: _stored(mailbox, 1))

        [entry] = await mailbox.read_since(principal="u1", since=None)
        assert (entry.event_id, entry.hlc, entry.payload) == ("evt-1", hlc, {"text": "hi"})
        assert stats.mailboxed == 1

    async def test_redelivered_signal_is_stored_once(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        stats = RealtimeGatewayStats()
        writer = RealtimeMailboxWriter(
            source=StreamGroupSignalSource(stream_spec=realtime_stream_spec(), poll_interval=_FAST),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
            stats=stats,
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await _append(ctx, _principal(), event_id="evt-1")
            await _append(ctx, _principal(), event_id="evt-1")  # a relay retry
            await _run_until(writer, ctx, lambda: _counted(stats, "dedup_skipped", 1))

        assert len(await mailbox.read_since(principal="u1", since=None)) == 1
        assert (stats.mailboxed, stats.dedup_skipped) == (1, 1)

    async def test_two_nodes_in_one_group_store_each_signal_once(self) -> None:
        # Two writers share the consumer group, the inbox and the mailbox — two nodes. Every
        # signal is appended twice (relay retries), so duplicates can reach either node.
        mailbox = InMemoryRealtimeMailbox()
        stats = RealtimeGatewayStats()
        n = 10

        def _node(consumer: str) -> RealtimeMailboxWriter:
            return RealtimeMailboxWriter(
                source=StreamGroupSignalSource(
                    stream_spec=realtime_stream_spec(),
                    consumer=consumer,
                    poll_interval=_FAST,
                    batch=3,
                    stats=stats,
                ),
                dedup=_dedup(),
                mailbox_factory=lambda _ctx: mailbox,
                stats=stats,
            )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()

            for i in range(n):
                for _ in range(2):
                    await _append(ctx, _principal(f"{i}"), event_id=f"evt-{i}")

            other = asyncio.create_task(_node("node-2").run(ctx))

            try:
                await _run_until(_node("node-1"), ctx, lambda: _counted(stats, "dedup_skipped", n))

            finally:
                other.cancel()

                with pytest.raises(asyncio.CancelledError):
                    await other

        entries = await mailbox.read_since(principal="u1", since=None)
        assert sorted(entry.event_id for entry in entries) == sorted(f"evt-{i}" for i in range(n))
        assert (stats.mailboxed, stats.dedup_skipped) == (n, n)

    async def test_signals_with_nothing_replay_safe_are_left_unmarked(self) -> None:
        # Ephemeral, topic-addressed and opted-out signals: the writer has no live leg, so
        # it stores nothing and marks nothing — a mark would claim a delivery never made.
        mailbox = InMemoryRealtimeMailbox()
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
            event_catalog=RealtimeEventCatalog.of(_SHIPPED, _TYPING),
        )
        topic = RealtimeSignal.of(Audience.topic("room"), "order.shipped", {"text": "x"})

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await writer._handle(ctx, mailbox, _principal(), None, None, _HLC)  # pyright: ignore[reportPrivateUsage]
            await writer._handle(ctx, mailbox, topic, None, "evt-topic", _HLC)  # pyright: ignore[reportPrivateUsage]
            await writer._handle(  # pyright: ignore[reportPrivateUsage]
                ctx, mailbox, _principal(event="typing"), None, "evt-typing", _HLC
            )

            assert await _is_unmarked(ctx, "evt-topic")
            assert await _is_unmarked(ctx, "evt-typing")

        assert await mailbox.read_since(principal="u1", since=None) == []
        assert await mailbox.read_since(principal="room", since=None) == []

    async def test_catalog_admits_normalized_and_rejects_undeclared(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        stats = RealtimeGatewayStats()
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
            event_catalog=RealtimeEventCatalog.of(_SHIPPED),
            stats=stats,
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await writer._handle(ctx, mailbox, _principal(), None, "evt-1", _HLC)  # pyright: ignore[reportPrivateUsage]
            await writer._handle(  # pyright: ignore[reportPrivateUsage]
                ctx, mailbox, _principal(event="undeclared"), None, "evt-2", _HLC
            )

            assert await _is_unmarked(ctx, "evt-2")

        [entry] = await mailbox.read_since(principal="u1", since=None)
        assert entry.payload == {"text": "hi", "kind": "note"}  # the catalog's shape
        assert stats.admission_rejected == 1

    async def test_untenanted_signal_is_dropped_where_a_tenant_is_required(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        stats = RealtimeGatewayStats()
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
            bind_tenant_from_headers=True,  # a tenanted deployment: require_tenant follows
            stats=stats,
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await writer._handle(ctx, mailbox, _principal(), None, "evt-1", _HLC)  # pyright: ignore[reportPrivateUsage]

            assert await _is_unmarked(ctx, "evt-1")

        assert await mailbox.read_since(principal="u1", since=None) == []
        assert stats.untenanted_dropped == 1

    async def test_header_tenant_scopes_a_tenant_aware_mailbox(self) -> None:
        # The same principal in two tenants: the writer stores under the signal's tenant
        # only, never into the other tenant's mailbox.
        writer = RealtimeMailboxWriter(
            source=StreamGroupSignalSource(stream_spec=realtime_stream_spec(), poll_interval=_FAST),
            dedup=_dedup(),
            mailbox_factory=lambda ctx: build_realtime_mailbox(ctx, retention=UNSWEPT),
            bind_tenant_from_headers=True,
        )
        module = MockDepsModule(
            routes={str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True)}
        )

        runtime = _runtime(module)
        async with runtime.scope():
            ctx = runtime.get_context()
            await _append(
                ctx,
                _principal("for t1"),
                event_id=str(UUID(int=1)),
                headers={HEADER_TENANT_ID: str(_T1)},
            )
            await _run_until(writer, ctx, lambda: _tenant_rows(ctx, _T1, 1))

            t1 = await _rows(ctx, _T1)
            t2 = await _rows(ctx, _T2)

        assert [entry.payload["text"] for entry in t1] == ["for t1"]
        assert t2 == []

    async def test_tenant_aware_mailbox_without_a_bound_tenant_names_the_writer_setting(
        self,
    ) -> None:
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda ctx: build_realtime_mailbox(ctx, retention=UNSWEPT),
        )
        module = MockDepsModule(
            routes={str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True)}
        )

        runtime = _runtime(module)
        async with runtime.scope():
            ctx = runtime.get_context()
            mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT)

            with pytest.raises(CoreException) as caught:
                await writer._handle(ctx, mailbox, _principal(), _T1, str(UUID(int=1)), _HLC)  # pyright: ignore[reportPrivateUsage]

            # the failed store rolled the mark back: a redelivery is not swallowed
            assert await _is_unmarked(ctx, str(UUID(int=1)))

        assert caught.value.code == "realtime_mailbox_tenant_unbound"
        assert caught.value.kind is not ExceptionKind.CONFIGURATION
        assert "RealtimeMailboxWriter.bind_tenant_from_headers" in caught.value.summary

    async def test_sharded_source_stores_under_the_stream_tenant(self) -> None:
        spec = realtime_stream_spec()
        writer = RealtimeMailboxWriter(
            source=TenantShardedSignalSource(
                shard=RealtimeShard(stream_spec=spec, tenants=[_T1, _T2]), poll_interval=_FAST
            ),
            dedup=_dedup(),
            mailbox_factory=lambda ctx: build_realtime_mailbox(ctx, retention=UNSWEPT),
        )
        module = MockDepsModule(
            routes={
                str(spec.name): MockRouteConfig(tenant_aware=True),
                str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
            }
        )

        runtime = _runtime(module)
        async with runtime.scope():
            ctx = runtime.get_context()

            for n, tenant in enumerate((_T1, _T2), start=1):
                # no tenant header: the tenant is the stream the signal was read from
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await _append(ctx, _principal(f"for {n}"), event_id=str(UUID(int=n)))

            await _run_until(
                writer,
                ctx,
                lambda: _all(_tenant_rows(ctx, _T1, 1), _tenant_rows(ctx, _T2, 1)),
            )

            t1 = await _rows(ctx, _T1)
            t2 = await _rows(ctx, _T2)

        assert [entry.payload["text"] for entry in t1] == ["for 1"]
        assert [entry.payload["text"] for entry in t2] == ["for 2"]

    def test_a_pubsub_source_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            RealtimeMailboxWriter(
                source=PubSubSignalSource(pubsub_spec=realtime_pubsub_spec()),
                dedup=_dedup(),
                mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            )

        assert caught.value.code == "realtime_mailbox_writer_source_unsupported"


# ....................... #


class TestClaimDedupInbox:
    """A gateway and a writer on one dedup inbox steal each other's signals; one process
    refuses the pairing, consumers of one kind share freely."""

    def test_a_gateway_and_a_writer_cannot_hold_one_inbox(self) -> None:
        with (
            claim_dedup_inbox(_dedup(), "RealtimeGateway"),
            pytest.raises(CoreException) as caught,
            claim_dedup_inbox(_dedup(), "RealtimeMailboxWriter"),
        ):
            pass  # pragma: no cover

        assert caught.value.code == "realtime_dedup_inbox_shared"
        assert caught.value.kind is ExceptionKind.CONFIGURATION

        with (
            claim_dedup_inbox(_dedup(), "RealtimeMailboxWriter"),
            pytest.raises(CoreException),
            claim_dedup_inbox(_dedup(), "RealtimeGateway"),
        ):
            pass  # pragma: no cover

    def test_one_kind_shares_and_the_claim_is_released_on_exit(self) -> None:
        other_inbox = GatewayDedup(inbox_spec=realtime_inbox_spec("other-inbox"), tx_route="mock")

        with (
            claim_dedup_inbox(_dedup(), "RealtimeMailboxWriter"),
            claim_dedup_inbox(_dedup(), "RealtimeMailboxWriter"),  # a second node
            claim_dedup_inbox(other_inbox, "RealtimeGateway"),  # on its own inbox
        ):
            pass

        with claim_dedup_inbox(_dedup(), "RealtimeGateway"):  # both writers released
            pass

    async def test_the_writer_refuses_to_run_on_a_gateways_inbox(self) -> None:
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        )

        runtime = _runtime()
        async with runtime.scope():
            with (
                claim_dedup_inbox(_dedup(), "RealtimeGateway"),
                pytest.raises(CoreException) as caught,
            ):
                await writer.run(runtime.get_context())

        assert caught.value.code == "realtime_dedup_inbox_shared"


# ....................... #


class TestRealtimeMailboxWriterLifecycleStep:
    async def test_the_step_stores_and_drains_with_the_runtime(self) -> None:
        mailbox = InMemoryRealtimeMailbox()
        writer = RealtimeMailboxWriter(
            source=StreamGroupSignalSource(stream_spec=realtime_stream_spec(), poll_interval=_FAST),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: mailbox,
        )
        step = realtime_mailbox_writer_lifecycle_step(writer)
        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_modules(MockDepsModule()).freeze(),
            lifecycle=LifecyclePlan.from_steps(step).freeze(),
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await _append(ctx, _principal(), event_id="evt-1")

            for _ in range(200):
                if await _stored(mailbox, 1):
                    break

                await asyncio.sleep(0.01)

            assert step.startup.loop_name == "realtime_mailbox_writer"  # type: ignore[attr-defined]

        assert [
            entry.event_id for entry in await mailbox.read_since(principal="u1", since=None)
        ] == ["evt-1"]
        assert step.startup.task is None or step.startup.task.done()  # type: ignore[attr-defined]

    async def test_a_tenant_global_source_over_a_tenant_aware_stream_fails_startup(self) -> None:
        spec = realtime_stream_spec()
        writer = RealtimeMailboxWriter(
            source=StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        )
        step = realtime_mailbox_writer_lifecycle_step(writer)
        runtime = _runtime(
            MockDepsModule(routes={str(spec.name): MockRouteConfig(tenant_aware=True)})
        )

        async with runtime.scope():
            with pytest.raises(CoreException) as caught:
                await step.startup(runtime.get_context())

        assert caught.value.code == "tenant_global_required"

    def test_non_positive_supervision_settings_are_refused(self) -> None:
        writer = RealtimeMailboxWriter(
            source=_NullSource(),
            dedup=_dedup(),
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        )

        with pytest.raises(CoreException):
            realtime_mailbox_writer_lifecycle_step(writer, restart_backoff=timedelta(0))

        with pytest.raises(CoreException):
            realtime_mailbox_writer_lifecycle_step(writer, max_consecutive_crashes=0)


# ----------------------- #


async def _stored(mailbox: RealtimeMailbox, count: int) -> bool:
    return len(await mailbox.read_since(principal="u1", since=None)) >= count


async def _counted(stats: RealtimeGatewayStats, name: str, count: int) -> bool:
    return getattr(stats, name) >= count


async def _rows(ctx: ExecutionContext, tenant: UUID) -> list[Any]:
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        return await build_realtime_mailbox(ctx, retention=UNSWEPT).read_since(
            principal="u1", since=None
        )


async def _tenant_rows(ctx: ExecutionContext, tenant: UUID, count: int) -> bool:
    return len(await _rows(ctx, tenant)) >= count


async def _all(*checks: Awaitable[bool]) -> bool:
    return all([await check for check in checks])
