"""The offline-mailbox OTel instrument — counts are kept per spec, outside any instance, so
every mailbox built for a spec records, and the replay drain records one replay-size
observation per connect.

# covers: forze_kits.integrations.realtime.observability.instrument_realtime_mailbox

The counts are process totals, so each test names its own specs: a spec another test
shares would carry that test's counts.
"""

from __future__ import annotations

import gc
from typing import Any
from uuid import UUID, uuid4

import pytest
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.execution.realtime.observability import (
    _forze_meter,  # pyright: ignore[reportPrivateUsage]
)
from forze.application.integrations.realtime import (
    BacklogDrain,
    acknowledge_up_to,
    iter_backlog,
)
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import (
    CursorDocumentSpec,
    MailboxDocumentSpec,
    build_realtime_cursors,
    build_realtime_mailbox,
    instrument_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_spec,
)
from forze_mock import MockDepsModule
from tests.support.realtime_retention import UNSWEPT

# ----------------------- #

_TENANT = TenantIdentity(tenant_id=UUID(int=1))


def _specs() -> tuple[MailboxDocumentSpec, CursorDocumentSpec]:
    channel = f"metrics-{uuid4().hex[:8]}"

    return realtime_mailbox_spec(channel), realtime_cursor_spec(channel)


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _reader() -> tuple[InMemoryMetricReader, Any]:
    reader = InMemoryMetricReader()

    return reader, MeterProvider(metric_readers=[reader]).get_meter("test")


def _points(reader: InMemoryMetricReader) -> dict[str, dict[str, Any]]:
    """Every exported point, by metric name, then by its spec label."""

    data = reader.get_metrics_data()
    found: dict[str, dict[str, Any]] = {}

    for resource in data.resource_metrics if data is not None else ():
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    spec = point.attributes["forze.document"]
                    found.setdefault(metric.name, {})[spec] = point

    return found


async def _store(ctx: ExecutionContext, spec: MailboxDocumentSpec, n: int) -> None:
    """Store one entry through a fresh mailbox, as the writer's or gateway's step would."""

    await build_realtime_mailbox(ctx, retention=UNSWEPT, spec=spec).store(
        principal="u1",
        event_id=str(UUID(int=n)),
        hlc=HlcTimestamp(physical_ms=n, logical=0),
        signal=RealtimeSignal.of(Audience.principal("u1"), "e", {"n": n}),
    )


async def _connect(
    ctx: ExecutionContext, spec: MailboxDocumentSpec, since: HlcTimestamp | None
) -> int:
    """One connect's replay through the mailbox its factory builds, as every transport
    drains it."""

    mailbox = build_realtime_mailbox(ctx, retention=UNSWEPT, spec=spec)
    drained = iter_backlog(mailbox, principal="u1", since=since, outcome=BacklogDrain())

    return len([entry async for entry in drained])


# ----------------------- #


async def test_a_per_connect_factory_records_across_connects() -> None:
    mailbox_spec, cursor_spec = _specs()
    reader, meter = _reader()
    instrument_realtime_mailbox(meter=meter)  # at assembly, before any mailbox exists
    instrument_realtime_mailbox(meter=meter)  # a repeat adds nothing

    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            for n in range(1, 4):
                await _store(ctx, mailbox_spec, n)

            assert await _connect(ctx, mailbox_spec, None) == 3  # a device three behind
            gc.collect()  # the connect's mailbox is gone; its counts are not
            assert await _connect(ctx, mailbox_spec, HlcTimestamp(physical_ms=3, logical=0)) == 0

            await acknowledge_up_to(
                build_realtime_mailbox(ctx, retention=UNSWEPT, spec=mailbox_spec),
                build_realtime_cursors(ctx, spec=cursor_spec),
                principal="u1",
                client_key="phone",
                event_id=str(UUID(int=3)),
            )

    mailbox, cursors = str(mailbox_spec.name), str(cursor_spec.name)
    points = _points(reader)

    assert points["forze.realtime.mailbox.stored"][mailbox].value == 3
    assert points["forze.realtime.mailbox.replayed"][mailbox].value == 3
    assert points["forze.realtime.mailbox.trimmed"][mailbox].value == 3
    assert points["forze.realtime.mailbox.overflowed"][mailbox].value == 0
    assert points["forze.realtime.mailbox.acked"][cursors].value == 1

    replay_size = points["forze.realtime.mailbox.replay_size"][mailbox]
    assert (replay_size.count, replay_size.sum, replay_size.min, replay_size.max) == (2, 3, 0, 3)


async def test_counts_before_the_call_are_exported_and_replays_are_not() -> None:
    mailbox_spec, _ = _specs()
    reader, meter = _reader()

    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            await _store(ctx, mailbox_spec, 1)
            await _connect(ctx, mailbox_spec, None)  # not instrumented yet

            instrument_realtime_mailbox(meter=meter)
            await _connect(ctx, mailbox_spec, None)

    points = _points(reader)
    mailbox = str(mailbox_spec.name)

    # the counters are process totals; the histogram has no past to report
    assert points["forze.realtime.mailbox.replayed"][mailbox].value == 2
    assert points["forze.realtime.mailbox.replay_size"][mailbox].count == 1


async def test_two_specs_give_two_series() -> None:
    (orders, _), (chat, _) = _specs(), _specs()
    reader, meter = _reader()
    instrument_realtime_mailbox(meter=meter)

    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            await _store(ctx, orders, 1)
            await _store(ctx, chat, 1)
            await _store(ctx, chat, 2)
            await _connect(ctx, orders, None)
            await _connect(ctx, chat, None)

    points = _points(reader)
    stored = points["forze.realtime.mailbox.stored"]
    replay_size = points["forze.realtime.mailbox.replay_size"]

    assert stored[str(orders.name)].value == 1
    assert stored[str(chat.name)].value == 2
    assert replay_size[str(orders.name)].sum == 1
    assert replay_size[str(chat.name)].sum == 2


async def test_nothing_is_registered_without_the_call(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[str] = []
    real = metrics.get_meter

    def spy(name: str, *args: Any, **kwargs: Any) -> Any:
        asked.append(name)
        return real(name, *args, **kwargs)

    _forze_meter.cache_clear()  # the default meter is fetched once per process
    monkeypatch.setattr(metrics, "get_meter", spy)
    mailbox_spec, cursor_spec = _specs()

    try:
        runtime = _runtime()

        async with runtime.scope():
            ctx = runtime.get_context()

            with ctx.inv_ctx.bind_identity(tenant=_TENANT):
                await _store(ctx, mailbox_spec, 1)
                await _connect(ctx, mailbox_spec, None)
                await acknowledge_up_to(
                    build_realtime_mailbox(ctx, retention=UNSWEPT, spec=mailbox_spec),
                    build_realtime_cursors(ctx, spec=cursor_spec),
                    principal="u1",
                    client_key="phone",
                    event_id=str(UUID(int=1)),
                )

        assert asked == []

        instrument_realtime_mailbox()
        assert asked == ["forze"]  # the global meter, only once asked to

    finally:
        _forze_meter.cache_clear()
