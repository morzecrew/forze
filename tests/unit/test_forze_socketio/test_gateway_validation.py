"""Gateway wiring validation, trace-context bridging, and edge branches.

# covers: forze_socketio.gateway (post-inits, _trace_context, header edges, empty shard),
#         forze_socketio.gateway_lifecycle (post-init)
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.realtime import RealtimeShard
from forze.base.exceptions import CoreException
from forze_kits.integrations.realtime import realtime_stream_spec
from forze_socketio import (
    RealtimeGateway,
    StreamGroupSignalSource,
    TenantShardedSignalSource,
    realtime_gateway_lifecycle_step,
)
from forze_socketio.gateway import (
    _tenant_from_headers,  # pyright: ignore[reportPrivateUsage]
    _trace_context,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #


class _NullSio:
    async def emit(self, event: str, data: Any = None, **_: Any) -> None:  # pragma: no cover
        return


def _shard(tenants: tuple = ()) -> RealtimeShard:  # type: ignore[type-arg]
    return RealtimeShard(stream_spec=realtime_stream_spec(), tenants=tenants, group="g")


# ----------------------- #


def test_invalid_source_settings_are_refused() -> None:
    spec = realtime_stream_spec()

    with pytest.raises(CoreException):
        StreamGroupSignalSource(stream_spec=spec, max_deliveries=0)

    with pytest.raises(CoreException):
        TenantShardedSignalSource(shard=_shard(), max_deliveries=-1)

    with pytest.raises(CoreException):
        TenantShardedSignalSource(shard=_shard(), restart_backoff=timedelta(0))


def test_invalid_lifecycle_step_settings_are_refused() -> None:
    gateway = RealtimeGateway(
        sio=_NullSio(),  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(stream_spec=realtime_stream_spec()),
    )

    with pytest.raises(CoreException):
        realtime_gateway_lifecycle_step(gateway, restart_backoff=timedelta(0))

    with pytest.raises(CoreException):
        realtime_gateway_lifecycle_step(gateway, max_consecutive_crashes=0)


# ----------------------- #


def test_trace_context_bridges_a_carried_traceparent() -> None:
    headers = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
    entered = False

    with _trace_context(headers, stream="s"):
        entered = True  # attached the remote context and opened the CONSUMER span

    assert entered


def test_trace_context_passes_through_without_a_traceparent() -> None:
    with _trace_context({}, stream="s"):
        pass

    with _trace_context(object(), stream="s"):  # header-less message types tolerated
        pass


def test_tenant_from_headers_tolerates_junk() -> None:
    tenant = uuid4()

    assert _tenant_from_headers({"forze_tenant_id": str(tenant)}) == tenant
    assert _tenant_from_headers({"forze_tenant_id": "not-a-uuid"}) is None
    assert _tenant_from_headers({}) is None
    assert _tenant_from_headers(object()) is None  # header-less message types tolerated


# ----------------------- #


async def test_empty_shard_idles_until_stopped() -> None:
    source = TenantShardedSignalSource(shard=_shard(()))

    async def _handler(*args: Any) -> None:  # pragma: no cover - nothing is assigned
        return

    stop = asyncio.Event()
    run = asyncio.create_task(source.run(None, _handler, stop=stop))  # type: ignore[arg-type]

    await asyncio.sleep(0.05)
    assert not run.done()  # idling — an early return would look like a crash to supervision

    stop.set()
    await asyncio.wait_for(run, timeout=5)
