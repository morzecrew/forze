"""RFC 0017: the counter plane round-trips through a *real* Redis, context to context.

# covers: forze_kits.integrations.portability (counter plane on real Redis)

The mock proves the orchestration; this proves it where ``list_counters`` is a real ``SCAN`` and
``reset`` a real ``SET``. Enumerate a namespace's partitions (a suffixed one and the unsuffixed one),
export, import into a *different* namespace, and the sequence continues where the source left off —
no number the source issued is reissued. Two namespaces stand in for source and target within one
Redis (a per-route namespace isolates counters), same spec name both sides so the fingerprints agree.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.execution import ExecutionContext
from forze.testing import context_from_deps
from forze_kits.integrations.portability import ArchiveExporter, ArchiveImporter, FullScope
from forze_kits.integrations.quiesce import QuiesceReport
from forze_redis import RedisCounterConfig, RedisDepsModule
from forze_redis.kernel.client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

SPEC = CounterSpec(name="invoices")
_ATTESTED = QuiesceReport(planes=(), admission_held=True)


def _registry() -> FrozenSpecRegistry:
    return SpecRegistry().register(SPEC).freeze()


def _ctx(client: RedisClient, namespace: str) -> ExecutionContext:
    return context_from_deps(
        RedisDepsModule(
            client=client, counters={"invoices": RedisCounterConfig(namespace=namespace)}
        )()
    )


# ....................... #


async def test_counter_round_trips_through_real_redis(
    redis_client: RedisClient, tmp_path: Path
) -> None:
    source = _ctx(redis_client, f"it:port:{uuid4().hex[:12]}")
    target = _ctx(redis_client, f"it:port:{uuid4().hex[:12]}")

    counter = source.counter(SPEC)
    await counter.incr_batch(size=9)  # the unsuffixed partition → 9
    await counter.incr_batch(size=3, suffix="2026")  # the "2026" partition → 3

    archive = tmp_path / "archive"
    export = await ArchiveExporter()(
        source, _registry(), archive, scope=FullScope(quiesce=_ATTESTED)
    )
    assert export.total_counters == 2

    result = await ArchiveImporter()(target, _registry(), archive)
    assert result.total_counters == 2

    # The sequence continues on the target over real Redis — no number the source issued is reissued.
    assert await target.counter(SPEC).incr() == 10
    assert await target.counter(SPEC).incr(suffix="2026") == 4
