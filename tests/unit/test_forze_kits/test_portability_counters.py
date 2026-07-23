"""The counter plane round-trips — every suffix partition, including the unsuffixed one.

# covers: forze_kits.integrations.portability (counter plane)

A counter is the durable state behind every invoice, order and ticket number an application has
handed out, so an export that left it at zero would silently reissue numbers already in customers'
hands. This proves the loop: seed a few partitions (a suffixed pair and the unsuffixed ``None`` one
— a real, distinct counter, not "no counter"), export, import into a fresh backend, and every
partition's value is restored so the next allocation continues where the source left off. It also
proves the direct ``migrate`` carries the plane, and that a bound counter no longer refuses export.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from forze import build_runtime
from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionRuntime
from forze_kits.integrations.portability import (
    UNTENANTED,
    ExportReport,
    FullScope,
    ImportReport,
    export_archive,
    import_archive,
    migrate,
)
from forze_mock import MockDepsModule
from forze_mock.state import MockState
from tests.support.quiesce import attested_report

# ----------------------- #

SPEC = CounterSpec(name="invoices")
_ATTESTED = attested_report()

# The unsuffixed counter (suffix=None) is a real, distinct partition most apps actually use.
_EXPECTED = {(None, 100), ("eu", 50), ("us", 7)}


def _runtime(state: MockState) -> ExecutionRuntime:
    return build_runtime(
        MockDepsModule(state=state), specs=SpecRegistry().register(SPEC), allow_unregistered=True
    )


async def _seed(runtime: ExecutionRuntime) -> None:
    async with runtime.scope():
        counter = runtime.get_context().counter(SPEC)
        await counter.reset(100)  # the unsuffixed (None) partition
        await counter.reset(50, suffix="eu")
        await counter.reset(7, suffix="us")


async def _partitions(runtime: ExecutionRuntime) -> set[tuple[str | None, int]]:
    async with runtime.scope():
        entries = await runtime.get_context().counter.admin(SPEC).list_counters()

    return {(entry.suffix, entry.value) for entry in entries}


async def _export(runtime: ExecutionRuntime, dest: Path) -> ExportReport:
    async with runtime.scope():
        return await export_archive(
            runtime, dest, scope=FullScope(quiesce=_ATTESTED, tenants=UNTENANTED)
        )


async def _import(runtime: ExecutionRuntime, src: Path) -> ImportReport:
    async with runtime.scope():
        return await import_archive(runtime, src)


# ....................... #


@pytest.mark.asyncio
async def test_counter_round_trip_preserves_every_partition(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    report = await _export(source, archive)

    assert report.total_counters == 3
    assert (archive / "counters" / "invoices.jsonl.gz").exists()

    target = _runtime(MockState())
    result = await _import(target, archive)

    assert result.total_counters == 3
    assert await _partitions(target) == _EXPECTED


@pytest.mark.asyncio
async def test_restored_counter_continues_from_its_value(tmp_path: Path) -> None:
    """The unsuffixed counter is truly at 100 in the target — the next allocation is 101, not 1, so
    the restore set the backend value rather than merely echoing it back through ``list_counters``."""

    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    await _export(source, archive)

    target = _runtime(MockState())
    await _import(target, archive)

    async with target.scope():
        assert await target.get_context().counter(SPEC).incr() == 101


@pytest.mark.asyncio
async def test_counter_migrate_carries_the_plane(tmp_path: Path) -> None:
    """The direct ports-to-ports migrate carries the counter plane too — no file, values intact."""

    source = _runtime(MockState())
    await _seed(source)

    target = _runtime(MockState())
    async with source.scope(), target.scope():
        report = await migrate(
            source, target, scope=FullScope(quiesce=_ATTESTED, tenants=UNTENANTED)
        )

    assert report.total_counters == 3
    assert await _partitions(target) == _EXPECTED


@pytest.mark.asyncio
async def test_counter_reimport_is_idempotent(tmp_path: Path) -> None:
    """``reset`` sets an absolute value, so a re-run converges to the same partitions, not double."""

    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    await _export(source, archive)

    target = _runtime(MockState())
    await _import(target, archive)
    await _import(target, archive)

    assert await _partitions(target) == _EXPECTED
