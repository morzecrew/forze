"""P3 trust story: the export → import → re-export round-trip, on the mock oracle.

# covers: forze_kits.integrations.portability.conformance

``run_export_import_roundtrip`` generalizes the inline re-export equality check into the
backend-agnostic scenario the integration legs run against real Postgres and Mongo. Here it runs
mock↔mock — the oracle leg — proving the harness itself is sound before it is trusted to judge a
real backend. The divergence catalog is checked to be well-formed reviewed data, and the Decimal
probe it names (``decimal-is-string-canonical``) is pinned here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze_kits.integrations.portability import ExportScope, FullScope, TenantScope
from forze_kits.integrations.portability.conformance import (
    PORTABILITY_DIVERGENCES,
    RoundTripOutcome,
    run_export_import_roundtrip,
)
from forze_kits.integrations.quiesce import QuiesceReport
from forze_mock.state import MockState
from tests.support.portability_corpus import (
    mock_runtime,
    order_corpus,
    read_orders,
    seed_attachments,
    seed_orders,
)

# ----------------------- #

_ATTESTED = QuiesceReport(planes=(), admission_held=True)

Seed = Callable[[ExecutionContext], Awaitable[None]]


async def _run(
    source: ExecutionRuntime,
    target: ExecutionRuntime,
    *,
    seed: Seed,
    scope: ExportScope,
    workdir: Path,
) -> RoundTripOutcome:
    async with source.scope(), target.scope():
        assert source.spec_registry is not None
        return await run_export_import_roundtrip(
            source.get_context(),
            target.get_context(),
            source.spec_registry,
            seed=seed,
            workdir=workdir,
            scope=scope,
        )


def _seed_orders(count: int, *, tenant: UUID | None = None) -> Seed:
    async def seed(ctx: ExecutionContext) -> None:
        await seed_orders(ctx, order_corpus(count), tenant=tenant)

    return seed


async def _noop_seed(_ctx: ExecutionContext) -> None:
    return None


# ....................... #


@pytest.mark.asyncio
async def test_roundtrip_is_lossless_on_the_mock_oracle(tmp_path: Path) -> None:
    tenant = uuid4()

    outcome = await _run(
        mock_runtime(MockState()),
        mock_runtime(MockState()),
        seed=_seed_orders(4, tenant=tenant),
        scope=TenantScope(tenant_id=tenant),
        workdir=tmp_path,
    )

    assert outcome.lossless
    assert outcome.documents_match
    assert outcome.exported == outcome.imported == outcome.reexported == 4


@pytest.mark.asyncio
async def test_roundtrip_carries_documents_and_blobs_losslessly_full_scope(tmp_path: Path) -> None:
    async def seed(ctx: ExecutionContext) -> None:
        await seed_orders(ctx, order_corpus(3))
        await seed_attachments(ctx, [(b"%PDF first", {"k": "a"}), (b"\x00\x01 bytes", {})])

    outcome = await _run(
        mock_runtime(MockState(), with_blobs=True),
        mock_runtime(MockState(), with_blobs=True),
        seed=seed,
        scope=FullScope(quiesce=_ATTESTED),
        workdir=tmp_path,
    )

    assert outcome.lossless
    assert outcome.documents_match
    assert outcome.blobs_match
    assert outcome.exported == outcome.reexported == 3


@pytest.mark.asyncio
async def test_decimal_field_round_trips_exactly(tmp_path: Path) -> None:
    """Pins the ``decimal-is-string-canonical`` divergence: a Decimal survives the round-trip
    exactly, because the canonical row renders it as a *string* — ``1.99`` has no exact binary-float
    representation, so a float field would not come back equal here."""

    tenant = uuid4()

    source = mock_runtime(MockState())
    async with source.scope():
        seeded = await seed_orders(source.get_context(), order_corpus(1), tenant=tenant)

    (order_id,) = seeded
    assert seeded[order_id].total == Decimal("1.99")  # what the corpus actually seeded

    target = mock_runtime(MockState())
    outcome = await _run(
        source,
        target,
        seed=_noop_seed,  # source already carries the corpus
        scope=TenantScope(tenant_id=tenant),
        workdir=tmp_path,
    )

    assert outcome.lossless

    async with target.scope():
        restored = await read_orders(target.get_context(), [order_id], tenant=tenant)

    assert restored[order_id].total == Decimal("1.99")


def test_divergence_catalog_is_well_formed() -> None:
    assert PORTABILITY_DIVERGENCES, "the catalog must not be empty"

    names = [d.name for d in PORTABILITY_DIVERGENCES]
    assert len(names) == len(set(names)), "divergence names must be unique"

    for divergence in PORTABILITY_DIVERGENCES:
        assert divergence.name and divergence.reason and divergence.source
        assert divergence.probe is None or divergence.probe, "probe is None or a real test name"

    checked = {d.name for d in PORTABILITY_DIVERGENCES if d.probe}
    assert "datetime-subsecond-precision" in checked
    assert "decimal-is-string-canonical" in checked
