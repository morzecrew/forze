"""The derived-read differential against a real view: one seed, two stores, the derived fields diffed.

``test_pg_document_derived_read_fields`` proves one hand-built row agrees. This leg runs the
shipped check (:func:`~forze_dst.conformance.run_derived_fidelity`) the way an app author
would: one :class:`~forze_mock.seeding.SeedPlan` loaded through the write path of Postgres and
of the mock, the same ids read through both, and only the derived fields compared.

The view projects an **expanded reference** — ``warehouse: {id, name}`` — over a ``LEFT JOIN``,
which is the shape ``DerivedReadField(fields=...)`` exists for. The rows cover a matching
reference, an unset key and a key whose target is not there, so the optional semantics are
measured against the view rather than argued. The second case keeps the view and swaps the
mock's resolution for the hand-written callback a suite would otherwise carry, and shows the
check names the rows it gets wrong.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.conformity import DerivedReadField
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import ExecutionContext
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_deps, context_from_modules
from forze_dst.conformance import run_derived_fidelity
from forze_mock import MockDepsModule, MockDerivedRegistry
from forze_mock.seeding import SeedPlan, SeedResult, SpecSeed
from forze_mock.seeding.apply import apply_seed
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.kernel.client.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class _Warehouse(Document):
    name: str
    code: str


class _WarehouseCreate(CreateDocumentCmd):
    name: str
    code: str


class _WarehouseRead(ReadDocument):
    name: str
    code: str


WAREHOUSES = DocumentSpec(
    name="warehouses",
    read=_WarehouseRead,
    write=DocumentWriteTypes(domain=_Warehouse, create_cmd=_WarehouseCreate),
)


class _WarehouseRef(BaseModel):
    id: UUID
    name: str


class _Order(Document):
    warehouse_id: UUID | None = None


class _OrderCreate(CreateDocumentCmd):
    warehouse_id: UUID | None = None


class _OrderRead(ReadDocument):
    warehouse_id: UUID | None = None
    warehouse: _WarehouseRef | None = None
    """Produced by the view as ``jsonb_build_object('id', w.id, 'name', w.name)``."""


def _orders(derived: DerivedReadField | None) -> DocumentSpec:
    return DocumentSpec(
        name="orders",
        read=_OrderRead,
        write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
        derived_read_fields={"warehouse": derived},
    )


RESOLVED = _orders(
    DerivedReadField(source="warehouses", via="warehouse_id", fields=("id", "name"), optional=True)
)
MARKED = _orders(None)

NORTH = UUID("00000000-0000-4000-8000-0000000000b1")
SOUTH = UUID("00000000-0000-4000-8000-0000000000b2")
GONE = UUID("00000000-0000-4000-8000-0000000000bf")
"""A key no warehouse was seeded for: the ``LEFT JOIN`` yields ``NULL``."""


def _plan(spec: DocumentSpec) -> SeedPlan:
    return SeedPlan(
        specs=(
            SpecSeed(
                spec=WAREHOUSES,
                fixtures=(
                    {"id": NORTH, "name": "North", "code": "N1"},
                    {"id": SOUTH, "name": "South", "code": "S1"},
                ),
            ),
            SpecSeed(
                spec=spec,
                fixtures=(
                    {"warehouse_id": NORTH},
                    {"warehouse_id": SOUTH},
                    {"warehouse_id": None},
                    {"warehouse_id": GONE},
                ),
            ),
        ),
        # The fixtures carry their keys; inference would overwrite them with picks.
        links={"orders": {"warehouse_id": None}},
    )


def _seed(plan: SeedPlan):
    async def seed(ctx: ExecutionContext) -> SeedResult:
        return await apply_seed(ctx, plan)

    return seed


async def _real(pg_client: PostgresClient) -> ExecutionContext:
    """Two tables and a view expanding the reference — the relation the mock stands in for."""

    suffix = uuid4().hex[:8]
    warehouses, orders = f"pg_fid_warehouses_{suffix}", f"pg_fid_orders_{suffix}"
    view = f"pg_fid_orders_view_{suffix}"
    bookkeeping = (
        "id uuid PRIMARY KEY, rev integer NOT NULL, created_at timestamptz NOT NULL, "
        "last_update_at timestamptz NOT NULL"
    )

    await pg_client.execute(
        f"CREATE TABLE public.{warehouses} ({bookkeeping}, name text NOT NULL, code text NOT NULL);"
    )
    await pg_client.execute(f"CREATE TABLE public.{orders} ({bookkeeping}, warehouse_id uuid);")
    await pg_client.execute(
        f"""
        CREATE VIEW public.{view} AS
        SELECT o.id, o.rev, o.created_at, o.last_update_at, o.warehouse_id,
               CASE WHEN w.id IS NULL THEN NULL
                    ELSE jsonb_build_object('id', w.id, 'name', w.name) END AS warehouse
        FROM public.{orders} o
        LEFT JOIN public.{warehouses} w ON w.id = o.warehouse_id;
        """
    )

    def config(read: str, write: str) -> PostgresDocumentConfig:
        return PostgresDocumentConfig(
            read=("public", read), write=("public", write), bookkeeping_strategy="application"
        )

    deps = PostgresDepsModule(
        client=pg_client,
        rw_documents={
            "warehouses": config(warehouses, warehouses),
            "orders": config(view, orders),
        },
    )()

    return context_from_deps(deps)


# ----------------------- #


@pytest.mark.conformance(plane="derived_reads", engine="postgres")
async def test_a_resolved_reference_matches_the_view(pg_client: PostgresClient) -> None:
    report = await run_derived_fidelity(
        spec=RESOLVED,
        seed=_seed(_plan(RESOLVED)),
        real=await _real(pg_client),
        mock=context_from_modules(MockDepsModule()),
    )

    assert report.agrees, report.render_markdown()
    assert len(report.ids) == 4


async def test_a_wrong_hand_written_join_is_reported(pg_client: PostgresClient) -> None:
    def by_hand(row: JsonDict) -> dict[str, Any]:
        # The drift a suite accumulates: one display name for every reference, and an
        # object even where the view's LEFT JOIN found no row.
        key = row.get("warehouse_id")
        return {"warehouse": None if key is None else {"id": key, "name": "North"}}

    report = await run_derived_fidelity(
        spec=MARKED,
        seed=_seed(_plan(MARKED)),
        real=await _real(pg_client),
        mock=context_from_modules(
            MockDepsModule(derived_values=MockDerivedRegistry().on("orders", by_hand))
        ),
    )

    south, gone = report.ids[1], report.ids[3]

    assert [(m.field, m.id) for m in report.mismatches] == [
        ("warehouse", south),
        ("warehouse", gone),
    ]
    assert report.mismatches[0].real == {"id": str(SOUTH), "name": "South"}
    assert report.mismatches[0].mock == {"id": str(SOUTH), "name": "North"}
    assert report.mismatches[1].real is None
    assert "| `warehouse` | 2 | 2 |" in report.render_markdown()
