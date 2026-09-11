"""A view-backed aggregate under simulation: an invariant runs over one, and what it took.

The mock's derived read fields are covered against the adapter elsewhere, and against a real
Postgres view in the integration suite. Neither answers the question this file exists for: a
simulation's workload *creates* its own rows, and a marked derived field travels through no
command, so every row the workload makes is a row nothing supplies a value for. Seeding
cannot reach those rows — it runs once, before the workload — which is why the registered
source is the seam rather than a bigger seed.

Both halves are checked, because they fail differently: a registered source serves the rows
the workload creates, and a ``setup`` seed serves the baseline rows a scenario starts from.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import JsonDict, uuid4
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst import invariants as inv
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule, MockDerivedRegistry, MockState
from forze_mock.seeding import SeedPlan, SpecSeed
from forze_mock.seeding.apply import apply_seed

# ----------------------- #
# The origin application's shape: a required nested reference and an aggregate total, both
# produced by the view and declared with no join.


class _SupplierRef(BaseModel):
    id: UUID
    rev: int
    name: str


class _Order(Document):
    supplier_id: UUID


class _OrderCreate(CreateDocumentCmd):
    supplier_id: UUID


class _OrderRead(ReadDocument):
    supplier_id: UUID
    supplier: _SupplierRef
    stock_quantity: float


ORDERS = DocumentSpec(
    name="orders",
    read=_OrderRead,
    write=DocumentWriteTypes(domain=_Order, create_cmd=_OrderCreate),
    derived_read_fields={"supplier": None, "stock_quantity": None},
)

SEEDED = {
    "supplier": {"id": str(uuid4()), "rev": 1, "name": "seeded"},
    "stock_quantity": 12.5,
}


def _view(row: JsonDict) -> Mapping[str, Any]:
    """What the view would have produced for this row.

    A function of the row and nothing else, which is what a replayable simulation needs: a
    source drawing from a clock or a counter would make two runs of one seed disagree.
    """

    supplier_id = str(row["supplier_id"])

    return {
        "supplier": {"id": supplier_id, "rev": 1, "name": f"supplier-{supplier_id[:8]}"},
        "stock_quantity": float(len(supplier_id)),
    }


class PlaceOrder(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True)
class _PlaceOrder(Handler[PlaceOrder, None]):
    """Create a row and read it back — read-your-writes over the aggregate the view backs."""

    ctx: ExecutionContext
    seen: list[str]

    async def __call__(self, args: PlaceOrder) -> None:
        _ = args
        # The framework's `uuid4`, not the stdlib's: the simulation binds a seeded entropy
        # source per run, and a handler minting ids outside it does not replay.
        created = await self.ctx.doc.command(ORDERS).create(_OrderCreate(supplier_id=uuid4()))
        row = await self.ctx.doc.query(ORDERS).get(created.id)
        self.seen.append(row.supplier.name)


class ScanOrders(BaseModel):
    pass


@attrs.define(slots=True, kw_only=True)
class _ScanOrders(Handler[ScanOrders, None]):
    """Read the baseline rows a ``setup`` seed created."""

    ctx: ExecutionContext
    seen: list[str]

    async def __call__(self, args: ScanOrders) -> None:
        _ = args
        page = await self.ctx.doc.query(ORDERS).find_many()
        self.seen.extend(row.supplier.name for row in page.hits)


def _registry(seen: list[str]) -> Any:
    return OperationRegistry(
        handlers={
            "place_order": lambda ctx: _PlaceOrder(ctx=ctx, seen=seen),
            "scan_orders": lambda ctx: _ScanOrders(ctx=ctx, seen=seen),
        },
        descriptors={
            "place_order": OperationDescriptor(
                input_type=PlaceOrder,
                output_type=None,
                description="place an order and read it back",
            ),
            "scan_orders": OperationDescriptor(
                input_type=ScanOrders,
                output_type=None,
                description="scan the view-backed aggregate",
            ),
        },
    ).freeze()


async def _seed(ctx: ExecutionContext) -> None:
    await apply_seed(
        ctx,
        SeedPlan(specs=(SpecSeed(spec=ORDERS, count=3, derived=SEEDED),), rng_seed=7),
    )


def _simulation(
    *,
    seen: list[str],
    source: bool,
    setup: bool,
    histories: list[History] | None = None,
) -> Simulation:
    def deps() -> Sequence[DepsModule]:
        # A fresh state per run, so seeds cannot see each other's rows.
        return [
            MockDepsModule(
                state=MockState(),
                derived_values=MockDerivedRegistry().on("orders", _view) if source else None,
            )
        ]

    def capture(history: History) -> list[Violation]:
        if histories is not None:
            histories.append(history)

        return []

    return Simulation(
        operations=_registry(seen),
        deps=deps,
        setup=_seed if setup else None,
        # `operation_succeeds` carries this file: a configuration refusal raises
        # `CoreException`, which `no_unexpected_error` classifies as an expected domain
        # outcome, so a run in which every operation fails on wiring is otherwise green.
        invariants=[capture, inv.no_unexpected_error(), inv.operation_succeeds("place_order")],
    )


def _run(sim: Simulation, op: str, *, seeds: range = range(2)) -> Any:
    return sim.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=4,
            act_count=4,
            concurrency=2,
            seeds=seeds,
        ),
        cases=[
            OperationCase(
                op=op,
                inputs=lambda _rng: PlaceOrder() if op == "place_order" else ScanOrders(),
            )
        ],
    )


# ----------------------- #


class TestWithoutASource:
    def test_the_workload_cannot_read_what_it_created(self) -> None:
        """The state P2 started from: every operation fails, on wiring rather than on logic.

        Asserted rather than assumed, because it is the default posture — a spec that
        registers nothing keeps the refusal, and this is what that costs a simulation.
        """

        seen: list[str] = []
        report = _run(_simulation(seen=seen, source=False, setup=False), "place_order")

        assert report is not None, "a read with no value for a required marked field must fail"
        assert [v.invariant for v in report.violations] == ["operation_succeeds"]
        assert seen == []


class TestWithASource:
    def test_an_invariant_runs_over_a_view_backed_aggregate(self) -> None:
        """P2's question, answered: the workload creates rows and reads them back clean."""

        seen: list[str] = []
        report = _run(_simulation(seen=seen, source=True, setup=False), "place_order")

        assert report is None, f"no violation expected, got {report}"
        assert seen, "the workload should have read rows back"
        assert all(name.startswith("supplier-") for name in seen)

    def test_the_value_is_per_row(self) -> None:
        """A source is a function of the row, which a seeded constant cannot be.

        The distinction the hand-off residue called out: `SpecSeed.derived` applies one
        value to every row, so twenty orders share one supplier.
        """

        seen: list[str] = []
        _run(_simulation(seen=seen, source=True, setup=False), "place_order")

        assert len(set(seen)) > 1, f"expected per-row values, got {set(seen)}"

    def test_the_same_seed_replays(self) -> None:
        """A simulation whose derived values changed per run would not be replayable."""

        first: list[str] = []
        second: list[str] = []

        _run(_simulation(seen=first, source=True, setup=False), "place_order")
        _run(_simulation(seen=second, source=True, setup=False), "place_order")

        assert first == second
        assert first, "the workload should have read rows back"


class TestTheRegistryIsPerSpec:
    def test_a_source_registered_for_another_spec_does_not_serve_this_one(self) -> None:
        """Fail-closed per spec: the refusal is what an unregistered spec keeps.

        A registry keyed loosely would serve one spec's stand-in to another, which is a
        wrong value rather than a missing one — the failure that does not announce itself.
        """

        seen: list[str] = []

        def deps() -> Sequence[DepsModule]:
            return [
                MockDepsModule(
                    state=MockState(),
                    derived_values=MockDerivedRegistry().on("invoices", _view),
                )
            ]

        sim = Simulation(
            operations=_registry(seen),
            deps=deps,
            invariants=[inv.no_unexpected_error(), inv.operation_succeeds("place_order")],
        )
        report = _run(sim, "place_order")

        assert report is not None, "the orders spec registered nothing, so the read must fail"
        assert [v.invariant for v in report.violations] == ["operation_succeeds"]
        assert seen == []


class TestSeededBaseline:
    def test_a_setup_seed_serves_the_baseline_rows(self) -> None:
        """`apply_seed` from a `setup` hook, which is how a scenario starts from real rows.

        Its own case rather than a variation on the above: the seeder reaches the mock
        adapter through whatever the runtime wrapped it in, and under simulation something
        always has.
        """

        seen: list[str] = []
        report = _run(
            _simulation(seen=seen, source=False, setup=True),
            "scan_orders",
            seeds=range(1),
        )

        assert report is None, f"no violation expected, got {report}"
        assert set(seen) == {"seeded"}

    def test_the_row_outranks_the_source(self) -> None:
        """Both wired: a seeded value is what a test wrote on purpose, so it wins."""

        seen: list[str] = []
        report = _run(
            _simulation(seen=seen, source=True, setup=True),
            "scan_orders",
            seeds=range(1),
        )

        assert report is None, f"no violation expected, got {report}"
        assert set(seen) == {"seeded"}
