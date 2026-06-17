"""Auto-derived scenarios — a draft model inferred from the operation catalog.

The payoff: hand-write zero rules. `derive_scenario` reads the catalog, infers that
``create_order`` produces an ``order`` and ``pay_order`` consumes one (by its ``order_id``
field), and emits the arrange→act scenario. Driven through the harness it finds the same
concurrent double-charge as the hand-written scenario.
"""

from __future__ import annotations

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze_dst import (
    ModelState,
    Simulation,
    derive_scenario,
    no_duplicate_effect,
    record_event,
)
from forze_dst.derive import _entity_for_field, _entity_produced_by
from forze_mock import MockDepsModule

# ----------------------- #


class PayDTO(BaseModel):
    order_id: str


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, str]):
    orders: dict[str, dict]

    async def __call__(self, _args: None) -> str:
        order_id = str(len(self.orders))
        self.orders[order_id] = {"paid": False}
        return order_id


@attrs.define(slots=True, kw_only=True)
class _PayOrder(Handler[PayDTO, None]):
    orders: dict[str, dict]

    async def __call__(self, args: PayDTO) -> None:
        order = self.orders[args.order_id]
        if order["paid"]:
            return
        import asyncio

        await asyncio.sleep(0)  # concurrent payments race through the check
        order["paid"] = True
        record_event("charge", order_id=args.order_id)


def _payments_simulation() -> Simulation:
    orders: dict[str, dict] = {}

    registry = OperationRegistry(
        handlers={
            "create_order": lambda _c: _CreateOrder(orders=orders),
            "pay_order": lambda _c: _PayOrder(orders=orders),
        },
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="Create an order."
            ),
            "pay_order": OperationDescriptor(
                input_type=PayDTO, output_type=None, description="Pay an order."
            ),
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        orders.clear()

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        invariants=[no_duplicate_effect("charge", by="order_id")],
    )


# ....................... #


class TestDeriveScenario:
    def test_inferred_scenario_shape(self) -> None:
        scenario = derive_scenario(_payments_simulation().operations)

        # create_order → arrange producer of "order".
        assert [rule.op for rule in scenario.arrange] == ["create_order"]
        assert scenario.arrange[0].produces == "order"

        # pay_order → act consumer requiring "order".
        assert [rule.op for rule in scenario.act] == ["pay_order"]
        assert scenario.act[0].requires == ("order",)

    def test_derived_scenario_finds_double_charge(self) -> None:
        sim = _payments_simulation()
        report = sim.explore_scenario(
            derive_scenario(sim.operations),
            act_count=6,
            concurrency=6,
            seeds=range(5),
        )
        assert report is not None
        assert report.violations[0].invariant == "no_duplicate_effect"
        assert [op for op, _ in report.workload] == ["pay_order", "pay_order"]

    def test_arrange_each_multiplies_producers(self) -> None:
        scenario = derive_scenario(
            _payments_simulation().operations, arrange_each=3
        )
        assert [rule.op for rule in scenario.arrange] == ["create_order"] * 3

    def test_arg_builder_fills_entity_field_from_pool(self) -> None:
        scenario = derive_scenario(_payments_simulation().operations)
        state = ModelState()
        state.add("order", "the-order")

        import random

        arg = scenario.act[0].arg(state, random.Random(0))
        assert isinstance(arg, PayDTO)
        assert arg.order_id == "the-order"  # filled from the arranged pool, not generated


class TestArgBuilderBranches:
    def test_no_input_op_and_mixed_field_op(self) -> None:
        import random

        class ShipDTO(BaseModel):
            order_id: str
            carrier: str  # a non-entity field → auto-generated

        registry = OperationRegistry(
            handlers={
                "create_order": lambda _c: _CreateOrder(orders={}),
                "ship_order": lambda _c: _CreateOrder(orders={}),
                "heartbeat": lambda _c: _CreateOrder(orders={}),
            },
            descriptors={
                "ship_order": OperationDescriptor(
                    input_type=ShipDTO, output_type=None, description="Ship."
                ),
                "heartbeat": OperationDescriptor(
                    input_type=None, output_type=None, description="Beat."
                ),
            },
        ).freeze()

        scenario = derive_scenario(registry)
        by_op = {rule.op: rule for rule in scenario.act}

        # Mixed op: entity field filled from pool, scalar field auto-generated (polyfactory).
        state = ModelState()
        state.add("order", "ord-1")
        ship_arg = by_op["ship_order"].arg(state, random.Random(0))
        assert isinstance(ship_arg, ShipDTO)
        assert ship_arg.order_id == "ord-1"  # from the pool
        assert isinstance(ship_arg.carrier, str)  # generated

        # No-input op: arg builder yields None.
        assert by_op["heartbeat"].requires == ()
        assert by_op["heartbeat"].arg(state, random.Random(0)) is None


class TestHeuristics:
    def test_entity_produced_by_create_verbs(self) -> None:
        assert _entity_produced_by("create_order", frozenset({"create"})) == "order"
        assert _entity_produced_by("open_account", frozenset({"open"})) == "account"
        assert _entity_produced_by("pay_order", frozenset({"create"})) is None
        assert _entity_produced_by("create", frozenset({"create"})) is None  # no entity

    def test_entity_for_field_matches_known_entities(self) -> None:
        entities = frozenset({"order", "account"})
        assert _entity_for_field("order_id", entities) == "order"
        assert _entity_for_field("account", entities) == "account"
        assert _entity_for_field("account_uuid", entities) == "account"
        assert _entity_for_field("amount", entities) is None
        assert _entity_for_field("widget_id", entities) is None  # unknown entity
