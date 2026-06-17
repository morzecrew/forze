"""Reactive refinement of derived scenarios.

Static derivation can't see that ``fulfill`` only ever runs as a *cascade* of ``checkout``
(saga step / domain-event handler) — the operation registries hold opaque callables, not
operation references. So the harness probes: it fires each candidate act op once and diffs
the engine trace. An op the trace shows invoked but the harness never drove directly is an
internal effect, and is dropped from the act set so it isn't driven as a standalone entry
point.
"""

from __future__ import annotations

import attrs
from pydantic import BaseModel

from forze.application.contracts.domain.deps import DomainDeps
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.domain.handler import DomainEventRegistry
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import DomainEvent

from forze_dst import ReactiveMap, Simulation
from forze_mock import MockDepsModule

# ----------------------- #


class FulfillDTO(BaseModel):
    order_id: str


def _reactive_simulation() -> Simulation:
    # A mutable holder breaks the registry↔handler cycle: the checkout handler reaches the
    # frozen registry to invoke ``fulfill`` reactively (stand-in for a saga/event cascade).
    holder: dict[str, object] = {}

    @attrs.define(slots=True, kw_only=True)
    class _Checkout(Handler[None, None]):
        ctx: ExecutionContext

        async def __call__(self, _args: None) -> None:
            registry = holder["registry"]
            await run_operation(registry, "fulfill", FulfillDTO(order_id="x"), self.ctx)  # type: ignore[arg-type]

    @attrs.define(slots=True)
    class _Fulfill(Handler[FulfillDTO, None]):
        async def __call__(self, _args: FulfillDTO) -> None:
            return None

    registry = OperationRegistry(
        handlers={
            "checkout": lambda ctx: _Checkout(ctx=ctx),
            "fulfill": lambda _c: _Fulfill(),
        },
        descriptors={
            "checkout": OperationDescriptor(
                input_type=None, output_type=None, description="Checkout."
            ),
            "fulfill": OperationDescriptor(
                input_type=FulfillDTO, output_type=None, description="Fulfill (reactive)."
            ),
        },
    ).freeze()
    holder["registry"] = registry

    return Simulation(operations=registry, deps=lambda: MockDepsModule())


# ....................... #


class TestReactiveDerivation:
    def test_probe_drops_reactively_triggered_op(self) -> None:
        sim = _reactive_simulation()

        # Static derivation alone treats both as entry points.
        static = sim.derive_scenario(probe=False)
        assert {rule.op for rule in static.act} == {"checkout", "fulfill"}

        # The probe sees fulfill invoked only as checkout's cascade → drops it from act.
        refined = sim.derive_scenario(probe=True)
        assert {rule.op for rule in refined.act} == {"checkout"}

    def test_probe_keeps_independent_entry_points(self) -> None:
        # Two unrelated ops, neither triggering the other → both stay entry points.
        @attrs.define(slots=True)
        class _Noop(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                return None

        registry = OperationRegistry(
            handlers={"a": lambda _c: _Noop(), "b": lambda _c: _Noop()}
        ).freeze()
        sim = Simulation(operations=registry, deps=lambda: MockDepsModule())

        refined = sim.derive_scenario(probe=True)
        assert {rule.op for rule in refined.act} == {"a", "b"}


class TestReactiveMap:
    def test_recovers_cascade_topology(self) -> None:
        rmap = _reactive_simulation().reactive_map()

        assert rmap.cascades["checkout"] == frozenset({"fulfill"})
        assert rmap.cascades["fulfill"] == frozenset()
        assert rmap.reactive_ops == frozenset({"fulfill"})
        assert rmap.entry_points() == frozenset({"checkout"})
        assert rmap.triggers("checkout") == frozenset({"fulfill"})

        rendered = rmap.format()
        assert "checkout → fulfill" in rendered
        assert "entry points: checkout" in rendered


# ----------------------- #
# A genuine domain-event cascade: an op dispatches an event whose registered handler runs
# another op. The event→operation link is opaque to static inspection (handler factories are
# closures); the probe recovers it — both the triggered op and the event type that carried it.


class ThingHappened(DomainEvent):
    pass


def _event_cascade_simulation() -> Simulation:
    holder: dict[str, object] = {}

    @attrs.define(slots=True, kw_only=True)
    class _Place(Handler[None, None]):
        ctx: ExecutionContext

        async def __call__(self, _args: None) -> None:
            dispatcher = DomainDeps(ctx=self.ctx)()
            await dispatcher.dispatch([ThingHappened()])

    @attrs.define(slots=True)
    class _React(Handler[None, None]):
        async def __call__(self, _args: None) -> None:
            return None

    registry = OperationRegistry(
        handlers={
            "place": lambda ctx: _Place(ctx=ctx),
            "react": lambda _c: _React(),
        },
        descriptors={
            "place": OperationDescriptor(
                input_type=None, output_type=None, description="Place."
            ),
            "react": OperationDescriptor(
                input_type=None, output_type=None, description="React (event handler)."
            ),
        },
    ).freeze()
    holder["registry"] = registry

    events = DomainEventRegistry()

    def _react_factory(ctx: ExecutionContext):  # type: ignore[no-untyped-def]
        async def handle(_event: DomainEvent) -> None:
            await run_operation(holder["registry"], "react", None, ctx)  # type: ignore[arg-type]

        return handle

    events.register(ThingHappened, _react_factory)

    # The mock module wires the domain-event dispatcher; hand it our handler registry.
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(domain_events=events),
    )


class TestDomainEventCascade:
    def test_recovers_event_and_triggered_op(self) -> None:
        rmap = _event_cascade_simulation().reactive_map()

        assert rmap.cascades["place"] == frozenset({"react"})  # the triggered op
        assert "ThingHappened" in rmap.events["place"]  # the event that carried it
        assert rmap.entry_points() == frozenset({"place"})

        rendered = rmap.format()
        assert "place →" in rendered and "[ThingHappened]" in rendered and "react" in rendered

    def test_derive_scenario_drops_the_event_handler_op(self) -> None:
        refined = _event_cascade_simulation().derive_scenario(probe=True)
        assert {rule.op for rule in refined.act} == {"place"}

    def test_deps_factory_may_return_multiple_modules(self) -> None:
        # The deps factory accepts a list of modules (app plane + custom wiring).
        sim = _event_cascade_simulation()
        registry = sim.operations
        events = sim  # reuse the same wiring via a fresh list-returning factory

        multi = Simulation(
            operations=registry,
            deps=lambda: [MockDepsModule()],  # a one-element list exercises the splat
        )
        # A bare probe runs without error through the multi-module deps path.
        assert multi.reactive_map().entry_points() >= frozenset({"place"})
        del events


class TestReactiveMapFormat:
    def test_no_cascades_renders_placeholder(self) -> None:
        rmap = ReactiveMap(
            cascades={"a": frozenset(), "b": frozenset()},
            events={"a": frozenset(), "b": frozenset()},
        )
        rendered = rmap.format()
        assert "(no cascades)" in rendered
        assert "entry points: a, b" in rendered
