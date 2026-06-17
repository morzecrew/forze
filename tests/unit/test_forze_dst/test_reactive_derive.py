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

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze_dst import Simulation
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
