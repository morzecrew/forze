"""Durable-function bodies run under DST's determinism guard.

A durable step is ordinary async code driven on the simulation's virtual-time loop, which
refuses real I/O (`RealIOForbidden`): a durable body that offloads to a real thread / socket
is caught and surfaces as an `no_unexpected_error` violation. Clock / id / entropy reads stay
deterministic when they go through the `utcnow` / `uuid7` / entropy seams (DST binds a seeded
source); a body that bypasses them is caught by the replay oracle's fingerprint divergence.
This pins the "durable bodies are subject to the same determinism guard as everything else".
"""

from __future__ import annotations

import asyncio

import attrs

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import no_unexpected_error
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_step,
)
from forze_mock import MockDepsModule

# ----------------------- #


async def _real_io(ctx: ExecutionContext, _input: dict | None) -> dict:
    step = resolve_durable_step(ctx)

    async def work() -> dict:
        # A real thread offload — the simulation's virtual-time loop refuses it.
        await asyncio.to_thread(lambda: 1)
        return {}

    await step.run("io", work)
    return {}


_REGISTRY = DurableFunctionRegistry()
_REGISTRY.register("io", _real_io)
_RUNNER = DurableFunctionRunner(registry=_REGISTRY)


@attrs.define(slots=True, kw_only=True)
class _RunIo(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await _RUNNER.run_now(self.ctx, "io", {})


def _operations() -> OperationRegistry:
    return OperationRegistry(
        handlers={"io": lambda ctx: _RunIo(ctx=ctx)},
        descriptors={
            "io": OperationDescriptor(
                input_type=None, output_type=None, description="durable body doing real I/O"
            )
        },
    ).freeze()


_SCENARIO = Scenario(state=ModelState, act=(Rule(op="io"),))


class TestDurableDeterminism:
    def test_real_io_in_a_durable_body_is_refused(self) -> None:
        sim = Simulation(
            operations=_operations(),
            deps=lambda: MockDepsModule(),
            invariants=[no_unexpected_error()],
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(1)
            ),
            scenario=_SCENARIO,
        )

        assert report is not None
        assert "RealIOForbidden" in report.format()
