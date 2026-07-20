"""Durable execution under DST: a crash mid-run replays committed steps exactly once.

A seeded :class:`~forze_dst.CrashPolicy` kills the process during the *second* durable step's
side-effect (the first step has already committed + journaled). The harness restarts a fresh
runtime over the SAME persisted store and runs the recovery scanner, which re-invokes the run:
the completed first step replays from its memo (its effect is NOT re-run) and the second step
runs to completion. The ``no_duplicate_trace_effect`` oracle asserts each ``(run, step)`` body
executes at most once across the whole crash arc — the durable journal's exactly-once promise,
checked by the simulator rather than a hand-enumerated boundary.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import attrs

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.faults import CrashPolicy
from forze_dst.invariants import Invariant, expect, no_duplicate_trace_effect
from forze_dst.markers import record_event
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    resolve_durable_step,
)
from forze_mock import MockDepsModule

# ----------------------- #
# Two document routes: step "charge" writes one, step "ship" writes the other.


class Charge(Document):
    pass


class ChargeCreate(CreateDocumentCmd):
    pass


class ChargeRead(ReadDocument):
    pass


class Ship(Document):
    pass


class ShipCreate(CreateDocumentCmd):
    pass


class ShipRead(ReadDocument):
    pass


CHARGE_SPEC = DocumentSpec(
    name="charge",
    read=ChargeRead,
    write=DocumentWriteTypes(domain=Charge, create_cmd=ChargeCreate),
)
SHIP_SPEC = DocumentSpec(
    name="ship",
    read=ShipRead,
    write=DocumentWriteTypes(domain=Ship, create_cmd=ShipCreate),
)


# ....................... #
# The durable function: two journaled steps, each with a document side-effect.


async def _fulfil(ctx: ExecutionContext, _input: dict | None) -> dict:
    step = resolve_durable_step(ctx)

    async def charge() -> dict:
        doc = await ctx.document.command(CHARGE_SPEC).create(ChargeCreate())
        return {"charge_id": str(doc.id)}

    async def ship() -> dict:
        doc = await ctx.document.command(SHIP_SPEC).create(ShipCreate())
        return {"ship_id": str(doc.id)}

    charged = await step.run("charge", charge)
    shipped = await step.run("ship", ship)
    return {"charge": charged, "ship": shipped}


_REGISTRY = DurableFunctionRegistry()
_REGISTRY.register("fulfil", _fulfil)
# A short lease so the recovery pass reclaims the crashed run after a brief virtual-time wait.
_RUNNER = DurableFunctionRunner(registry=_REGISTRY, lease_for=timedelta(seconds=1))


@attrs.define(slots=True, kw_only=True)
class _Fulfil(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await _RUNNER.run_now(self.ctx, "fulfil", {})


def _operations() -> OperationRegistry:
    return OperationRegistry(
        handlers={"fulfil": lambda ctx: _Fulfil(ctx=ctx)},
        descriptors={
            "fulfil": OperationDescriptor(
                input_type=None, output_type=None, description="run the durable fulfil"
            )
        },
    ).freeze()


async def _recover(ctx: ExecutionContext) -> None:
    # The crashed run is RUNNING under the dead worker's lease; wait past it (virtual time),
    # then the scanner reclaims and re-invokes it — completed steps replay from the journal.
    await asyncio.sleep(2)
    await _RUNNER.recover(ctx)


async def _observe(ctx: ExecutionContext) -> None:
    """Record the post-restart world: how many charge / ship documents exist."""

    charges = await ctx.document.query(CHARGE_SPEC).find_many()
    ships = await ctx.document.query(SHIP_SPEC).find_many()
    record_event("world", charges=len(charges.hits), ships=len(ships.hits))


_SCENARIO = Scenario(state=ModelState, act=(Rule(op="fulfil"),))

# Crash during the *second* step's write — the first step ("charge") has already committed.
_CRASH = CrashPolicy(surface="document_command", route="ship", op="create")

# Each durable step body must execute at most once per (run, step) across the whole crash arc.
_EXACTLY_ONCE = no_duplicate_trace_effect(
    domain="durable",
    op="step",
    outcome="executed",
    by=("route", "key"),
    name="durable_step_executed_once",
)
_CHARGE_ONCE = expect(
    "world", lambda e: e.fields["charges"] == 1, message="charge not committed exactly once"
)
_SHIP_ONCE = expect(
    "world", lambda e: e.fields["ships"] == 1, message="ship not committed exactly once"
)
_SHIP_MISSING = expect(
    "world", lambda e: e.fields["ships"] == 0, message="ship committed despite the crash"
)


def _sim(*, recover: object | None, invariants: list[Invariant]) -> Simulation:
    return Simulation(
        operations=_operations(),
        deps=lambda: MockDepsModule(),
        recover=recover,  # type: ignore[arg-type]
        observe=_observe,
        invariants=invariants,
    )


def _config() -> SimulationConfig:
    return SimulationConfig(
        strategy=Strategy.SCENARIO,
        act_count=1,
        concurrency=1,
        seeds=range(3),
        crash=_CRASH,
    )


# ....................... #


class TestDurableCrashRecovery:
    def test_crash_leaves_the_second_step_uncommitted(self) -> None:
        # No recovery: the crash really fired mid-run — the first step's write committed and
        # survives the restart, the second step's write did not (proves the scenario bites).
        report = _sim(recover=None, invariants=[_CHARGE_ONCE, _SHIP_MISSING]).run(
            _config(), scenario=_SCENARIO
        )
        assert report is None

    def test_recovery_replays_committed_step_exactly_once(self) -> None:
        # Recovery re-invokes the run: the committed first step replays from its memo (never
        # re-executes), the second step completes — both effects land exactly once.
        report = _sim(
            recover=_recover,
            invariants=[_EXACTLY_ONCE, _CHARGE_ONCE, _SHIP_ONCE],
        ).run(_config(), scenario=_SCENARIO)
        assert report is None

    def test_reproducible_from_one_seed(self) -> None:
        config = _config()
        invariants = [_EXACTLY_ONCE, _CHARGE_ONCE, _SHIP_ONCE]
        a = _sim(recover=_recover, invariants=invariants).run(config, scenario=_SCENARIO)
        b = _sim(recover=_recover, invariants=invariants).run(config, scenario=_SCENARIO)
        assert a is None and b is None
