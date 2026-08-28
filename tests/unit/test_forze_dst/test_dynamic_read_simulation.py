"""The dynamic-read plane under simulation: reachable from a workload, and masked on the trace.

The plane's capture policy is tested against a hand-built tracing context elsewhere. This runs
it through the **real** simulation harness instead, which is the path a DST bundle is actually
produced by — a masking rule that held only in a hand-wired context and not here would leak
statements into the artifact people store and share, and nothing would have said so.

Concurrency and the scheduler are on: several dynamic reads interleave under a perturbed
schedule per seed, so the route's per-tenant resolution and its shared shell are exercised from
more than one task at a time rather than in the single-file order a unit test produces.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

import attrs
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.dynamic_read import DynamicReadDepKey, DynamicReadSpec
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.tracing.port_proxy import REDACTED
from forze.application.integrations.dynamic_read import DynamicReadRequest
from forze.base.primitives import JsonDict
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockDynamicReadAdapter, MockDynamicReadRegistry

# ----------------------- #

ROUTE = "widgets"
STATEMENT = "SELECT revenue FROM gold WHERE customer_email = 'nadia@example.com'"


class RunWidget(BaseModel):
    """The operation's input — the widget whose catalog SQL is about to run."""

    widget: str = "revenue"


@attrs.define(slots=True, kw_only=True)
class _RunWidget(Handler[RunWidget, None]):
    ctx: ExecutionContext
    spec: DynamicReadSpec

    async def __call__(self, args: RunWidget) -> None:
        _ = args
        await self.ctx.dynamic_read.query(self.spec).run(STATEMENT, {"limit": 10})


def _handler(request: DynamicReadRequest, state: MockState) -> Sequence[JsonDict]:
    _ = request, state
    return [{"revenue": 10}]


def _simulation(spec: DynamicReadSpec, captured: list[History]) -> Simulation:
    registry = OperationRegistry(
        handlers={"run_widget": lambda ctx: _RunWidget(ctx=ctx, spec=spec)},
        descriptors={
            "run_widget": OperationDescriptor(
                input_type=RunWidget,
                output_type=None,
                description="run a catalog widget's statement",
            )
        },
    ).freeze()

    def capture(history: History) -> list[Violation]:
        captured.append(history)
        return []

    def deps() -> Sequence[DepsModule]:
        # A fresh state and registry per simulation run, so seeds cannot see each other's rows.
        state = MockState()
        mock_registry = MockDynamicReadRegistry().on(ROUTE, _handler)

        def factory(ctx: ExecutionContext, resolved: DynamicReadSpec) -> MockDynamicReadAdapter:
            _ = ctx
            return MockDynamicReadAdapter(
                state=state,
                spec=resolved,
                registry=mock_registry,
                statement_timeout=timedelta(seconds=5),
            )

        return [
            MockDepsModule(state=state),
            lambda: Deps.routed({DynamicReadDepKey: {ROUTE: factory}}),
        ]

    return Simulation(operations=registry, deps=deps, invariants=[capture])


_CASE = OperationCase(op="run_widget", inputs=lambda _rng: RunWidget())


def _statements(history: History) -> list[object]:
    """Whatever the trace recorded as the statement, per dynamic-read call."""

    return [
        dict(event.fields["payload"])["statement"]
        for event in history.events
        if event.kind == "trace"
        and event.fields.get("surface") == DynamicReadDepKey.name
        and event.fields.get("payload") is not None
    ]


def _run(spec: DynamicReadSpec, *, capture_values: bool) -> History:
    captured: list[History] = []
    _simulation(spec, captured).run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            count=6,
            act_count=6,
            concurrency=4,
            seeds=range(4),
            capture_values=capture_values,
        ),
        cases=[_CASE],
    )

    assert captured, "the simulation should have produced a history"
    return captured[0]


# ....................... #


def test_the_plane_runs_under_a_perturbed_schedule() -> None:
    """The route is reachable from a workload, and interleaving it changes nothing.

    Worth its own check before the capture ones: they assert on trace payloads, so if the
    operation never actually reached the port they would pass by finding nothing to complain
    about.
    """

    history = _run(DynamicReadSpec(name=ROUTE), capture_values=False)

    calls = [
        event
        for event in history.events
        if event.kind == "trace" and event.fields.get("surface") == DynamicReadDepKey.name
    ]

    assert calls, "the workload should have driven the dynamic-read port"
    assert all(event.fields.get("payload") is None for event in calls), (
        "production posture: no captured values without the gate"
    )


def test_statement_text_is_masked_on_a_captured_bundle() -> None:
    """The default: the run is visible, the literals it was compiled with are not."""

    history = _run(DynamicReadSpec(name=ROUTE), capture_values=True)
    statements = _statements(history)

    assert statements, "capture_values should record the dynamic-read call"
    assert set(statements) == {REDACTED}
    assert "nadia@example.com" not in repr(history.events)


def test_opting_in_records_the_statement_verbatim() -> None:
    """``capture_statements=True`` is what a statement-level invariant needs."""

    history = _run(
        DynamicReadSpec(name=ROUTE, capture_statements=True),
        capture_values=True,
    )

    assert set(_statements(history)) == {STATEMENT}


def test_capture_is_seed_deterministic() -> None:
    """Same seeds, same captured statements — a bundle has to replay."""

    spec = DynamicReadSpec(name=ROUTE, capture_statements=True)

    assert _statements(_run(spec, capture_values=True)) == _statements(
        _run(spec, capture_values=True)
    )
