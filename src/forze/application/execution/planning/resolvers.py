from typing import TYPE_CHECKING, Callable

from forze.application.contracts.execution import (
    ExecutionGraph,
    ExecutionPipeline,
    GraphStep,
    Step,
)

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


def resolve_graph[X: GraphStep, S](
    g: ExecutionGraph[X],
    ctx: "ExecutionContext",
    *,
    resolver: Callable[[X, "ExecutionContext"], S],
) -> ExecutionGraph[S]:
    steps = {step_id: resolver(step, ctx) for step_id, step in g.steps.items()}

    return ExecutionGraph(steps=steps, waves=g.waves)


# ....................... #


def resolve_pipe[X: Step, S](
    p: ExecutionPipeline[X],
    ctx: "ExecutionContext",
    *,
    resolver: Callable[[X, "ExecutionContext"], S],
) -> ExecutionPipeline[S]:
    steps = tuple(resolver(step, ctx) for step in p.steps)

    return ExecutionPipeline(steps=steps)
