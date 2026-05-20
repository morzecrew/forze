from typing import TYPE_CHECKING, Callable

from ..core.value_objects import Graph, GraphStep, Pipeline, Step

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


def resolve_graph[X: GraphStep, S](
    g: Graph[X],
    ctx: "ExecutionContext",
    *,
    resolver: Callable[[X, "ExecutionContext"], S],
) -> Graph[S]:
    steps = {step_id: resolver(step, ctx) for step_id, step in g.steps.items()}

    return Graph(steps=steps, waves=g.waves)


# ....................... #


def resolve_pipe[X: Step, S](
    p: Pipeline[X],
    ctx: "ExecutionContext",
    *,
    resolver: Callable[[X, "ExecutionContext"], S],
) -> Pipeline[S]:
    steps = tuple(resolver(step, ctx) for step in p.steps)

    return Pipeline(steps=steps)
