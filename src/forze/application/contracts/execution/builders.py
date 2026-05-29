from typing import Any, Callable

from forze.base.exceptions import exc
from forze.base.primitives import AbstractSequence, DirectedAcyclicGraph, StrKey

from .value_objects import ExecutionGraph, ExecutionPipeline, GraphStep, Step

# ----------------------- #


def steps_graph_from_sequence[X: GraphStep](
    seq: AbstractSequence[X],
    /,
    *,
    ready_sort_key: Callable[[StrKey], Any] | None = None,
) -> ExecutionGraph[X]:
    """Build a graph from a sequence of steps."""

    step_list = tuple(seq.items)

    steps: dict[StrKey, X] = {}
    order: dict[StrKey, int] = {}

    for s in step_list:
        if s.id in steps:
            raise exc.internal(f"Step ID {s.id} is not unique")

        steps[s.id] = s
        order[s.id] = s.priority

    provider_by_capability: dict[StrKey, StrKey] = {}

    for s in step_list:
        for cap in s.provides:
            if cap in provider_by_capability:
                raise exc.internal(
                    f"Capability {cap} is provided by more than one step"
                )

            provider_by_capability[cap] = s.id

    nodes = set(steps)
    edges: set[tuple[StrKey, StrKey]] = set()

    for s in step_list:
        for dep_id in s.depends_on:
            if dep_id not in steps:
                raise exc.internal(f"Step ID {dep_id} is not found")

            edges.add((dep_id, s.id))

        for cap in s.requires:
            pid = provider_by_capability.get(cap)

            if pid is None:
                raise exc.internal(
                    f"Capability {cap} is required by step {s.id} but no step provides it"
                )

            if pid != s.id:
                edges.add((pid, s.id))

    dag = DirectedAcyclicGraph.from_edges(
        nodes,
        edges,
        u_before_v=True,
    )

    # If sort key is not provided, sort by priority in descending order
    resolved_sort_key: Callable[[StrKey], Any] = (
        ready_sort_key if ready_sort_key is not None else (lambda sid: -order[sid])
    )
    waves = tuple(dag.topological_batches(ready_sort_key=resolved_sort_key))

    return ExecutionGraph(steps=steps, waves=waves)


# ....................... #


def steps_pipe_from_sequence[X: Step](
    seq: AbstractSequence[X], /
) -> ExecutionPipeline[X]:
    """Build a pipeline from a sequence of steps."""

    step_list = tuple(seq.items)
    sorted_steps = sorted(step_list, key=lambda x: x.priority)

    return ExecutionPipeline(steps=tuple(sorted_steps))
