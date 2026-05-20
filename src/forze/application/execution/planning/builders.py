from forze.base.errors import CoreError
from forze.base.primitives import AbstractSequence, DirectedAcyclicGraph, StrKey

from ..core.value_objects import Graph, GraphStep, Pipeline, Step

# ----------------------- #


def graph_from_sequence[X: GraphStep](seq: AbstractSequence[X], /) -> Graph[X]:
    """Build a graph from a sequence of steps."""

    step_list = tuple(seq.items)

    steps: dict[StrKey, X] = {}
    order: dict[StrKey, int] = {}

    for s in step_list:
        if s.id in steps:
            raise CoreError(f"Step ID {s.id} is not unique")

        steps[s.id] = s
        order[s.id] = s.priority

    provider_by_capability: dict[StrKey, StrKey] = {}

    for s in step_list:
        for cap in s.provides:
            if cap in provider_by_capability:
                raise CoreError(f"Capability {cap} is provided by more than one step")

            provider_by_capability[cap] = s.id

    nodes = set(steps)
    edges: set[tuple[StrKey, StrKey]] = set()

    for s in step_list:
        for dep_id in s.depends_on:
            if dep_id not in steps:
                raise CoreError(f"Step ID {dep_id} is not found")

            edges.add((dep_id, s.id))

        for cap in s.requires:
            pid = provider_by_capability.get(cap)

            if pid is None:
                raise CoreError(
                    f"Capability {cap} is required by step {s.id} but no step provides it"
                )

            if pid != s.id:
                edges.add((pid, s.id))

    dag = DirectedAcyclicGraph.from_edges(
        nodes,
        edges,
        u_before_v=True,
    )
    waves = tuple(dag.topological_batches(ready_sort_key=lambda x: order[x]))

    return Graph(steps=steps, waves=waves)


# ....................... #


def pipe_from_sequence[X: Step](seq: AbstractSequence[X], /) -> Pipeline[X]:
    """Build a pipeline from a sequence of steps."""

    step_list = tuple(seq.items)
    sorted_steps = sorted(step_list, key=lambda x: x.priority)

    return Pipeline(steps=tuple(sorted_steps))
