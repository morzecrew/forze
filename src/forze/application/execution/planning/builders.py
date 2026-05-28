from collections.abc import Callable, Sequence
from typing import Any

from forze.application.contracts.execution import (
    ExecutionGraph,
    ExecutionPipeline,
    GraphStep,
    LifecycleStep,
    Step,
)
from forze.base.exceptions import exc
from forze.base.primitives import AbstractSequence, DirectedAcyclicGraph, StrKey

# ----------------------- #


def graph_from_sequence[X: GraphStep](
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
    resolved_sort_key: Callable[[StrKey], Any] = (
        ready_sort_key if ready_sort_key is not None else (lambda sid: order[sid])
    )
    waves = tuple(dag.topological_batches(ready_sort_key=resolved_sort_key))

    return ExecutionGraph(steps=steps, waves=waves)


# ....................... #


def lifecycle_steps_from_sequence(
    steps: Sequence[LifecycleStep],
) -> tuple[LifecycleStep, ...]:
    """Topologically order lifecycle steps with stable registration tie-break.

    Uses :func:`graph_from_sequence` on ``requires``, ``provides``, and
    ``depends_on`` metadata. Within a wave, higher ``priority`` runs first;
    equal priorities preserve input order.
    """

    step_list = tuple(steps)

    if not step_list:
        return ()

    index = {s.id: i for i, s in enumerate(step_list)}
    priorities = {s.id: s.priority for s in step_list}

    graph = graph_from_sequence(
        AbstractSequence(items=step_list),
        ready_sort_key=lambda sid: (-priorities[sid], index[sid]),
    )

    return tuple(graph.steps[sid] for wave in graph.waves for sid in wave)


# ....................... #


def pipe_from_sequence[X: Step](seq: AbstractSequence[X], /) -> ExecutionPipeline[X]:
    """Build a pipeline from a sequence of steps."""

    step_list = tuple(seq.items)
    sorted_steps = sorted(step_list, key=lambda x: x.priority)

    return ExecutionPipeline(steps=tuple(sorted_steps))
