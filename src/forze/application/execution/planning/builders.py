from typing import Sequence

from forze.application.contracts.execution import (
    ExecutionGraph,
    LifecycleStep,
    steps_graph_from_sequence,
)
from forze.base.primitives import AbstractSequence

# ----------------------- #


def lifecycle_graph_from_sequence(
    steps: Sequence[LifecycleStep],
) -> ExecutionGraph[LifecycleStep]:
    """Build a lifecycle execution graph with topological waves.

    Uses :func:`steps_graph_from_sequence` on ``requires``, ``provides``, and
    ``depends_on`` metadata. Within a wave, higher ``priority`` runs first;
    equal priorities preserve input order.
    """

    step_list = tuple(steps)

    if not step_list:
        return ExecutionGraph()

    index = {s.id: i for i, s in enumerate(step_list)}
    priorities = {s.id: s.priority for s in step_list}

    return steps_graph_from_sequence(
        AbstractSequence(items=step_list),
        ready_sort_key=lambda sid: (-priorities[sid], index[sid]),
    )


# ....................... #


def lifecycle_steps_from_sequence(
    steps: Sequence[LifecycleStep],
) -> tuple[LifecycleStep, ...]:
    """Flatten a lifecycle graph into a topologically ordered step tuple."""

    graph = lifecycle_graph_from_sequence(steps)

    return tuple(graph.steps[sid] for wave in graph.waves for sid in wave)
