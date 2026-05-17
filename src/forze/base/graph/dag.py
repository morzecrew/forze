from graphlib import CycleError, TopologicalSorter
from typing import Hashable, Iterable, Self

import attrs

from forze.base.errors import CoreError

from .utils import predecessors

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DirectedAcyclicGraph[T: Hashable]:
    """Immutable directed acyclic graph.

    Built-in cycle check and topological ordering derivation.
    """

    predecessors: dict[T, set[T]] = attrs.field(factory=dict)
    """Predecessors map: node -> predecessors."""

    # ....................... #

    @classmethod
    def from_edges(
        cls,
        nodes: Iterable[T],
        edges: Iterable[tuple[T, T]],
        *,
        u_before_v: bool = True,
    ) -> Self:
        preds = predecessors(nodes, edges, u_before_v=u_before_v)

        return cls(predecessors=preds)

    # ....................... #

    def validate(self) -> None:
        sorter = TopologicalSorter(self.predecessors)

        try:
            sorter.prepare()

        except CycleError as e:
            raise CoreError(f"Graph contains a cycle: {e}") from e

    # ....................... #

    def static_order(self) -> tuple[T, ...]:
        """One valid topological order (stdlib tie-breaking only)."""

        sorter = TopologicalSorter(self.predecessors)

        try:
            return tuple(sorter.static_order())

        except CycleError as e:
            raise CoreError(f"Graph contains a cycle: {e}") from e

    # ....................... #

    def topological_order(self) -> tuple[T, ...]:
        """Topological order with optional deterministic tie-break among ready nodes."""

        sorter = TopologicalSorter(self.predecessors)

        try:
            sorter.prepare()

        except CycleError as e:
            raise CoreError(f"Graph contains a cycle: {e}") from e

        order: list[T] = []

        while sorter.is_active():
            ready = list(sorter.get_ready())

            for node in ready:
                order.append(node)
                sorter.done(node)

        return tuple(order)
