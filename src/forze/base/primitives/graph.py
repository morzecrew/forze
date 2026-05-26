from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import Any, Callable, Hashable, Iterable, Iterator, cast

import attrs

from ..exceptions import exc

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
    def from_edges[X: Hashable](
        cls,
        nodes: Iterable[X] | set[X],
        edges: Iterable[tuple[X, X]] | set[tuple[X, X]],
        *,
        u_before_v: bool = True,
    ) -> DirectedAcyclicGraph[X]:
        preds = build_predecessor_map(nodes, edges, u_before_v=u_before_v)

        g = cast(type[DirectedAcyclicGraph[X]], cls)(predecessors=preds)
        g.validate()

        return g

    # ....................... #

    def validate(self) -> None:
        sorter = TopologicalSorter(self.predecessors)

        try:
            sorter.prepare()

        except CycleError as e:
            raise exc.internal(f"Graph contains a cycle: {e}") from e

    # ....................... #

    def static_order(self) -> tuple[T, ...]:
        """One valid topological order (stdlib tie-breaking only)."""

        sorter = TopologicalSorter(self.predecessors)

        try:
            return tuple(sorter.static_order())

        except CycleError as e:
            raise exc.internal(f"Graph contains a cycle: {e}") from e

    # ....................... #

    def topological_order(
        self,
        *,
        ready_sort_key: Callable[[T], Any] | None = None,
    ) -> tuple[T, ...]:
        """Topological order with optional deterministic tie-break among ready nodes."""

        return tuple(
            n
            for b in self.topological_batches(ready_sort_key=ready_sort_key)
            for n in b
        )

    # ....................... #

    def topological_batches(
        self,
        *,
        ready_sort_key: Callable[[T], Any] | None = None,
    ) -> Iterator[tuple[T, ...]]:
        """Iterate over topological batches of nodes.

        Each batch is a tuple of nodes that are ready to be executed.
        """

        sorter = TopologicalSorter(self.predecessors)

        try:
            sorter.prepare()

        except CycleError as e:
            raise exc.internal(f"Graph contains a cycle: {e}") from e

        while sorter.is_active():
            ready = list(sorter.get_ready())
            ready.sort(key=ready_sort_key or str)

            if not ready:
                break

            yield tuple(ready)

            for node in ready:
                sorter.done(node)


# ....................... #


def build_predecessor_map[T: Hashable](
    nodes: Iterable[T] | set[T],
    edges: Iterable[tuple[T, T]] | set[tuple[T, T]],
    *,
    u_before_v: bool = True,
) -> dict[T, set[T]]:
    """Build predecessor map for ``graphlib.TopologicalSorter``.

    If ``u_before_v=True`` (default), then each edge ``(u, v)`` is interpreted
    as ``v`` depends on ``u``, i.e. ``u -> v``.
    """

    preds: dict[T, set[T]] = {node: set() for node in nodes}
    universe = frozenset(preds)

    for a, b in edges:
        if a not in universe or b not in universe:
            raise exc.internal(f"Edge ({a!r}, {b!r}) references unknown node")

        if a == b:
            raise exc.internal(f"Edge ({a!r}, {b!r}) is a self-loop")

        u, v = (a, b) if u_before_v else (b, a)
        preds[v].add(u)

    return preds
