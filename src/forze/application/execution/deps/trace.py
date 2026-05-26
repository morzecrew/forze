"""Observed dependency resolution graph for development diagnostics."""

from __future__ import annotations

from typing import final

import attrs

from forze.base.primitives import DirectedAcyclicGraph

from .resolution import ResolutionFrame

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class DepsResolutionTrace:
    """Mutable accumulator of observed resolution edges for one container and task.

    Built during resolution when :attr:`~forze.application.execution.deps.container.Deps.trace_resolution`
    is enabled. Export via :meth:`to_dag` for topological inspection.
    """

    edges: set[tuple[ResolutionFrame, ResolutionFrame]] = attrs.field(factory=set)
    """Observed directed edges ``(parent, child)`` where the child depends on the parent."""

    # ....................... #

    def add_edge(self, parent: ResolutionFrame, child: ResolutionFrame) -> None:
        """Record a dependency edge (idempotent)."""

        self.edges.add((parent, child))

    # ....................... #

    def nodes(self) -> frozenset[ResolutionFrame]:
        """Return all frames that appear as edge endpoints."""

        out: set[ResolutionFrame] = set()

        for parent, child in self.edges:
            out.add(parent)
            out.add(child)

        return frozenset(out)

    # ....................... #

    def to_dag(self) -> DirectedAcyclicGraph[ResolutionFrame]:
        """Build an immutable DAG from observed edges."""

        return DirectedAcyclicGraph.from_edges(self.nodes(), self.edges)

    # ....................... #

    def format_edges(self) -> str:
        """Return human-readable edge lines for logging."""

        lines = sorted(
            f"{parent.label()} -> {child.label()}" for parent, child in self.edges
        )

        return "\n".join(lines)
