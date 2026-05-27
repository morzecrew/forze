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

    Built during resolution when a recording :class:`~forze.application.execution.deps.resolution_tracer.ResolutionTracer`
    is enabled on :class:`~forze.application.execution.deps.container.Deps`.
    Export via :meth:`to_dag` or :meth:`to_key_dag` for topological inspection.
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

    # ....................... #

    def canonical_edges(self) -> frozenset[tuple[str, str]]:
        """Return edges with routes collapsed to dependency key names only."""

        return frozenset(
            (parent.key_name, child.key_name) for parent, child in self.edges
        )

    # ....................... #

    def to_key_dag(self) -> DirectedAcyclicGraph[str]:
        """Build an immutable DAG from canonical key-level edges."""

        edges = self.canonical_edges()
        nodes = {name for pair in edges for name in pair}

        return DirectedAcyclicGraph.from_edges(nodes, edges)

    # ....................... #

    def format_canonical_edges(self) -> str:
        """Return human-readable canonical edge lines for logging."""

        lines = sorted(f"{parent} -> {child}" for parent, child in self.canonical_edges())

        return "\n".join(lines)
