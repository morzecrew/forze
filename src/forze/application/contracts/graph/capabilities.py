"""Which read features a graph backend supports — reported, not assumed.

Streaming a graph means walking it by **keyset**: order by the kind's key field, then ask for
the rows after the last one seen. Offset pagination cannot do this job — a graph being written
while it is walked shifts every row's offset, so pages silently skip and repeat — which is why
the streaming reads are a separate contract rather than a bigger ``limit``.

Not every backend can seek that way, so the capability is **declared** and checked, and a
backend that does not report is treated as supporting nothing. The alternative to failing
closed here is a partial scan that looks like a complete one, and an export built on that
carries a subset of the graph and says it carried the graph.
"""

from collections.abc import AsyncGenerator, Sequence
from typing import Protocol, runtime_checkable

import attrs

from .value_objects import ExportedEdge

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class GraphReadCapabilities:
    """Read features a :class:`GraphQueryPort` backend supports.

    Reported through the opt-in :class:`GraphStreamingAware` protocol. Both default to
    ``False``: a backend that cannot report its capabilities has none, so a stream call
    against it fails closed rather than falling back to something that looks similar.
    """

    supports_vertex_streaming: bool = False
    """Can ``find_vertices_stream`` keyset-walk a node kind to exhaustion?"""

    supports_edge_streaming: bool = False
    """Can ``find_edges_stream`` keyset-walk an edge kind to exhaustion?

    Separate from vertices because it is strictly harder: an edge only has somewhere to
    bookmark when it has a key of its own (``identity="key"``). See
    :func:`~forze.application.integrations.graph.assert_edge_streamable`.
    """


# ....................... #


@runtime_checkable
class GraphStreamingAware(Protocol):
    """Opt-in extension for graph query ports that report their read capabilities.

    Kept off :class:`GraphQueryPort` so a backend opts in only when it can genuinely seek —
    mirroring :class:`~forze.application.contracts.stream.CommitStreamGroupAware`.
    """

    def read_capabilities(self) -> GraphReadCapabilities:
        """Report the read features this backend supports."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class GraphEdgeExportAware(Protocol):
    """Opt-in extension for graph query ports that can stream edges **with their endpoints**.

    :meth:`~forze.application.contracts.graph.GraphQueryPort.find_edges_stream` yields read models
    only — an edge's own properties — which is enough to display an edge but not to re-create one,
    because an edge's identity is its ``(from, to)`` endpoints and those are not stored properties.
    A backend that can keyset-walk edges already reads their endpoints for the cursor, so surfacing
    them costs nothing. Kept off :class:`GraphQueryPort` (a sibling of :class:`GraphStreamingAware`)
    so it stays non-breaking; a port without it simply cannot have its graph edges exported, and the
    portability driver refuses, naming the module rather than shipping edgeless vertices.
    """

    def find_edges_export_stream(
        self,
        edge_kind: str,
        *,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[ExportedEdge]]:
        """Yield keyset batches of every edge of *edge_kind* as :class:`ExportedEdge`\\ s.

        The walk :meth:`~forze.application.contracts.graph.GraphQueryPort.find_edges_stream` does —
        same cursor, same completeness, same duplicate-pair handling — but each row carries its
        endpoints and read model, the two halves an import needs to ``ensure_edge`` it back.
        """
        ...  # pragma: no cover
