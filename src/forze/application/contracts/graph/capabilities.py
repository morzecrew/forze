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

from typing import Protocol, runtime_checkable

import attrs

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
