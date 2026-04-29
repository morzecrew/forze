"""Enums and value types for graph contracts."""

from enum import StrEnum
from typing import final

# ----------------------- #
#! Maybe literals


@final
class GraphDirection(StrEnum):
    """Edge direction for neighborhood and walk queries (relative to the origin)."""

    OUT = "out"
    """Follow only edges whose tail is the current vertex (outgoing)."""

    IN = "in"
    """Follow only edges whose head is the current vertex (incoming)."""

    BOTH = "both"
    """Follow edges in both directions (semantics depend on :class:`GraphEdgeDirectionality`)."""


# ....................... #


@final
class GraphEdgeDirectionality(StrEnum):
    """How an edge kind is stored and interpreted across backends (Neo4j, ArangoDB)."""

    DIRECTED = "directed"
    """Tail-to-head is the canonical write direction; reverse traversal uses ``IN``/incoming."""

    SYMMETRIC = "symmetric"
    """Semantically undirected: queries typically use :attr:`~GraphDirection.BOTH` on this kind."""
