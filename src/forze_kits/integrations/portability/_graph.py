"""Graph-plane row shaping — the vertex/edge analog of ``_core``'s ``portable_row``/``keyed_create``.

A **vertex** kind declares a create model (like a document), so a vertex row decodes straight
through it, key and all. An **edge** kind does *not* declare a create model — the create command is
app-defined and validated by the adapter, which reads the endpoints off it and stores the rest as
properties — so an edge row is reshaped into a permissive command the adapter can read: its
endpoints (which ``find_edges_stream`` drops and :class:`ExportedEdge` restores) kept apart from its
own read-model properties.

The archive layout is ``graph/<module>/nodes/<kind>`` and ``graph/<module>/edges/<kind>`` — a
per-module split so import restores every vertex kind *before* any edge kind (an edge needs its
endpoints to exist), and node-vs-edge is legible from the path, not guessed.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel, ConfigDict

from forze.application.contracts.graph import ExportedEdge
from forze.base.primitives import JsonDict

from ._core import portable_row
from .format import Compression, data_suffix

# ----------------------- #

_NODES = "nodes"
_EDGES = "edges"


class _EdgeCreate(BaseModel):
    """A permissive create command for a graph edge on import.

    Graph edge kinds declare no create model (unlike node kinds and documents): the create DTO is
    app-defined and the adapter reads ``from_key``/``to_key`` off it and stores the rest as
    properties. So import feeds a command built from the archive row — ``extra="allow"`` lets every
    field through to the adapter's ``model_dump``, and the adapter seals any encrypted properties on
    write, the same re-seal a document field gets.
    """

    model_config = ConfigDict(extra="allow")


# ....................... #


def node_file(module: str, kind: str, compression: Compression) -> str:
    """Archive path for one vertex kind's rows."""

    return f"graph/{module}/{_NODES}/{kind}{data_suffix(compression)}"


def edge_file(module: str, kind: str, compression: Compression) -> str:
    """Archive path for one edge kind's rows."""

    return f"graph/{module}/{_EDGES}/{kind}{data_suffix(compression)}"


# ....................... #


def exported_edge_row(edge: ExportedEdge) -> JsonDict:
    """The archive row for one edge: its endpoints kept apart from its read-model properties.

    Separating the structural endpoints from the stored properties keeps the row unambiguous — a
    property could never collide with an endpoint field — and self-documenting.
    """

    return {
        "from": {"kind": edge.from_kind, "key": edge.from_key},
        "to": {"kind": edge.to_kind, "key": edge.to_key},
        "props": portable_row(edge.model),
    }


def edge_create_from_row(row: JsonDict) -> _EdgeCreate:
    """Rebuild the adapter-facing edge create command from an archive row.

    The properties, plus the endpoints as the transient ``from_kind``/``from_key`` /
    ``to_kind``/``to_key`` routing fields the adapter pops (``from_kind``/``to_kind`` disambiguate
    the pair for a multi-endpoint kind; a single-endpoint kind ignores them).
    """

    frm = cast("dict[str, Any]", row["from"])
    to = cast("dict[str, Any]", row["to"])
    props = cast("dict[str, Any]", row["props"])

    return _EdgeCreate.model_validate(
        {
            **props,
            "from_kind": frm["kind"],
            "from_key": frm["key"],
            "to_kind": to["kind"],
            "to_key": to["key"],
        }
    )
