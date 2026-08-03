"""Shared wiring for the graph differential leg — one spec, one fixture, two engines.

The scenarios in :mod:`forze_dst.conformance.graph` are engine-agnostic but not
model-agnostic: every graph is application-defined, so the DTOs and the module spec live
here and both legs (mock, Neo4j) drive the same script through them.
"""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
)

# ----------------------- #

HUB_KIND = "ConfHub"
WANTED_KIND = "ConfWanted"
UNWANTED_KIND = "ConfUnwanted"
EDGE_KIND = "CONF_LINKED"

# ....................... #


class ConfVertexRead(BaseModel):
    """Read DTO shared by every vertex kind in the leg's spec."""

    id: str


class ConfVertexCreate(BaseModel):
    """Create DTO shared by every vertex kind in the leg's spec."""

    id: str


class ConfEdgeRead(BaseModel):
    """Read DTO for the leg's single edge kind — endpoint-identified, no properties."""


class ConfEdgeCreate(BaseModel):
    """Create DTO for the leg's edge kind.

    ``from_kind`` / ``to_kind`` are the routing fields a multi-endpoint edge kind requires:
    the scenarios link three different vertex kinds through one relationship type, which is
    exactly the shape that cannot be inferred.
    """

    from_key: str
    to_key: str
    from_kind: str
    to_kind: str


# ....................... #


def graph_conformance_spec() -> GraphModuleSpec:
    """The module spec both legs wire: three vertex kinds, one edge kind between any pair."""

    kinds = (HUB_KIND, WANTED_KIND, UNWANTED_KIND)

    return GraphModuleSpec(
        name="graph_conformance",
        nodes=tuple(
            GraphNodeSpec(name=kind, read=ConfVertexRead, create=ConfVertexCreate)
            for kind in kinds
        ),
        edges=(
            GraphEdgeSpec(
                name=EDGE_KIND,
                read=ConfEdgeRead,
                identity="endpoints",
                endpoints=tuple(
                    GraphEdgeEndpoint(from_kind=from_kind, to_kind=to_kind)
                    for from_kind in kinds
                    for to_kind in kinds
                ),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


@final
@attrs.define(slots=True, frozen=True)
class GraphConformanceFixture:
    """Builds the spec's command payloads for the engine-agnostic scenarios."""

    def vertex(self, kind: str, key: str) -> Any:
        _ = kind  # every kind shares one create DTO here
        return ConfVertexCreate(id=key)

    def edge(
        self,
        *,
        from_kind: str,
        from_key: str,
        to_kind: str,
        to_key: str,
    ) -> Any:
        return ConfEdgeCreate(
            from_key=from_key,
            to_key=to_key,
            from_kind=from_kind,
            to_kind=to_kind,
        )
