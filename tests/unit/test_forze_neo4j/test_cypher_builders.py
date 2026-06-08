"""Unit tests for the openCypher string builders."""

from forze.application.contracts.graph import GraphDirection
from forze_neo4j.kernel.cypher import builders


def test_quote_escapes_backticks() -> None:
    assert builders.quote("Us`er") == "`Us``er`"


def test_get_vertex_plain_and_tenant() -> None:
    plain = builders.get_vertex("User", "id")
    assert "MATCH (n:`User` {id: $key})" in plain
    assert "$tenant" not in plain

    scoped = builders.get_vertex("User", "id", tenant_field="tenant_id")
    assert "{id: $key, tenant_id: $tenant}" in scoped


def test_create_edge_merge_vs_create() -> None:
    created = builders.create_edge(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        edge_type="FOLLOWS",
        merge=False,
    )
    assert "CREATE (a)-[r:`FOLLOWS`]->(b)" in created

    merged = builders.create_edge(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        edge_type="FOLLOWS",
        merge=True,
    )
    assert "MERGE (a)-[r:`FOLLOWS`]->(b)" in merged
    assert "ON CREATE SET r += $props" in merged


def test_neighbors_direction_arrows() -> None:
    out = builders.neighbors(
        label="User", key_field="id", direction=GraphDirection.OUT, edge_types=["FOLLOWS"]
    )
    assert "-[r:`FOLLOWS`]->(m)" in out

    inc = builders.neighbors(
        label="User", key_field="id", direction=GraphDirection.IN, edge_types=["FOLLOWS"]
    )
    assert "<-[r:`FOLLOWS`]-(m)" in inc

    both = builders.neighbors(
        label="User", key_field="id", direction=GraphDirection.BOTH, edge_types=[]
    )
    assert "-[r]-(m)" in both  # no type filter when edge_types empty


def test_expand_inlines_depth() -> None:
    q = builders.expand(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        max_depth=3,
    )
    assert "*1..3" in q


def test_shortest_path_inlines_hops() -> None:
    q = builders.shortest_path(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        direction=GraphDirection.OUT,
        edge_types=[],
        max_hops=5,
    )
    assert "shortestPath((a)" in q
    assert "*..5" in q
