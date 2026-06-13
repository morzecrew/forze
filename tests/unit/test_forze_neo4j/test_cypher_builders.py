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


# ----------------------- #
# interior (full-path) tenant scoping


def test_neighbors_anchor_only_leaves_terminal_unscoped() -> None:
    # tenant_field set but interior=False -> only the anchor carries the tenant map.
    q = builders.neighbors(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        tenant_field="tenant_id",
    )
    assert "{id: $key, tenant_id: $tenant}" in q
    assert "(m)" in q  # terminal node unconstrained


def test_neighbors_interior_scopes_terminal_node() -> None:
    q = builders.neighbors(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        tenant_field="tenant_id",
        interior=True,
    )
    assert "(m {tenant_id: $tenant})" in q


def test_neighbors_interior_noop_without_tenant_field() -> None:
    q = builders.neighbors(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        interior=True,
    )
    assert "$tenant" not in q
    assert "(m)" in q


def test_expand_interior_constrains_all_path_nodes() -> None:
    q = builders.expand(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        max_depth=3,
        tenant_field="tenant_id",
        interior=True,
    )
    assert "WHERE all(_n IN nodes(path) WHERE _n.`tenant_id` = $tenant)" in q


def test_expand_anchor_only_has_no_path_predicate() -> None:
    q = builders.expand(
        label="User",
        key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        max_depth=3,
        tenant_field="tenant_id",
    )
    assert "all(_n IN nodes(path)" not in q


def test_shortest_path_interior_constrains_all_path_nodes() -> None:
    q = builders.shortest_path(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        direction=GraphDirection.OUT,
        edge_types=[],
        max_hops=5,
        tenant_field="tenant_id",
        interior=True,
    )
    assert "shortestPath((a)" in q
    assert "WHERE all(_n IN nodes(path) WHERE _n.`tenant_id` = $tenant)" in q
