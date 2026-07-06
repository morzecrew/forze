"""Unit tests for the openCypher string builders."""

import pytest

from forze.application.contracts.graph import GraphDirection
from forze_neo4j.kernel.cypher import builders


def test_quote_escapes_backticks() -> None:
    assert builders.quote("Us`er") == "`Us``er`"


def test_get_vertex_plain_and_tenant() -> None:
    plain = builders.get_vertex("User", "id")
    assert "MATCH (n:`User` {`id`: $key})" in plain
    assert "$tenant" not in plain

    scoped = builders.get_vertex("User", "id", tenant_field="tenant_id")
    assert "{`id`: $key, `tenant_id`: $tenant}" in scoped


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


def test_create_edge_keyed_merge_matches_on_key() -> None:
    """A keyed-edge MERGE must carry the key so distinct keyed edges stay separate."""

    merged = builders.create_edge(
        from_label="Account",
        from_key_field="id",
        to_label="Account",
        to_key_field="id",
        edge_type="TRANSFER",
        merge=True,
        key_field="ref",
    )
    # The key is part of the relationship identity, not just set as a property.
    assert "MERGE (a)-[r:`TRANSFER` {`ref`: $edge_key}]->(b)" in merged


def test_create_edge_keyed_merge_scopes_identity_by_tenant() -> None:
    """Under tagged tenancy the tenant is folded into the keyed merge identity so a
    foreign tenant reusing the key gets its own edge (not the other tenant's)."""

    merged = builders.create_edge(
        from_label="Account",
        from_key_field="id",
        to_label="Account",
        to_key_field="id",
        edge_type="TRANSFER",
        merge=True,
        key_field="ref",
        tenant_field="tenant_id",
    )
    assert (
        "MERGE (a)-[r:`TRANSFER` {`ref`: $edge_key, `tenant_id`: $tenant}]->(b)" in merged
    )


def test_create_edge_keyed_create_does_not_add_key_pattern() -> None:
    """``merge=False`` never keys the pattern (CREATE always makes a fresh edge)."""

    created = builders.create_edge(
        from_label="Account",
        from_key_field="id",
        to_label="Account",
        to_key_field="id",
        edge_type="TRANSFER",
        merge=False,
        key_field="ref",
    )
    assert "CREATE (a)-[r:`TRANSFER`]->(b)" in created
    assert "$edge_key" not in created


# ----------------------- #
# schema provisioning


def test_node_uniqueness_constraint_single_vs_composite() -> None:
    single = builders.node_uniqueness_constraint("c1", "User", "id")
    assert single == (
        "CREATE CONSTRAINT `c1` IF NOT EXISTS FOR (n:`User`) REQUIRE n.`id` IS UNIQUE"
    )

    composite = builders.node_uniqueness_constraint(
        "c2", "User", "id", tenant_field="tenant_id"
    )
    # Tenant-scoped uniqueness is composite (key unique within a tenant).
    assert "REQUIRE (n.`id`, n.`tenant_id`) IS UNIQUE" in composite
    assert composite.endswith("IS UNIQUE")


def test_edge_uniqueness_constraint_targets_relationship() -> None:
    stmt = builders.edge_uniqueness_constraint("c3", "TRANSFER", "ref")
    assert stmt == (
        "CREATE CONSTRAINT `c3` IF NOT EXISTS "
        "FOR ()-[r:`TRANSFER`]-() REQUIRE r.`ref` IS UNIQUE"
    )


def test_edge_uniqueness_constraint_composite_under_tenancy() -> None:
    # Tenant-scoped edge-key uniqueness is composite (key unique within a tenant), so two
    # tenants may validly reuse a key without a cross-tenant conflict.
    stmt = builders.edge_uniqueness_constraint(
        "c3", "TRANSFER", "ref", tenant_field="tenant_id"
    )
    assert "REQUIRE (r.`ref`, r.`tenant_id`) IS UNIQUE" in stmt
    assert stmt.endswith("IS UNIQUE")


def test_property_index_and_drops() -> None:
    assert builders.property_index("i1", "User", "tenant_id") == (
        "CREATE INDEX `i1` IF NOT EXISTS FOR (n:`User`) ON (n.`tenant_id`)"
    )
    assert builders.drop_constraint("c1") == "DROP CONSTRAINT `c1` IF EXISTS"
    assert builders.drop_index("i1") == "DROP INDEX `i1` IF EXISTS"


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


def test_k_shortest_paths_uses_native_shortest_k() -> None:
    q = builders.k_shortest_paths(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        direction=GraphDirection.OUT,
        edge_types=["FOLLOWS"],
        max_hops=5,
        k=3,
    )
    assert "SHORTEST 3 (a)" in q
    assert "*..5" in q
    assert "ORDER BY length(path)" in q


def test_k_shortest_paths_coerces_k_and_hops() -> None:
    with pytest.raises(ValueError):
        builders.k_shortest_paths(
            from_label="User",
            from_key_field="id",
            to_label="User",
            to_key_field="id",
            direction=GraphDirection.OUT,
            edge_types=[],
            max_hops=5,
            k="3) MATCH (x) DETACH DELETE x //",  # type: ignore[arg-type]
        )


def test_gds_project_weighted_tenant_filters_and_maps_weight() -> None:
    q = builders.gds_project_weighted(
        edge_types=["FOLLOWS"], weight_property="cost", tenant_field="tenant_id"
    )
    assert "MATCH (s)-[r:`FOLLOWS`]->(t)" in q
    assert "WHERE s.`tenant_id` = $tenant AND t.`tenant_id` = $tenant" in q
    assert "relationshipProperties: {weight: r.`cost`}" in q
    assert "gds.graph.project($graph_name" in q


def test_gds_project_weighted_untenanted_has_no_where() -> None:
    q = builders.gds_project_weighted(edge_types=[], weight_property="cost")
    assert "WHERE" not in q
    assert "MATCH (s)-[r]->(t)" in q


def test_gds_weighted_paths_uses_yens_and_rebuilds_edges() -> None:
    q = builders.gds_weighted_paths(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        edge_types=["FOLLOWS"],
        weight_property="cost",
        tenant_field=None,
    )
    assert "gds.shortestPath.yens.stream($graph_name" in q
    assert "relationshipWeightProperty: 'weight'" in q
    # edges rebuilt as the min-weight real edge between consecutive nodes
    assert "gds.util.asNode(nodeIds[i])" in q
    assert "ORDER BY coalesce(r.`cost`, 0.0) ASC LIMIT 1" in q
    # max_hops bounds the search: over-fetch cost-ordered candidates, drop over-long ones,
    # then keep the cheapest k (not a post-filter of the top-k).
    assert "k: $candidate_k" in q
    assert "WHERE size(nodeIds) - 1 <= $max_hops" in q
    assert q.rstrip().endswith("LIMIT $k")


def test_gds_drop_is_non_failing() -> None:
    assert builders.gds_drop() == (
        "CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName"
    )


def test_k_shortest_paths_scopes_interior_nodes_by_tenant() -> None:
    q = builders.k_shortest_paths(
        from_label="User",
        from_key_field="id",
        to_label="User",
        to_key_field="id",
        direction=GraphDirection.OUT,
        edge_types=[],
        max_hops=4,
        k=2,
        tenant_field="tenant_id",
        interior=True,
    )
    assert "all(_n IN nodes(path) WHERE _n.`tenant_id` = $tenant)" in q


def test_shortest_path_coerces_max_hops_blocking_injection() -> None:
    """The inlined quantifier goes through ``int()`` — a non-integer string can't reach
    the query text (defense-in-depth even though the field is typed ``int``)."""

    with pytest.raises(ValueError):
        builders.shortest_path(
            from_label="User",
            from_key_field="id",
            to_label="User",
            to_key_field="id",
            direction=GraphDirection.OUT,
            edge_types=[],
            max_hops="5]->() DETACH DELETE n //",  # type: ignore[arg-type]
        )


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
    assert "{`id`: $key, `tenant_id`: $tenant}" in q
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
    assert "(m {`tenant_id`: $tenant})" in q


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


# ----------------------- #
# scoped_walk


def test_scoped_walk_single_segment_plain() -> None:
    q = builders.scoped_walk(
        anchor_label="User",
        anchor_key_field="id",
        segments=[(GraphDirection.OUT, ["FOLLOWS"], 1, 3)],
        target_label="User",
    )
    assert "MATCH path = (n0:`User` {`id`: $key})" in q
    assert "-[:`FOLLOWS`*1..3]->(m:`User`)" in q
    assert "RETURN DISTINCT properties(m) AS m" in q
    assert "$tenant" not in q  # no tenant field → no scoping


def test_scoped_walk_tenant_scoped_anchor_target_and_path() -> None:
    q = builders.scoped_walk(
        anchor_label="User",
        anchor_key_field="id",
        segments=[(GraphDirection.OUT, ["FOLLOWS"], 1, 2)],
        target_label="User",
        tenant_field="tenant_id",
    )
    assert "(n0:`User` {`id`: $key, `tenant_id`: $tenant})" in q  # anchor scoped
    assert "(m:`User` {`tenant_id`: $tenant})" in q  # target scoped
    assert "WHERE all(_n IN nodes(path) WHERE _n.`tenant_id` = $tenant)" in q  # interior


def test_scoped_walk_multi_segment_chains_with_junction() -> None:
    q = builders.scoped_walk(
        anchor_label="User",
        anchor_key_field="id",
        segments=[
            (GraphDirection.OUT, ["FOLLOWS"], 1, 1),
            (GraphDirection.IN, ["LIKES"], 1, 2),
        ],
        target_label="Post",
        tenant_field="tenant_id",
    )
    # segment 1 → anonymous junction () → segment 2 → typed target
    assert "-[:`FOLLOWS`*1..1]->()<-[:`LIKES`*1..2]-(m:`Post` {`tenant_id`: $tenant})" in q
