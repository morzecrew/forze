"""Pure openCypher string builders (no driver/engine state).

Kept free of Neo4j-driver types so a future openCypher sibling (Memgraph, Neptune,
AGE) can reuse them. Queries return property maps (``properties(x)``) so result rows
materialize as plain dicts. Variable-length bounds are inlined (Cypher disallows a
parameter for the ``*min..max`` quantifier); callers must pass validated integers.

When ``tenant_field`` is supplied, anchor nodes (the matched vertex, edge, or path
endpoints) are additionally constrained by ``{<tenant_field>: $tenant}`` for
tenant-property isolation; the parameter ``$tenant`` must then be provided.

For traversals (``neighbors`` / ``expand`` / ``shortest_path``) passing ``interior=True``
additionally constrains every *interior and terminal* node on the path, not just the
anchor — so a cross-tenant edge cannot leak a foreign node's properties (defense-in-depth
that does not depend on the "no edge crosses a tenant boundary" write-path invariant).
"""

import re
from collections.abc import Iterable, Sequence

from forze.application.contracts.graph import GraphDirection

# ----------------------- #


def quote(name: str) -> str:
    """Backtick-quote a label / relationship type, escaping embedded backticks."""

    return "`" + name.replace("`", "``") + "`"


# ....................... #


def _match_map(key_field: str, tenant_field: str | None, *, key_param: str = "key") -> str:
    if tenant_field:
        return f"{{{quote(key_field)}: ${key_param}, {quote(tenant_field)}: $tenant}}"

    return f"{{{quote(key_field)}: ${key_param}}}"


# ....................... #


def _rel_key_map(key_field: str | None, tenant_field: str | None, *, key_param: str) -> str:
    """Inline the keyed-edge ``MERGE`` relationship map.

    Empty for a keyless merge; ``{<key>: $key_param}`` for a keyed edge, with
    ``, <tenant>: $tenant`` folded in under tagged tenancy so the merge identity
    (and the stamped relationship property) is scoped per tenant.
    """

    if not key_field:
        return ""

    if tenant_field:
        return f" {{{quote(key_field)}: ${key_param}, {quote(tenant_field)}: $tenant}}"

    return f" {{{quote(key_field)}: ${key_param}}}"


# ....................... #


def _tenant_only_map(tenant_field: str | None, *, interior: bool) -> str:
    """Inline ``{<tenant_field>: $tenant}`` for an adjacent (keyless) traversal node."""

    if tenant_field and interior:
        return f" {{{quote(tenant_field)}: $tenant}}"

    return ""


def _path_tenant_pred(tenant_field: str | None, *, interior: bool, path_var: str = "path") -> str:
    """``WHERE``-clause constraining every node on *path_var* to ``$tenant``.

    Used for variable-length and shortest-path matches where interior nodes cannot be
    pinned with an inline property map.
    """

    if not (tenant_field and interior):
        return ""

    return f"WHERE all(_n IN nodes({path_var}) WHERE _n.{quote(tenant_field)} = $tenant)\n"


# ....................... #


def _type_pattern(types: Iterable[str]) -> str:
    kinds = list(types)

    return ":" + "|".join(quote(t) for t in kinds) if kinds else ""


# ....................... #


def _rel(
    direction: GraphDirection,
    type_pattern: str,
    *,
    quant: str = "",
    var: str = "r",
) -> str:
    body = f"[{var}{type_pattern}{quant}]"

    if direction is GraphDirection.OUT:
        return f"-{body}->"

    return f"<-{body}-" if direction is GraphDirection.IN else f"-{body}-"


# ....................... #
# Vertices


def get_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\nRETURN properties(n) AS n"
    )


# ....................... #


def vertex_exists(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"RETURN count(n) > 0 AS exists"
    )


# ....................... #


def create_vertex(label: str) -> str:
    return f"CREATE (n:{quote(label)})\nSET n = $props\nRETURN properties(n) AS n"


# ....................... #


def update_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"SET n += $props\nRETURN properties(n) AS n"
    )


# ....................... #


def delete_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\nDETACH DELETE n"


# ....................... #
# Edges


def get_edge_by_key(
    edge_type: str,
    key_field: str,
    *,
    tenant_field: str | None = None,
) -> str:
    return (
        f"MATCH ()-[r:{quote(edge_type)} {_match_map(key_field, tenant_field)}]->()\n"
        f"RETURN properties(r) AS r"
    )


# ....................... #


def create_edge(
    *,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    edge_type: str,
    merge: bool,
    tenant_field: str | None = None,
    key_field: str | None = None,
    key_param: str = "edge_key",
) -> str:
    """Create (or, when ``merge``, idempotently ensure) a directed edge a→b.

    For a *keyed* edge (``key_field`` set) a ``merge`` includes the key in the
    relationship pattern (``MERGE (a)-[r:T {<key_field>: $edge_key}]->(b)``) so
    distinct keyed edges between the same pair are separate identities — without it
    the ``MERGE`` matches any edge of the type and collapses them. Under tagged
    tenancy the ``tenant_field`` is folded into that same map so the merge identity
    is ``(key, tenant)``: a foreign tenant reusing the key gets its own edge (and the
    property is stamped for the composite edge-uniqueness constraint) rather than
    matching another tenant's relationship.
    """

    head = (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
    )

    if merge:
        rel_key = _rel_key_map(key_field, tenant_field, key_param=key_param)
        body = f"MERGE (a)-[r:{quote(edge_type)}{rel_key}]->(b)\nON CREATE SET r += $props\n"
    else:
        body = f"CREATE (a)-[r:{quote(edge_type)}]->(b)\nSET r += $props\n"

    return head + body + "RETURN properties(r) AS r"


# ....................... #
# Traversal


def neighbors(
    *,
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    tenant_field: str | None = None,
    interior: bool = False,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types))
    other = f"(m{_tenant_only_map(tenant_field, interior=interior)})"

    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}{other}\n"
        f"RETURN properties(m) AS other, labels(m) AS other_labels, "
        f"properties(r) AS via_edge, type(r) AS via_type\nLIMIT $limit"
    )


# ....................... #


def expand(
    *,
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    max_depth: int,
    tenant_field: str | None = None,
    interior: bool = False,
) -> str:
    # Coerce to int before inlining: the ``*1..n`` quantifier can't be parameterized, so
    # this value is interpolated into the query text — ``int()`` keeps a non-integer from
    # ever reaching it (defense-in-depth even though the field is typed).
    rel = _rel(direction, _type_pattern(edge_types), quant=f"*1..{int(max_depth)}")

    return (
        f"MATCH path = (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}(m)\n"
        f"{_path_tenant_pred(tenant_field, interior=interior)}"
        f"RETURN length(path) AS depth, "
        f"properties(m) AS vertex, labels(m) AS vertex_labels, "
        f"properties(last(relationships(path))) AS from_parent, "
        f"type(last(relationships(path))) AS from_parent_type, "
        f"properties(nodes(path)[-2]) AS parent, labels(nodes(path)[-2]) AS parent_labels\n"
        f"ORDER BY depth\nLIMIT $max_results"
    )


# ....................... #


def scoped_walk(
    *,
    anchor_label: str,
    anchor_key_field: str,
    segments: Sequence[tuple[GraphDirection, Iterable[str], int, int]],
    target_label: str,
    tenant_field: str | None = None,
) -> str:
    """Tenant-safe multi-segment walk: anchor → chained var-length segments → typed target.

    Every node on the path is tenant-constrained (anchor inline, interior + target via the
    ``WHERE all(...)`` predicate), so the traversal cannot cross tenants. Anonymous junction
    nodes separate consecutive segments. Returns distinct target property maps.
    """

    parts = [f"(n0:{quote(anchor_label)} {_match_map(anchor_key_field, tenant_field)})"]
    last = len(segments) - 1

    for i, (direction, edge_types, lo, hi) in enumerate(segments):
        # Anonymous relationships (no ``r`` var) so multiple segments don't collide; the
        # walk returns only the target vertex, so edge bindings are not needed.
        parts.append(
            _rel(
                direction,
                _type_pattern(edge_types),
                quant=f"*{int(lo)}..{int(hi)}",
                var="",
            )
        )

        if i < last:
            parts.append("()")
        else:
            parts.append(
                f"(m:{quote(target_label)}{_tenant_only_map(tenant_field, interior=True)})"
            )

    pattern = "".join(parts)

    return (
        f"MATCH path = {pattern}\n"
        f"{_path_tenant_pred(tenant_field, interior=True)}"
        f"RETURN DISTINCT properties(m) AS m\nLIMIT $limit"
    )


# ....................... #


def shortest_path(
    *,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    max_hops: int,
    tenant_field: str | None = None,
    interior: bool = False,
) -> str:
    # Coerce to int before inlining: the ``*..n`` quantifier can't be parameterized, so
    # this value is interpolated into the query text — ``int()`` keeps a non-integer from
    # ever reaching it (defense-in-depth even though the field is typed).
    rel = _rel(direction, _type_pattern(edge_types), quant=f"*..{int(max_hops)}")

    # In full-path mode (``interior=True``) the ``all(nodes(path) ...)`` tenant predicate is the
    # canonical all-path-nodes form: Neo4j runs an *exhaustive* shortest-path search and returns
    # the shortest path that satisfies it (the same-tenant path, even when a shorter cross-tenant
    # one exists), emitting only an EXHAUSTIVE_SHORTEST_PATH perf notification — it does not
    # post-filter the global shortest and yield NULL. (With cypher.forbid_exhaustive_shortestpath
    # the engine errors instead — fail-closed — never returning a cross-tenant path.)
    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"MATCH path = shortestPath((a){rel}(b))\n"
        f"{_path_tenant_pred(tenant_field, interior=interior)}"
        f"RETURN [n IN nodes(path) | properties(n)] AS vertices, "
        f"[n IN nodes(path) | labels(n)] AS vertex_labels, "
        f"[e IN relationships(path) | properties(e)] AS edges, "
        f"[e IN relationships(path) | type(e)] AS edge_types"
    )


# ....................... #


def k_shortest_paths(
    *,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    max_hops: int,
    k: int,
    tenant_field: str | None = None,
    interior: bool = False,
) -> str:
    """Up to *k* shortest (simple) paths a→b via Neo4j 5's native ``SHORTEST k``.

    Unweighted / hop-bounded, matching the ``ShortestPathParams`` contract (no edge weights).
    ``k`` and the ``*..n`` bound are inlined (a selective path selector / quantifier cannot be
    parameterized), so both are ``int()``-coerced. Tenant scoping reuses the same all-path-nodes
    predicate as :func:`shortest_path`, applied during path selection.
    """

    rel = _rel(direction, _type_pattern(edge_types), quant=f"*..{int(max_hops)}")

    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"MATCH path = SHORTEST {int(k)} (a){rel}(b)\n"
        f"{_path_tenant_pred(tenant_field, interior=interior)}"
        f"RETURN [n IN nodes(path) | properties(n)] AS vertices, "
        f"[n IN nodes(path) | labels(n)] AS vertex_labels, "
        f"[e IN relationships(path) | properties(e)] AS edges, "
        f"[e IN relationships(path) | type(e)] AS edge_types\n"
        f"ORDER BY length(path)"
    )


# ....................... #
# Schema provisioning (constraints / indexes)


def node_uniqueness_constraint(
    name: str,
    label: str,
    key_field: str,
    *,
    tenant_field: str | None = None,
) -> str:
    """A node key-uniqueness constraint (composite with ``tenant_field`` when given).

    Under tagged tenancy the key is unique *within* a tenant, so the constraint is composite
    (``(n.key, n.tenant)``); otherwise it is a single-property uniqueness. Composite
    uniqueness is Community-edition (a NODE KEY constraint, which also enforces existence, is
    Enterprise-only).
    """

    if tenant_field:
        require = f"(n.{quote(key_field)}, n.{quote(tenant_field)})"
    else:
        require = f"n.{quote(key_field)}"

    return (
        f"CREATE CONSTRAINT {quote(name)} IF NOT EXISTS "
        f"FOR (n:{quote(label)}) REQUIRE {require} IS UNIQUE"
    )


def edge_uniqueness_constraint(
    name: str, edge_type: str, key_field: str, *, tenant_field: str | None = None
) -> str:
    """A relationship key-uniqueness constraint (Neo4j 5.7+).

    Backs keyed-edge identity so a concurrent ``ensure_edge`` cannot create two edges of the
    type with the same key (the in-query ``MERGE`` alone races). Under tagged tenancy the key
    is unique *within* a tenant, so the constraint is composite (``(r.key, r.tenant)``) —
    otherwise a second tenant reusing a key would collide with the first tenant's edge.
    """

    if tenant_field:
        require = f"(r.{quote(key_field)}, r.{quote(tenant_field)})"
    else:
        require = f"r.{quote(key_field)}"

    return (
        f"CREATE CONSTRAINT {quote(name)} IF NOT EXISTS "
        f"FOR ()-[r:{quote(edge_type)}]-() REQUIRE {require} IS UNIQUE"
    )


def property_index(name: str, label: str, field: str) -> str:
    """A single-property node index (e.g. the tenant discriminator for scoped matches)."""

    return f"CREATE INDEX {quote(name)} IF NOT EXISTS FOR (n:{quote(label)}) ON (n.{quote(field)})"


def drop_constraint(name: str) -> str:
    return f"DROP CONSTRAINT {quote(name)} IF EXISTS"


def drop_index(name: str) -> str:
    return f"DROP INDEX {quote(name)} IF EXISTS"


# ....................... #
# Weighted paths via GDS (graph-algorithms engine)
#
# GDS runs on a named in-memory projection, so a weighted path is three statements:
#   1. project a tenant-filtered subgraph over the weighted edge types (``gds_project_weighted``),
#   2. run Yen's k-shortest on it and rebuild typed vertices/edges (``gds_weighted_paths``;
#      Yen's with ``k=1`` is the single weighted shortest path),
#   3. drop the projection (``gds_drop`` — always, in a ``finally``, or the catalog leaks).
# The graph name is a ``$graph_name`` parameter; the weight alias inside the projection is the
# fixed literal ``weight`` (the caller's real property is read as ``r.<weight_property>``).


def gds_project_weighted(
    *,
    edge_types: Iterable[str],
    weight_property: str,
    tenant_field: str | None = None,
) -> str:
    """Project a tenant-filtered subgraph (over the weighted edge types) into the GDS catalog."""

    rel = _type_pattern(edge_types)
    where = (
        f"WHERE s.{quote(tenant_field)} = $tenant AND t.{quote(tenant_field)} = $tenant\n"
        if tenant_field
        else ""
    )

    return (
        f"MATCH (s)-[r{rel}]->(t)\n"
        f"{where}"
        f"WITH gds.graph.project($graph_name, s, t, "
        f"{{relationshipProperties: {{weight: r.{quote(weight_property)}}}}}) AS g\n"
        f"RETURN g.graphName AS name"
    )


def gds_weighted_paths(
    *,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    edge_types: Iterable[str],
    weight_property: str,
    tenant_field: str | None = None,
) -> str:
    """Yen's k-shortest weighted paths on the projection, rebuilt as typed vertices/edges.

    GDS returns node ids and per-node cumulative ``costs``, and its own ``path`` uses *virtual*
    relationships — so the typed edges are rebuilt by matching, for each consecutive node pair, the
    real edge whose weight equals the per-hop cost GDS charged (``costs[i+1] - costs[i]``). Picking
    by that cost — not simply the cheapest edge — recovers the exact relationship Yen's ranked when
    parallel edges of different weights connect the same pair (a later, costlier path would
    otherwise be rebuilt from a different, cheaper relationship).

    ``max_hops`` bounds the *search*, not just the result, but Yen's has no native hop limit — so
    this yields **one row per candidate** in cost order, carrying ``hops`` (= ``size(nodeIds) - 1``)
    and rebuilding vertices/edges only for rows within ``$max_hops`` (empty lists otherwise, kept
    cheap). The caller filters to bounded rows and keeps the cheapest ``k``; because every candidate
    is reported, it can also tell whether Yen's was exhausted (fewer rows than ``$candidate_k``) and
    grow the window if a bounded path is still hiding behind cheaper over-long ones — so no valid
    bounded path is dropped by a fixed window size.
    """

    rel = _type_pattern(edge_types)

    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"CALL gds.shortestPath.yens.stream($graph_name, {{sourceNode: a, targetNode: b, "
        f"k: $candidate_k, relationshipWeightProperty: 'weight'}}) YIELD index, nodeIds, costs, totalCost\n"
        f"WITH index, nodeIds, costs, totalCost, size(nodeIds) - 1 AS hops\n"
        f"CALL {{ WITH nodeIds, hops\n"
        f"  UNWIND (CASE WHEN hops <= $max_hops THEN range(0, size(nodeIds) - 1) ELSE [] END) AS i\n"
        f"  WITH gds.util.asNode(nodeIds[i]) AS n, i ORDER BY i\n"
        f"  RETURN collect(properties(n)) AS vertices, collect(labels(n)) AS vertex_labels }}\n"
        f"CALL {{ WITH nodeIds, costs, hops\n"
        f"  UNWIND (CASE WHEN hops <= $max_hops THEN range(0, size(nodeIds) - 2) ELSE [] END) AS i\n"
        f"  CALL {{ WITH nodeIds, costs, i\n"
        f"    WITH gds.util.asNode(nodeIds[i]) AS u, gds.util.asNode(nodeIds[i + 1]) AS v, "
        f"costs[i + 1] - costs[i] AS hop_cost\n"
        f"    MATCH (u)-[r{rel}]->(v)\n"
        f"    RETURN r ORDER BY abs(coalesce(r.{quote(weight_property)}, 0.0) - hop_cost) ASC LIMIT 1 }}\n"
        f"  WITH i, r ORDER BY i\n"
        f"  RETURN collect(properties(r)) AS edges, collect(type(r)) AS edge_types }}\n"
        f"RETURN index, hops, vertices, vertex_labels, edges, edge_types, totalCost\n"
        f"ORDER BY totalCost, index"
    )


def gds_drop() -> str:
    """Drop the per-call projection (``false`` = do not error if already gone)."""

    return "CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName"


# ....................... #
# Read introspection (get/exists/count/degree/find)


def _tenant_pred(alias: str, tenant_field: str | None) -> str:
    return f"{alias}.{quote(tenant_field)} = $tenant" if tenant_field else ""


def _where(*preds: str) -> str:
    active = [p for p in preds if p]
    return f"WHERE {' AND '.join(active)}\n" if active else ""


_FILTER_KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_valid_filter_key(key: str) -> bool:
    """Whether *key* can be safely embedded in a ``$pf_<key>`` parameter name."""

    return _FILTER_KEY_RE.fullmatch(key) is not None


def property_predicate(alias: str, keys: Sequence[str]) -> str:
    """Equality ``AND`` predicate over *keys* (params ``$pf_<key>``); empty for no keys.

    Each key lands inside a ``$pf_<key>`` parameter *name*, which — unlike the property
    access, protected by :func:`quote` — cannot be backtick-quoted. Keys are therefore
    restricted to identifier characters; anything else fails closed here rather than
    becoming query text.
    """

    for key in keys:
        if not is_valid_filter_key(key):
            raise ValueError(f"Invalid property-filter key: {key!r}")

    return " AND ".join(f"{alias}.{quote(k)} = $pf_{k}" for k in keys)


def get_vertices_by_keys(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    where = _where(f"n.{quote(key_field)} IN $keys", _tenant_pred("n", tenant_field))
    return (
        f"MATCH (n:{quote(label)})\n{where}RETURN properties(n) AS n, n.{quote(key_field)} AS _key"
    )


def get_edges_by_keys(edge_type: str, key_field: str, *, tenant_field: str | None = None) -> str:
    where = _where(
        f"r.{quote(key_field)} IN $keys",
        _tenant_pred("a", tenant_field),
        _tenant_pred("b", tenant_field),
    )
    return (
        f"MATCH (a)-[r:{quote(edge_type)}]->(b)\n{where}"
        f"RETURN properties(r) AS r, r.{quote(key_field)} AS _key"
    )


def edge_exists_by_key(edge_type: str, key_field: str, *, tenant_field: str | None = None) -> str:
    where = _where(
        f"r.{quote(key_field)} = $key",
        _tenant_pred("a", tenant_field),
        _tenant_pred("b", tenant_field),
    )
    return f"MATCH (a)-[r:{quote(edge_type)}]->(b)\n{where}RETURN count(r) > 0 AS exists"


def edge_exists_by_endpoints(
    *,
    edge_type: str,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    tenant_field: str | None = None,
) -> str:
    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')})"
        f"-[r:{quote(edge_type)}]->"
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"RETURN count(r) > 0 AS exists"
    )


def get_edge_by_endpoints(
    *,
    edge_type: str,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    tenant_field: str | None = None,
) -> str:
    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')})"
        f"-[r:{quote(edge_type)}]->"
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"RETURN properties(r) AS r"
    )


def count_vertices(
    label: str, *, tenant_field: str | None = None, filter_keys: Sequence[str] = ()
) -> str:
    where = _where(_tenant_pred("n", tenant_field), property_predicate("n", filter_keys))
    return f"MATCH (n:{quote(label)})\n{where}RETURN count(n) AS c"


def count_edges(
    edge_type: str, *, tenant_field: str | None = None, filter_keys: Sequence[str] = ()
) -> str:
    where = _where(
        _tenant_pred("a", tenant_field),
        _tenant_pred("b", tenant_field),
        property_predicate("r", filter_keys),
    )
    return f"MATCH (a)-[r:{quote(edge_type)}]->(b)\n{where}RETURN count(r) AS c"


def vertex_degree(
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    *,
    tenant_field: str | None = None,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types))
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}()\n"
        f"RETURN count(r) AS c"
    )


def count_neighbors(
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    *,
    tenant_field: str | None = None,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types))
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}(m)\n"
        f"RETURN count(DISTINCT m) AS c"
    )


def incident_edges(
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    *,
    tenant_field: str | None = None,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types))
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}()\n"
        f"RETURN properties(r) AS r, type(r) AS t\nLIMIT $limit"
    )


def find_vertices(
    label: str,
    key_field: str,
    *,
    tenant_field: str | None = None,
    filter_keys: Sequence[str] = (),
) -> str:
    where = _where(_tenant_pred("n", tenant_field), property_predicate("n", filter_keys))
    return (
        f"MATCH (n:{quote(label)})\n{where}"
        f"RETURN properties(n) AS n\n"
        f"ORDER BY n.{quote(key_field)}\nSKIP $offset LIMIT $limit"
    )


def find_edges(
    edge_type: str,
    *,
    order_field: str | None = None,
    tenant_field: str | None = None,
    filter_keys: Sequence[str] = (),
) -> str:
    where = _where(
        _tenant_pred("a", tenant_field),
        _tenant_pred("b", tenant_field),
        property_predicate("r", filter_keys),
    )
    order = f"ORDER BY r.{quote(order_field)}\n" if order_field else ""
    return (
        f"MATCH (a)-[r:{quote(edge_type)}]->(b)\n{where}"
        f"RETURN properties(r) AS r\n{order}SKIP $offset LIMIT $limit"
    )


# ....................... #
# Writes (update / delete / ensure / bulk)


def update_edge_by_key(edge_type: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH ()-[r:{quote(edge_type)} {_match_map(key_field, tenant_field)}]->()\n"
        f"SET r += $props\nRETURN properties(r) AS r"
    )


def update_edge_by_endpoints(
    *,
    edge_type: str,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    tenant_field: str | None = None,
) -> str:
    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')})"
        f"-[r:{quote(edge_type)}]->"
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"SET r += $props\nRETURN properties(r) AS r"
    )


def delete_edge_by_key(edge_type: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return f"MATCH ()-[r:{quote(edge_type)} {_match_map(key_field, tenant_field)}]->()\nDELETE r"


def delete_edge_by_endpoints(
    *,
    edge_type: str,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    tenant_field: str | None = None,
) -> str:
    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')})"
        f"-[r:{quote(edge_type)}]->"
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"DELETE r"
    )


def ensure_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    """MERGE on the key (+ tenant); populate only on create, so an existing vertex is
    returned unchanged (create-if-missing semantics)."""

    return (
        f"MERGE (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"ON CREATE SET n = $props\nRETURN properties(n) AS n"
    )


def create_vertices(label: str) -> str:
    return (
        f"UNWIND $rows AS props\nCREATE (n:{quote(label)})\nSET n = props\n"
        f"RETURN properties(n) AS n"
    )


def delete_vertices(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    where = _where(f"n.{quote(key_field)} IN $keys", _tenant_pred("n", tenant_field))
    return f"MATCH (n:{quote(label)})\n{where}DETACH DELETE n"


def delete_edges_by_keys(edge_type: str, key_field: str, *, tenant_field: str | None = None) -> str:
    where = _where(
        f"r.{quote(key_field)} IN $keys",
        _tenant_pred("a", tenant_field),
        _tenant_pred("b", tenant_field),
    )
    return f"MATCH (a)-[r:{quote(edge_type)}]->(b)\n{where}DELETE r"
