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

from collections.abc import Iterable, Sequence

from forze.application.contracts.graph import GraphDirection

# ----------------------- #


def quote(name: str) -> str:
    """Backtick-quote a label / relationship type, escaping embedded backticks."""

    return "`" + name.replace("`", "``") + "`"


# ....................... #


def _match_map(
    key_field: str, tenant_field: str | None, *, key_param: str = "key"
) -> str:
    if tenant_field:
        return f"{{{quote(key_field)}: ${key_param}, {quote(tenant_field)}: $tenant}}"

    return f"{{{quote(key_field)}: ${key_param}}}"


# ....................... #


def _tenant_only_map(tenant_field: str | None, *, interior: bool) -> str:
    """Inline ``{<tenant_field>: $tenant}`` for an adjacent (keyless) traversal node."""

    if tenant_field and interior:
        return f" {{{quote(tenant_field)}: $tenant}}"

    return ""


def _path_tenant_pred(
    tenant_field: str | None, *, interior: bool, path_var: str = "path"
) -> str:
    """``WHERE``-clause constraining every node on *path_var* to ``$tenant``.

    Used for variable-length and shortest-path matches where interior nodes cannot be
    pinned with an inline property map.
    """

    if not (tenant_field and interior):
        return ""

    return (
        f"WHERE all(_n IN nodes({path_var}) WHERE _n.{quote(tenant_field)} = $tenant)\n"
    )


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
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"RETURN properties(n) AS n"
    )


# ....................... #


def vertex_exists(
    label: str, key_field: str, *, tenant_field: str | None = None
) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"RETURN count(n) > 0 AS exists"
    )


# ....................... #


def create_vertex(label: str) -> str:
    return f"CREATE (n:{quote(label)})\nSET n = $props\nRETURN properties(n) AS n"


# ....................... #


def update_vertex(
    label: str, key_field: str, *, tenant_field: str | None = None
) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"SET n += $props\nRETURN properties(n) AS n"
    )


# ....................... #


def delete_vertex(
    label: str, key_field: str, *, tenant_field: str | None = None
) -> str:
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
    the ``MERGE`` matches any edge of the type and collapses them.
    """

    head = (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
    )

    if merge:
        rel_key = f" {{{quote(key_field)}: ${key_param}}}" if key_field else ""
        body = (
            f"MERGE (a)-[r:{quote(edge_type)}{rel_key}]->(b)\nON CREATE SET r += $props\n"
        )
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
    rel = _rel(direction, _type_pattern(edge_types), quant=f"*1..{max_depth}")

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
