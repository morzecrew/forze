"""Pure openCypher string builders (no driver/engine state).

Kept free of Neo4j-driver types so a future openCypher sibling (Memgraph, Neptune,
AGE) can reuse them. Queries return property maps (``properties(x)``) so result rows
materialize as plain dicts. Variable-length bounds are inlined (Cypher disallows a
parameter for the ``*min..max`` quantifier); callers must pass validated integers.

When ``tenant_field`` is supplied, anchor nodes (the matched vertex, edge, or path
endpoints) are additionally constrained by ``{<tenant_field>: $tenant}`` for
tenant-property isolation; the parameter ``$tenant`` must then be provided.
"""

from collections.abc import Iterable

from forze.application.contracts.graph import GraphDirection

# ----------------------- #


def quote(name: str) -> str:
    """Backtick-quote a label / relationship type, escaping embedded backticks."""

    return "`" + name.replace("`", "``") + "`"


# ....................... #


def _match_map(key_field: str, tenant_field: str | None, *, key_param: str = "key") -> str:
    if tenant_field:
        return f"{{{key_field}: ${key_param}, {tenant_field}: $tenant}}"

    return f"{{{key_field}: ${key_param}}}"


# ....................... #


def _type_pattern(types: Iterable[str]) -> str:
    kinds = list(types)

    if not kinds:
        return ""

    return ":" + "|".join(quote(t) for t in kinds)


# ....................... #


def _rel(direction: GraphDirection, type_pattern: str, *, quant: str = "") -> str:
    body = f"[r{type_pattern}{quant}]"

    if direction is GraphDirection.OUT:
        return f"-{body}->"

    if direction is GraphDirection.IN:
        return f"<-{body}-"

    return f"-{body}-"


# ----------------------- #
# Vertices


def get_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"RETURN properties(n) AS n"
    )


def vertex_exists(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"RETURN count(n) > 0 AS exists"
    )


def create_vertex(label: str) -> str:
    return f"CREATE (n:{quote(label)})\nSET n = $props\nRETURN properties(n) AS n"


def update_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\n"
        f"SET n += $props\nRETURN properties(n) AS n"
    )


def delete_vertex(label: str, key_field: str, *, tenant_field: str | None = None) -> str:
    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)})\nDETACH DELETE n"
    )


# ----------------------- #
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


def create_edge(
    *,
    from_label: str,
    from_key_field: str,
    to_label: str,
    to_key_field: str,
    edge_type: str,
    merge: bool,
    tenant_field: str | None = None,
) -> str:
    """Create (or, when ``merge``, idempotently ensure) a directed edge a→b."""

    head = (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
    )

    if merge:
        body = f"MERGE (a)-[r:{quote(edge_type)}]->(b)\nON CREATE SET r += $props\n"
    else:
        body = f"CREATE (a)-[r:{quote(edge_type)}]->(b)\nSET r += $props\n"

    return head + body + "RETURN properties(r) AS r"


# ----------------------- #
# Traversal


def neighbors(
    *,
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    tenant_field: str | None = None,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types))

    return (
        f"MATCH (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}(m)\n"
        f"RETURN properties(m) AS other, labels(m) AS other_labels, "
        f"properties(r) AS via_edge, type(r) AS via_type\nLIMIT $limit"
    )


def expand(
    *,
    label: str,
    key_field: str,
    direction: GraphDirection,
    edge_types: Iterable[str],
    max_depth: int,
    tenant_field: str | None = None,
) -> str:
    rel = _rel(direction, _type_pattern(edge_types), quant=f"*1..{int(max_depth)}")

    return (
        f"MATCH path = (n:{quote(label)} {_match_map(key_field, tenant_field)}){rel}(m)\n"
        f"RETURN length(path) AS depth, "
        f"properties(m) AS vertex, labels(m) AS vertex_labels, "
        f"properties(last(relationships(path))) AS from_parent, "
        f"type(last(relationships(path))) AS from_parent_type, "
        f"properties(nodes(path)[-2]) AS parent, labels(nodes(path)[-2]) AS parent_labels\n"
        f"ORDER BY depth\nLIMIT $max_results"
    )


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
) -> str:
    rel = _rel(direction, _type_pattern(edge_types), quant=f"*..{int(max_hops)}")

    return (
        f"MATCH (a:{quote(from_label)} {_match_map(from_key_field, tenant_field, key_param='from_key')}), "
        f"(b:{quote(to_label)} {_match_map(to_key_field, tenant_field, key_param='to_key')})\n"
        f"MATCH path = shortestPath((a){rel}(b))\n"
        f"RETURN [n IN nodes(path) | properties(n)] AS vertices, "
        f"[n IN nodes(path) | labels(n)] AS vertex_labels, "
        f"[e IN relationships(path) | properties(e)] AS edges, "
        f"[e IN relationships(path) | type(e)] AS edge_types"
    )
