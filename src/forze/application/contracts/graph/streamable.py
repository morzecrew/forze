"""Which kinds in a graph spec can be keyset-walked, and what a cursor bookmarks on.

Two things have to agree on this and would otherwise each answer it themselves: the streaming
read, which refuses a kind it cannot walk, and the export inventory, which has to know *before
it writes a row* whether the graph plane can be carried at all. Those two answers drifting
apart is the failure worth designing against — an inventory that says "exportable" over a graph
whose stream will refuse mid-flight leaves a half-written artifact, and a half-written artifact
is exactly the "looks complete and is not" outcome the doctrine forbids. So the rule lives here,
once, in contracts, where both layers can reach it.

Backend *capability* is a separate question, asked of the port
(:class:`~forze.application.contracts.graph.GraphStreamingAware`); this module only asks whether
the kind is shaped so that a cursor could bookmark it at all.

A **sealed key field** used to be a refusal here. It no longer needs to be: a key that is
field-encrypted cannot be matched either, so a vertex written under one could never be fetched
back — and :func:`~forze.application.contracts.graph.assert_key_field_not_sealed` refuses it at
spec construction, where the damage is nil rather than silent.
"""

from typing import Any

from .specs import GraphEdgeSpec, GraphModuleSpec, GraphNodeSpec

# ----------------------- #

_MIXED_ENDPOINT_KEYS_REASON = (
    "it is an identity='endpoints' kind, so a cursor bookmarks on its endpoint pair — but its "
    "endpoint kinds do not agree on a key field ({fields}), so there is no single property pair "
    "to order by. Give the kind a key of its own (identity='key' + key_field=…), or key the "
    "endpoint node kinds consistently"
)


# ....................... #


def vertex_stream_blocker(node: GraphNodeSpec[Any]) -> str | None:
    """Why *node* cannot be keyset-walked, or ``None`` when it can.

    Nothing blocks a vertex kind: its key field is its cursor, and a key that could not serve as
    one — a sealed one — cannot be declared at all. Kept as the seam the inventory asks, so a
    future kind shape that genuinely *is* unwalkable has an obvious home.
    """

    return None


# ....................... #


def endpoint_key_fields(
    spec: GraphModuleSpec,
    edge: GraphEdgeSpec[Any],
) -> tuple[str, str] | None:
    """The ``(tail_key_field, head_key_field)`` an endpoint-pair cursor bookmarks on.

    ``None`` when the edge kind's endpoint pairs disagree — a multi-endpoint kind may link
    ``Post → Tag`` *and* ``Note → Tag``, and if ``Post`` and ``Note`` key on different properties
    there is no single ``ORDER BY`` covering both. They almost always agree: ``key_field``
    defaults to ``id`` on every node kind.
    """

    tails: set[str] = set()
    heads: set[str] = set()

    for endpoint in edge.endpoints:
        tail = spec.graph_node_by_kind(endpoint.from_kind)
        head = spec.graph_node_by_kind(endpoint.to_kind)

        if tail is None or head is None:  # pragma: no cover — an unresolvable endpoint kind
            return None

        tails.add(tail.key_field)
        heads.add(head.key_field)

    if len(tails) != 1 or len(heads) != 1:
        return None

    return tails.pop(), heads.pop()


# ....................... #


def edge_cursor_fields(
    spec: GraphModuleSpec,
    edge: GraphEdgeSpec[Any],
) -> tuple[str, ...] | None:
    """The properties an edge kind's keyset cursor orders and seeks on, in order.

    **One** field for an ``identity="key"`` edge — its own key. **Two** for an
    ``identity="endpoints"`` edge: the tail's and the head's node keys, which together *are* the
    identity that declaration asserts. ``None`` when the kind cannot be walked at all.

    Returning one shape for both is what lets the adapters and the inventory treat "keyed" and
    "endpoint-identified" as the same problem with a wider cursor, rather than two code paths
    that drift.
    """

    if edge.identity == "key" and edge.key_field is not None:
        return (edge.key_field,)

    return endpoint_key_fields(spec, edge)


# ....................... #


def edge_stream_blocker(spec: GraphModuleSpec, edge: GraphEdgeSpec[Any]) -> str | None:
    """Why *edge* cannot be keyset-walked, or ``None`` when it can."""

    if edge_cursor_fields(spec, edge) is not None:
        return None

    fields = sorted(
        {
            node.key_field
            for endpoint in edge.endpoints
            for kind in (endpoint.from_kind, endpoint.to_kind)
            if (node := spec.graph_node_by_kind(kind)) is not None
        }
    )

    return _MIXED_ENDPOINT_KEYS_REASON.format(fields=fields)


# ....................... #


def graph_stream_blockers(spec: GraphModuleSpec) -> tuple[tuple[str, str], ...]:
    """Every ``(kind, reason)`` in *spec* that cannot be keyset-walked.

    Empty means the whole module can be streamed — every vertex kind and every edge kind — and
    therefore that the graph plane can be carried in full. A **non-empty** result means it
    cannot: one unwalkable edge kind is enough, because an export that carried the other kinds
    and quietly left that one out would produce an artifact that reads as a complete graph and
    is not one.
    """

    blockers: list[tuple[str, str]] = []

    for node in spec.nodes:
        if (reason := vertex_stream_blocker(node)) is not None:
            blockers.append((str(node.name), reason))

    for edge in spec.edges:
        if (reason := edge_stream_blocker(spec, edge)) is not None:
            blockers.append((str(edge.name), reason))

    return tuple(blockers)
