"""Which kinds in a graph spec can be keyset-walked — a property of the *spec*, not the backend.

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
"""

from typing import Any

from .specs import GraphEdgeSpec, GraphModuleSpec, GraphNodeSpec

# ----------------------- #


def _sealed_key(encryption: Any, key_field: str) -> bool:
    if encryption is None:
        return False

    return key_field in (encryption.encrypted | encryption.searchable)


_SEALED_KEY_REASON = (
    "its key field {key!r} is field-encrypted, and a sealed value has no usable order to seek "
    "on: randomized ciphertext has no order at all, and deterministic ciphertext has one that "
    "is not the plaintext's — so a cursor bookmarked on the decrypted model would be compared "
    "against ciphertext in the store and skip rows without failing. Take the key field out of "
    "the encryption policy (encrypting a non-key property is fine)"
)

_ENDPOINTS_REASON = (
    "it is declared identity='endpoints', so its edges have no key of their own — there is "
    "nothing for a cursor to bookmark and no way to resume a walk. Give the kind a key "
    "(identity='key' + key_field=…) if its edges have a business identity; an endpoint-pair "
    "cursor for the ones that genuinely do not is a designed but unbuilt follow-up"
)


# ....................... #


def vertex_stream_blocker(node: GraphNodeSpec[Any]) -> str | None:
    """Why *node* cannot be keyset-walked, or ``None`` when it can."""

    if _sealed_key(node.encryption, node.key_field):
        return _SEALED_KEY_REASON.format(key=node.key_field)

    return None


# ....................... #


def edge_stream_blocker(edge: GraphEdgeSpec[Any]) -> str | None:
    """Why *edge* cannot be keyset-walked, or ``None`` when it can."""

    if edge.identity != "key" or edge.key_field is None:
        return _ENDPOINTS_REASON

    if _sealed_key(edge.encryption, edge.key_field):
        return _SEALED_KEY_REASON.format(key=edge.key_field)

    return None


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
        if (reason := edge_stream_blocker(edge)) is not None:
            blockers.append((str(edge.name), reason))

    return tuple(blockers)
