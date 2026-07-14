"""The keyset walk behind ``find_vertices_stream`` / ``find_edges_stream``, and its guards.

Both graph backends drive the same loop, so it lives here rather than being written twice and
drifting once. The cursor is a **tuple of stored property values** — one element for a vertex
(its key) or a keyed edge (its key), two for an endpoint-identified edge (the tail's and head's
node keys) — which is what lets a kind with no key of its own be walked at all.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
    GraphStreamingAware,
    edge_cursor_fields,
    edge_stream_blocker,
    vertex_stream_blocker,
)
from forze.base.exceptions import exc

# ----------------------- #

_UNSUPPORTED_CODE = "graph_streaming_unsupported"

# ....................... #


def graph_read_capabilities(port: object) -> GraphReadCapabilities:
    """What *port* says it can do — nothing, if it cannot say.

    A backend that does not implement :class:`GraphStreamingAware` supports neither stream, so
    the call is refused instead of falling through to something that resembles it.
    """

    if isinstance(port, GraphStreamingAware):
        return port.read_capabilities()

    return GraphReadCapabilities()


# ....................... #


def assert_vertex_streamable(
    node: GraphNodeSpec[BaseModel],
    *,
    kind: str,
    capabilities: GraphReadCapabilities,
) -> str:
    """Refuse unless *node* can be keyset-walked; return the field to bookmark on.

    Two independent questions: can the *backend* seek (a port capability), and is the *kind*
    shaped so that a cursor has anything to bookmark (a spec property, shared with the export
    inventory — see :mod:`forze.application.contracts.graph.streamable`).
    """

    if not capabilities.supports_vertex_streaming:
        raise exc.precondition(
            f"The wired graph backend does not support vertex streaming, so node kind "
            f"{kind!r} cannot be walked to exhaustion. A partial scan is not offered in its "
            f"place: it would be indistinguishable from a complete one.",
            code=_UNSUPPORTED_CODE,
        )

    if (reason := vertex_stream_blocker(node)) is not None:  # pragma: no cover
        raise exc.precondition(
            f"Node kind {kind!r} cannot be streamed: {reason}.",
            code=_UNSUPPORTED_CODE,
        )

    return node.key_field


# ....................... #


def assert_edge_streamable(
    spec: GraphModuleSpec,
    edge: GraphEdgeSpec[BaseModel],
    *,
    kind: str,
    capabilities: GraphReadCapabilities,
) -> tuple[str, ...]:
    """Refuse unless *edge* can be keyset-walked; return the properties to bookmark on.

    One field for a keyed edge, two for an endpoint-identified one — see
    :func:`~forze.application.contracts.graph.edge_cursor_fields`.
    """

    if not capabilities.supports_edge_streaming:
        raise exc.precondition(
            f"The wired graph backend does not support edge streaming, so edge kind {kind!r} "
            f"cannot be walked to exhaustion.",
            code=_UNSUPPORTED_CODE,
        )

    if (reason := edge_stream_blocker(spec, edge)) is not None:
        raise exc.precondition(
            f"Edge kind {kind!r} cannot be streamed: {reason}.",
            code=_UNSUPPORTED_CODE,
        )

    fields = edge_cursor_fields(spec, edge)

    if fields is None:  # pragma: no cover — refused above
        raise exc.internal(
            f"Edge kind {kind!r} passed the streamability check with no cursor.",
            code=_UNSUPPORTED_CODE,
        )

    return fields


# ....................... #


async def stream_keyset_pages[R](
    fetch: Callable[[Any | None, int], Awaitable[Sequence[tuple[Any, R]]]],
    *,
    chunk_size: int,
) -> AsyncGenerator[Sequence[R]]:
    """Drive a keyset cursor to exhaustion, yielding one batch of models per page.

    *fetch* takes ``(after, limit)`` and returns ``(cursor_key, model)`` pairs already ordered by
    key. The key is the value **as the store holds it**, never the model's — the cursor has to
    speak the backend's language, and the bookmark never leaves this generator, so no caller can
    resume from a stale one or hand one to a user.

    **A key may repeat.** For an endpoint-identified edge kind the cursor is the ``(tail, head)``
    pair, and the framework does not (yet) *enforce* the one-edge-per-pair identity that
    declaration asserts — ``create_edge`` will happily add a second parallel edge. So *limit* is
    a bound on **distinct keys**, not on rows, and a backend must return **every** row of the
    last key it includes. Anything else would silently drop the duplicates: the next page seeks
    strictly past that key, and whatever the page boundary cut off is never revisited. The walk
    would look complete and would not be — the failure the whole streaming contract exists to
    prevent.
    """

    if chunk_size < 1:
        raise exc.precondition(
            f"chunk_size must be at least 1, got {chunk_size}.",
            code="graph_streaming_invalid_chunk",
        )

    after: Any | None = None

    while True:
        page = await fetch(after, chunk_size)

        if not page:
            return

        keys = [key for key, _ in page]
        last_key = keys[-1]

        # Forward progress, asserted rather than assumed: a backend whose seek predicate is
        # wrong (``>=`` instead of ``>``, a key it failed to order by) hands back a page that
        # ends where the last one did, and the walk spins on it forever, yielding the same rows.
        # Loud beats endless.
        if after is not None and last_key == after:
            raise exc.internal(
                f"Graph keyset stream made no progress: the page after key {after!r} ends on "
                f"the same key, so the backend is not seeking past the cursor.",
                code="graph_streaming_no_progress",
            )

        yield [model for _key, model in page]

        # Exhaustion is measured in **distinct keys**, matching what ``chunk_size`` bounds: a key
        # may carry several rows, so a full page can be longer than the window. (Counting rows
        # would still terminate — a page short on rows is necessarily short on keys — but it
        # would ask for one more page than it needed whenever duplicates padded the last one.)
        if len(set(keys)) < chunk_size:
            return

        after = last_key
