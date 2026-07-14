"""The keyset walk behind ``find_vertices_stream`` / ``find_edges_stream``, and its guards.

Both graph backends drive the same loop over the same three refusals, so both live here rather
than being written twice and drifting once. The refusals are the interesting half: each one is
a case where a stream *could* be served and the rows it returned would be a lie.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.graph import (
    GraphEdgeSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
    GraphStreamingAware,
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

    if (reason := vertex_stream_blocker(node)) is not None:
        raise exc.precondition(
            f"Node kind {kind!r} cannot be streamed: {reason}.",
            code=_UNSUPPORTED_CODE,
        )

    return node.key_field


# ....................... #


def assert_edge_streamable(
    edge: GraphEdgeSpec[BaseModel],
    *,
    kind: str,
    capabilities: GraphReadCapabilities,
) -> str:
    """Refuse unless *edge* can be keyset-walked; return the field to bookmark on."""

    if not capabilities.supports_edge_streaming:
        raise exc.precondition(
            f"The wired graph backend does not support edge streaming, so edge kind {kind!r} "
            f"cannot be walked to exhaustion.",
            code=_UNSUPPORTED_CODE,
        )

    if (reason := edge_stream_blocker(edge)) is not None:
        raise exc.precondition(
            f"Edge kind {kind!r} cannot be streamed: {reason}.",
            code=_UNSUPPORTED_CODE,
        )

    key_field = edge.key_field

    if key_field is None:  # pragma: no cover — a keyless edge is already refused above
        raise exc.internal(
            f"Edge kind {kind!r} passed the streamability check with no key field.",
            code=_UNSUPPORTED_CODE,
        )

    return key_field


# ....................... #


async def stream_keyset_pages[R](
    fetch: Callable[[Any | None, int], Awaitable[Sequence[tuple[Any, R]]]],
    *,
    chunk_size: int,
) -> AsyncGenerator[Sequence[R]]:
    """Drive a keyset cursor to exhaustion, yielding one batch of models per page.

    *fetch* takes ``(after, limit)`` and returns ``(stored_key, model)`` pairs already ordered
    by key — the **stored** key, not the model's, because on an encrypted kind those differ and
    the cursor has to speak the store's language. The bookmark never leaves this generator, so
    no caller can resume from a stale one or hand one to a user.
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

        last_key = page[-1][0]

        # Forward progress, asserted rather than assumed: a backend whose seek predicate is
        # wrong (``>=`` instead of ``>``, a key it failed to order by) hands back a page that
        # starts where the last one did, and the walk spins on it forever, yielding the same
        # rows. Loud beats endless.
        if after is not None and last_key == after:
            raise exc.internal(
                f"Graph keyset stream made no progress: the page after key {after!r} ends on "
                f"the same key, so the backend is not seeking past the cursor.",
                code="graph_streaming_no_progress",
            )

        yield [model for _key, model in page]

        # A short page means the keyset is exhausted — unlike an offset window, a keyset seek
        # returns fewer rows than asked for only when there are no more.
        if len(page) < chunk_size:
            return

        after = last_key
