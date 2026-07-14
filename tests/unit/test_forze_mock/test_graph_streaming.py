"""`find_vertices_stream` / `find_edges_stream` — the keyset walk and the three refusals.

Streaming exists because the offset-paged `find_*` cannot promise *completeness*: `SKIP n`
counts rows from the start of a result set that is being written underneath it, so a vertex
created before the cursor shifts every later row one place along and the next page skips one.
For an export, a skipped page and an empty page produce the same artifact — which is why the
walk seeks by key instead, and why the refusals below fail closed rather than serving a scan
that looks complete and is not.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
    GraphStreamingAware,
)
from forze.application.integrations.graph import (
    assert_edge_streamable,
    assert_vertex_streamable,
    graph_read_capabilities,
    stream_keyset_pages,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockState
from forze_mock.adapters.graph import MockGraphAdapter

# ----------------------- #


class UserRead(BaseModel):
    id: str
    name: str | None = None


class UserCreate(BaseModel):
    id: str
    name: str | None = None


class KnowsRead(BaseModel):
    id: str
    weight: int | None = None


class KnowsCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    weight: int | None = None


class FollowsRead(BaseModel):
    weight: int | None = None


class FollowsCreate(BaseModel):
    from_key: str
    to_key: str
    weight: int | None = None


def _spec() -> GraphModuleSpec:
    return GraphModuleSpec(
        name="social",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserCreate),),
        edges=(
            # A keyed edge — streamable, because it has somewhere to bookmark.
            GraphEdgeSpec(
                name="KNOWS",
                read=KnowsRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            # An endpoint-identified edge — no per-edge key, so nothing to resume from.
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _adapter(spec: GraphModuleSpec | None = None) -> MockGraphAdapter:
    return MockGraphAdapter(spec=spec or _spec(), state=MockState(), namespace="social")


async def _collect(stream) -> list:
    return [row async for batch in stream for row in batch]


async def _batches(stream) -> list[int]:
    return [len(batch) async for batch in stream]


# ....................... #


class TestVertexStreaming:
    async def test_walks_every_vertex_in_key_order_across_pages(self) -> None:
        graph = _adapter()

        for i in range(25):
            await graph.create_vertex("User", UserCreate(id=f"u{i:02d}", name=f"n{i}"))

        rows = await _collect(graph.find_vertices_stream("User", chunk_size=10))

        assert [row.id for row in rows] == [f"u{i:02d}" for i in range(25)]

    async def test_pages_are_bounded_by_chunk_size(self) -> None:
        graph = _adapter()

        for i in range(25):
            await graph.create_vertex("User", UserCreate(id=f"u{i:02d}"))

        # 10 + 10 + 5, and the short final page ends the walk — no extra empty round-trip.
        assert await _batches(graph.find_vertices_stream("User", chunk_size=10)) == [
            10,
            10,
            5,
        ]

    async def test_an_empty_kind_yields_nothing(self) -> None:
        graph = _adapter()

        assert await _collect(graph.find_vertices_stream("User")) == []

    async def test_a_full_page_is_followed_by_a_probe_that_ends_it(self) -> None:
        # Exactly ``chunk_size`` rows: the walk cannot know it is done, so it asks once more
        # and gets nothing. Ending on the full page instead would be wrong the moment there
        # was one more row.
        graph = _adapter()

        for i in range(4):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        assert await _batches(graph.find_vertices_stream("User", chunk_size=4)) == [4]

    async def test_a_property_filter_scopes_the_walk(self) -> None:
        graph = _adapter()

        for i in range(6):
            await graph.create_vertex(
                "User", UserCreate(id=f"u{i}", name="keep" if i % 2 else "drop")
            )

        rows = await _collect(
            graph.find_vertices_stream("User", property_filter={"name": "keep"}, chunk_size=2)
        )

        assert sorted(row.id for row in rows) == ["u1", "u3", "u5"]

    async def test_a_vertex_added_behind_the_cursor_does_not_shift_the_walk(self) -> None:
        # The whole reason keyset exists. Under ``SKIP``/``LIMIT``, inserting a row *before*
        # the cursor pushes every later row one place along and the next page silently skips
        # one. A key bookmark does not move.
        graph = _adapter()

        for i in range(6):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        seen: list[str] = []
        stream = graph.find_vertices_stream("User", chunk_size=2)

        async for batch in stream:
            seen.extend(row.id for row in batch)

            if len(seen) == 2:
                # Sorts *before* everything still to come, i.e. behind the cursor.
                await graph.create_vertex("User", UserCreate(id="u0a"))

        # No row is skipped: every id after the bookmark is still visited.
        assert seen == ["u0", "u1", "u2", "u3", "u4", "u5"]


# ....................... #


class TestEdgeStreaming:
    async def test_walks_every_keyed_edge(self) -> None:
        graph = _adapter()

        for i in range(5):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        for i in range(4):
            await graph.create_edge(
                "KNOWS",
                KnowsCreate(id=f"e{i}", from_key=f"u{i}", to_key=f"u{i + 1}", weight=i),
            )

        rows = await _collect(graph.find_edges_stream("KNOWS", chunk_size=2))

        assert [row.id for row in rows] == ["e0", "e1", "e2", "e3"]

    async def test_an_endpoint_identified_edge_is_refused_by_name(self) -> None:
        # There is no per-edge key, so there is nothing to bookmark and no way to resume. The
        # spec already refuses ``binds_record_id`` on these edges for the same reason.
        graph = _adapter()

        with pytest.raises(CoreException) as exc_info:
            await _collect(graph.find_edges_stream("FOLLOWS"))

        assert exc_info.value.kind is ExceptionKind.PRECONDITION
        assert exc_info.value.code == "graph_streaming_unsupported"
        assert "FOLLOWS" in str(exc_info.value)


# ....................... #


class TestRefusals:
    """Each guard covers a case where a stream could be served and would be lying."""

    def test_a_backend_that_cannot_report_supports_nothing(self) -> None:
        class _Mute:
            """A graph port with no ``read_capabilities`` at all."""

        capabilities = graph_read_capabilities(_Mute())

        assert capabilities == GraphReadCapabilities()
        assert not capabilities.supports_vertex_streaming
        assert not capabilities.supports_edge_streaming

    def test_the_mock_reports_both_streams(self) -> None:
        graph = _adapter()

        assert isinstance(graph, GraphStreamingAware)
        assert graph_read_capabilities(graph) == GraphReadCapabilities(
            supports_vertex_streaming=True,
            supports_edge_streaming=True,
        )

    def test_an_incapable_backend_refuses_rather_than_partially_scanning(self) -> None:
        node = GraphNodeSpec(name="User", read=UserRead, create=UserCreate)

        with pytest.raises(CoreException, match="does not support vertex streaming"):
            assert_vertex_streamable(node, kind="User", capabilities=GraphReadCapabilities())

    def test_an_encrypted_key_field_is_refused(self) -> None:
        # A keyset cursor is an order over the *stored* values, and a sealed one has none it
        # can use: randomized ciphertext has no order, and deterministic ciphertext has an
        # order that is not the plaintext's — so a bookmark taken from the decrypted model
        # would seek to the wrong place and skip rows without ever failing.
        node = GraphNodeSpec(
            name="User",
            read=UserRead,
            create=UserCreate,
            encryption=FieldEncryption(encrypted=frozenset({"id"})),
        )

        with pytest.raises(CoreException) as exc_info:
            assert_vertex_streamable(
                node,
                kind="User",
                capabilities=GraphReadCapabilities(supports_vertex_streaming=True),
            )

        assert exc_info.value.code == "graph_streaming_unsupported"
        assert "no usable order" in str(exc_info.value)

    def test_a_deterministic_key_field_is_refused_too(self) -> None:
        # Searchable (deterministic) encryption gives a *stable* ciphertext order, which is
        # the tempting case — and still wrong, because that order is not the plaintext's.
        node = GraphNodeSpec(
            name="User",
            read=UserRead,
            create=UserCreate,
            encryption=FieldEncryption(searchable=frozenset({"id"})),
        )

        with pytest.raises(CoreException, match="no usable order"):
            assert_vertex_streamable(
                node,
                kind="User",
                capabilities=GraphReadCapabilities(supports_vertex_streaming=True),
            )

    def test_a_plaintext_key_beside_an_encrypted_property_still_streams(self) -> None:
        node = GraphNodeSpec(
            name="User",
            read=UserRead,
            create=UserCreate,
            encryption=FieldEncryption(encrypted=frozenset({"name"})),
        )

        key_field = assert_vertex_streamable(
            node,
            kind="User",
            capabilities=GraphReadCapabilities(supports_vertex_streaming=True),
        )

        assert key_field == "id"

    def test_an_endpoint_edge_is_refused_at_the_guard(self) -> None:
        edge = GraphEdgeSpec(
            name="FOLLOWS",
            read=FollowsRead,
            identity="endpoints",
            endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
        )

        with pytest.raises(CoreException, match="no key of their own"):
            assert_edge_streamable(
                edge,
                kind="FOLLOWS",
                capabilities=GraphReadCapabilities(supports_edge_streaming=True),
            )


# ....................... #


class TestKeysetLoop:
    """The shared loop's own invariants, driven directly."""

    async def test_a_backend_that_does_not_advance_raises_instead_of_spinning(
        self,
    ) -> None:
        # A wrong seek predicate (``>=`` instead of ``>``, or an unordered result) hands back
        # a page ending on the key the cursor is already at, and the walk yields the same rows
        # forever. Loud beats endless.
        async def _stuck(after, limit):  # type: ignore[no-untyped-def]
            return [("k1", "row")] * limit

        with pytest.raises(CoreException) as exc_info:
            await _collect(stream_keyset_pages(_stuck, chunk_size=2))

        assert exc_info.value.code == "graph_streaming_no_progress"

    async def test_a_chunk_size_below_one_is_refused(self) -> None:
        async def _never(after, limit):  # type: ignore[no-untyped-def]
            raise AssertionError("must not be called")

        with pytest.raises(CoreException, match="at least 1"):
            await _collect(stream_keyset_pages(_never, chunk_size=0))
