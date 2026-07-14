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

    async def test_an_endpoint_identified_edge_walks_on_its_endpoint_pair(self) -> None:
        # Such an edge has no key of its own — that is the declaration — so the cursor is the
        # (tail, head) pair, which *is* the identity the author asserted.
        graph = _adapter()

        for i in range(5):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        for i in range(4):
            await graph.create_edge(
                "FOLLOWS",
                FollowsCreate(from_key=f"u{i}", to_key=f"u{i + 1}", weight=i),
            )

        rows = await _collect(graph.find_edges_stream("FOLLOWS", chunk_size=2))

        assert sorted(row.weight for row in rows) == [0, 1, 2, 3]

    async def test_duplicate_edges_on_one_pair_are_all_walked(self) -> None:
        # The hazard the pair cursor is designed around. Nothing enforces the one-edge-per-pair
        # identity — ``create_edge`` will add a second parallel edge — so a page cut *within* a
        # pair would leave edges behind a cursor that seeks strictly past it. ``chunk_size=1``
        # puts a boundary on every pair, including the duplicated one.
        graph = _adapter()

        for i in range(3):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        await graph.create_edge("FOLLOWS", FollowsCreate(from_key="u0", to_key="u1", weight=1))
        await graph.create_edge("FOLLOWS", FollowsCreate(from_key="u0", to_key="u1", weight=99))
        await graph.create_edge("FOLLOWS", FollowsCreate(from_key="u1", to_key="u2", weight=2))

        rows = await _collect(graph.find_edges_stream("FOLLOWS", chunk_size=1))

        assert sorted(row.weight for row in rows) == [1, 2, 99]

    async def test_chunk_size_bounds_pairs_not_edges(self) -> None:
        # Every edge of an admitted pair travels with it, so a page can be longer than the
        # window — that is the whole point, and it is why exhaustion counts keys, not rows.
        graph = _adapter()

        for i in range(3):
            await graph.create_vertex("User", UserCreate(id=f"u{i}"))

        for weight in (1, 2, 3):
            await graph.create_edge(
                "FOLLOWS", FollowsCreate(from_key="u0", to_key="u1", weight=weight)
            )

        batches = [
            batch async for batch in graph.find_edges_stream("FOLLOWS", chunk_size=1)
        ]

        assert [len(batch) for batch in batches] == [3]  # one pair, three edges, one page


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

    def test_a_plaintext_key_beside_an_encrypted_property_still_streams(self) -> None:
        # The encryption rule is about the *key*, not about encryption: sealing an ordinary
        # property is fine, and sealing the key is refused at spec construction (see
        # ``TestSealedKeyField``), so no kind that reaches here can have one.
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

    def test_a_keyed_edge_bookmarks_on_one_field_an_endpoint_edge_on_two(self) -> None:
        spec = _spec()

        assert assert_edge_streamable(
            spec,
            spec.graph_edge_by_kind("KNOWS"),
            kind="KNOWS",
            capabilities=GraphReadCapabilities(supports_edge_streaming=True),
        ) == ("id",)

        # No key of its own, so the cursor is the endpoint pair — the identity it declared.
        assert assert_edge_streamable(
            spec,
            spec.graph_edge_by_kind("FOLLOWS"),
            kind="FOLLOWS",
            capabilities=GraphReadCapabilities(supports_edge_streaming=True),
        ) == ("id", "id")

    def test_an_endpoint_edge_whose_endpoint_kinds_key_differently_is_refused(self) -> None:
        # A multi-endpoint kind may link Post → Tag *and* Note → Tag. If Post and Note key on
        # different properties there is no single ORDER BY covering both, and a partial one is
        # not offered.
        spec = GraphModuleSpec(
            name="content",
            nodes=(
                GraphNodeSpec(name="Post", read=UserRead, key_field="id"),
                GraphNodeSpec(name="Note", read=KnowsRead, key_field="slug"),
                GraphNodeSpec(name="Tag", read=UserRead, key_field="id"),
            ),
            edges=(
                GraphEdgeSpec(
                    name="TAGGED",
                    read=FollowsRead,
                    identity="endpoints",
                    endpoints=(
                        GraphEdgeEndpoint(from_kind="Post", to_kind="Tag"),
                        GraphEdgeEndpoint(from_kind="Note", to_kind="Tag"),
                    ),
                    directionality=GraphEdgeDirectionality.DIRECTED,
                ),
            ),
        )

        with pytest.raises(CoreException, match="do not agree on a key field"):
            assert_edge_streamable(
                spec,
                spec.graph_edge_by_kind("TAGGED"),
                kind="TAGGED",
                capabilities=GraphReadCapabilities(supports_edge_streaming=True),
            )


# ....................... #


class TestSealedKeyField:
    """A sealed key is not a key — refused where the damage is nil, at construction.

    This used to be a *streaming* refusal, which badly undersold it. A field-encrypted key
    cannot be **matched** either: a lookup compares the caller's plaintext against the stored
    ciphertext, so a vertex created under a sealed key could never be fetched, updated or
    deleted by that key — a write-only black hole. The mock hid it completely (it stores
    properties unsealed and keys its store by the plaintext), so only a real backend showed it.
    """

    def test_a_node_kind_cannot_seal_its_own_key_field(self) -> None:
        with pytest.raises(CoreException) as exc_info:
            GraphNodeSpec(
                name="User",
                read=UserRead,
                create=UserCreate,
                encryption=FieldEncryption(encrypted=frozenset({"id"})),
            )

        assert exc_info.value.kind is ExceptionKind.CONFIGURATION
        assert exc_info.value.code == "graph_sealed_key_field"
        assert "A sealed key is not a key" in str(exc_info.value)

    def test_a_deterministic_key_is_refused_too(self) -> None:
        # Searchable (deterministic) encryption is the tempting case — the ciphertext *is*
        # matchable, so a lookup would work — but its order is not the plaintext's, and it is
        # still the key leaking through an equality oracle. One rule, no exceptions.
        with pytest.raises(CoreException, match="A sealed key is not a key"):
            GraphNodeSpec(
                name="User",
                read=UserRead,
                encryption=FieldEncryption(searchable=frozenset({"id"})),
            )

    def test_an_edge_kind_cannot_seal_its_own_key_field(self) -> None:
        with pytest.raises(CoreException, match="A sealed key is not a key"):
            GraphEdgeSpec(
                name="KNOWS",
                read=KnowsRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
                encryption=FieldEncryption(encrypted=frozenset({"id"})),
            )

    def test_sealing_an_ordinary_property_is_fine(self) -> None:
        node = GraphNodeSpec(
            name="User",
            read=UserRead,
            encryption=FieldEncryption(encrypted=frozenset({"name"})),
        )

        assert node.key_field == "id"


# ....................... #


class TestKeysetLoop:
    """The shared loop's own invariants, driven directly."""

    async def test_a_backend_that_does_not_advance_raises_instead_of_spinning(
        self,
    ) -> None:
        # A backend that ignores the seek — a dropped predicate, an unordered result — hands
        # back the same full page forever, and the walk yields the same rows forever with it.
        # Loud beats endless.
        async def _stuck(after, limit):  # type: ignore[no-untyped-def]
            return [(f"k{i}", "row") for i in range(limit)]

        with pytest.raises(CoreException) as exc_info:
            await _collect(stream_keyset_pages(_stuck, chunk_size=2))

        assert exc_info.value.code == "graph_streaming_no_progress"

    async def test_a_page_longer_than_the_window_does_not_end_the_walk(self) -> None:
        # ``chunk_size`` bounds distinct keys, and a key may carry several rows — so a full page
        # can be *longer* than the window. Counting rows would still terminate here, but the
        # window's meaning is keys, and that is what exhaustion has to be measured in.
        pages = [
            [("a", "r1"), ("a", "r2"), ("b", "r3")],  # 2 distinct keys == chunk_size → continue
            [("c", "r4")],  # 1 distinct key < chunk_size → done
        ]

        async def _fetch(after, limit):  # type: ignore[no-untyped-def]
            return pages.pop(0) if pages else []

        rows = await _collect(stream_keyset_pages(_fetch, chunk_size=2))

        assert rows == ["r1", "r2", "r3", "r4"]

    async def test_a_chunk_size_below_one_is_refused(self) -> None:
        async def _never(after, limit):  # type: ignore[no-untyped-def]
            raise AssertionError("must not be called")

        with pytest.raises(CoreException, match="at least 1"):
            await _collect(stream_keyset_pages(_never, chunk_size=0))
