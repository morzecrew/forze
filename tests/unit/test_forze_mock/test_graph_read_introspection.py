"""Unit tests for the mock graph read-introspection methods (WS2)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.graph import (
    EdgeRef,
    GraphDirection,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    VertexRef,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit


class UserRead(BaseModel):
    id: str
    name: str | None = None


class UserCreate(BaseModel):
    id: str
    name: str | None = None


class RatedRead(BaseModel):
    id: str
    score: int


class RatedCreate(BaseModel):
    id: str
    from_key: str
    to_key: str
    score: int


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
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
            GraphEdgeSpec(
                name="RATED",
                read=RatedRead,
                identity="key",
                key_field="id",
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


def _u(k: str) -> VertexRef:
    return VertexRef(kind="User", key=k)


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


async def _seed(cmd: object) -> None:
    await cmd.create_vertex("User", UserCreate(id="a", name="Ana"))
    await cmd.create_vertex("User", UserCreate(id="b", name="Bo"))
    await cmd.create_vertex("User", UserCreate(id="c", name="Ana"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="c"))
    await cmd.create_edge("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=5))


@pytest.mark.asyncio
async def test_get_vertices_found_only_in_order(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    out = await q.get_vertices([_u("c"), _u("missing"), _u("a")])
    assert [v.id for v in out] == ["c", "a"]


@pytest.mark.asyncio
async def test_get_edges_and_edge_exists(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    out = await q.get_edges(
        [EdgeRef.by_key("RATED", "r1"), EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))]
    )
    assert len(out) == 2

    assert await q.edge_exists(EdgeRef.by_key("RATED", "r1")) is True
    assert await q.edge_exists(EdgeRef.by_endpoints("FOLLOWS", _u("b"), _u("a"))) is False


@pytest.mark.asyncio
async def test_counts_and_filter(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert await q.count_vertices("User") == 3
    assert await q.count_vertices("User", property_filter={"name": "Ana"}) == 2
    assert await q.count_edges("FOLLOWS") == 2
    assert await q.count_edges("RATED", property_filter={"score": 5}) == 1


@pytest.mark.asyncio
async def test_degree_neighbors_incident(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert await q.vertex_degree(_u("a"), direction=GraphDirection.OUT) == 3
    assert (
        await q.vertex_degree(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        )
        == 2
    )
    assert (
        await q.count_neighbors(
            _u("a"), direction=GraphDirection.OUT, edge_kinds=frozenset({"FOLLOWS"})
        )
        == 2
    )
    incident = await q.incident_edges(_u("a"), GraphDirection.OUT, frozenset({"FOLLOWS"}), limit=10)
    assert len(incident) == 2


@pytest.mark.asyncio
async def test_find_vertices_orders_paginates_filters(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert [v.id for v in await q.find_vertices("User")] == ["a", "b", "c"]
    assert [v.id for v in await q.find_vertices("User", limit=1, offset=1)] == ["b"]
    assert [v.id for v in await q.find_vertices("User", property_filter={"name": "Ana"})] == [
        "a",
        "c",
    ]


@pytest.mark.asyncio
async def test_find_edges_keyed(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert [getattr(e, "id", None) for e in await q.find_edges("RATED")] == ["r1"]
    assert await q.find_edges("RATED", property_filter={"score": 99}) == []


class FollowsUpdate(BaseModel):
    weight: int | None = None


class RatedUpdate(BaseModel):
    score: int


@pytest.mark.asyncio
async def test_ensure_vertex_create_then_unchanged(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    first = await cmd.ensure_vertex("User", UserCreate(id="a", name="first"))
    assert first.name == "first"
    again = await cmd.ensure_vertex("User", UserCreate(id="a", name="second"))
    assert again.name == "first"  # existing returned unchanged
    assert (await qry.get_vertex(_u("a"))).name == "first"


@pytest.mark.asyncio
async def test_bulk_create_update_delete(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    out = await cmd.create_vertices([("User", UserCreate(id=k)) for k in ("a", "b", "c")])
    assert [v.id for v in out] == ["a", "b", "c"]

    await cmd.create_edges(
        [
            ("RATED", RatedCreate(id="r1", from_key="a", to_key="b", score=1)),
            ("RATED", RatedCreate(id="r2", from_key="a", to_key="c", score=2)),
        ]
    )
    updated = await cmd.update_edge(EdgeRef.by_key("RATED", "r1"), RatedUpdate(score=9))
    assert updated.score == 9

    await cmd.delete_edges([EdgeRef.by_key("RATED", "r1")])
    assert await qry.count_edges("RATED") == 1

    await cmd.delete_vertices([_u("a"), _u("b")])
    assert await qry.count_vertices("User") == 1


@pytest.mark.asyncio
async def test_update_edge_endpoints_and_missing(ctx: ExecutionContext) -> None:
    spec = _spec()
    cmd = ctx.graph.command(spec)

    await cmd.create_vertex("User", UserCreate(id="a"))
    await cmd.create_vertex("User", UserCreate(id="b"))
    await cmd.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=1))

    ref = EdgeRef.by_endpoints("FOLLOWS", _u("a"), _u("b"))
    assert (await cmd.update_edge(ref, FollowsUpdate(weight=7))).weight == 7

    with pytest.raises(CoreException, match="graph_edge_not_found"):
        await cmd.update_edge(EdgeRef.by_key("RATED", "nope"), RatedUpdate(score=1))


class _TagCreate(BaseModel):
    id: str


class _TaggedCreate(BaseModel):
    from_key: str
    to_key: str
    from_kind: str | None = None
    to_kind: str | None = None


def _multi_spec() -> GraphModuleSpec:
    from forze.application.contracts.graph import (
        GraphEdgeDirectionality,
        GraphEdgeEndpoint,
        GraphEdgeSpec,
    )

    class TaggedRead(BaseModel):
        weight: int | None = None

    return GraphModuleSpec(
        name="multi",
        nodes=(
            GraphNodeSpec(name="Post", read=UserRead, create=_TagCreate),
            GraphNodeSpec(name="Note", read=UserRead, create=_TagCreate),
            GraphNodeSpec(name="Tag", read=UserRead, create=_TagCreate),
        ),
        edges=(
            GraphEdgeSpec(
                name="TAGGED",
                read=TaggedRead,
                identity="endpoints",
                endpoints=(
                    GraphEdgeEndpoint(from_kind="Post", to_kind="Tag"),
                    GraphEdgeEndpoint(from_kind="Note", to_kind="Tag"),
                ),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_mock_multi_endpoint_distinct_by_kind() -> None:
    spec = _multi_spec()
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    cmd = ctx.graph.command(spec)
    qry = ctx.graph.query(spec)

    await cmd.create_vertex("Post", _TagCreate(id="1"))
    await cmd.create_vertex("Note", _TagCreate(id="1"))
    await cmd.create_vertex("Tag", _TagCreate(id="x"))
    # same key values, different endpoint kinds -> two distinct edges
    await cmd.create_edge(
        "TAGGED", _TaggedCreate(from_key="1", to_key="x", from_kind="Post", to_kind="Tag")
    )
    await cmd.create_edge(
        "TAGGED", _TaggedCreate(from_key="1", to_key="x", from_kind="Note", to_kind="Tag")
    )

    assert await qry.count_edges("TAGGED") == 2

    with pytest.raises(CoreException) as ei:
        await cmd.create_edge("TAGGED", _TaggedCreate(from_key="1", to_key="x"))
    assert ei.value.code == "graph_edge_endpoint_kind_required"


@pytest.mark.asyncio
async def test_filter_on_encrypted_field_rejected() -> None:
    """A filter on a sealed property fails closed (can't match ciphertext)."""

    from forze.application.contracts.crypto import FieldEncryption

    class SecretRead(BaseModel):
        id: str
        ssn: str

    class SecretCreate(BaseModel):
        id: str
        ssn: str

    spec = GraphModuleSpec(
        name="sec",
        nodes=(
            GraphNodeSpec(
                name="Secret",
                read=SecretRead,
                create=SecretCreate,
                encryption=FieldEncryption(encrypted=frozenset({"ssn"})),
            ),
        ),
        edges=(),
    )
    ctx = context_from_deps(MockDepsModule(state=MockState())())
    q = ctx.graph.query(spec)

    with pytest.raises(CoreException) as ei:
        await q.count_vertices("Secret", property_filter={"ssn": "123"})
    assert ei.value.code == "graph_filter_on_encrypted_field"


@pytest.mark.asyncio
async def test_filter_key_must_be_identifier(ctx: ExecutionContext) -> None:
    """A non-identifier filter key fails closed on every property-filter entry point.

    Neo4j rejects such a key before building a query (it lands in a ``$pf_<key>``
    parameter name); the mock enforces the same rule so a simulation cannot pass
    with a filter key that production would reject.
    """

    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    bad = {"name = $tenant OR true //": "x"}

    for call in (
        lambda: q.count_vertices("User", property_filter=bad),
        lambda: q.count_edges("RATED", property_filter=bad),
        lambda: q.find_vertices("User", property_filter=bad),
        lambda: q.find_edges("RATED", property_filter=bad),
    ):
        with pytest.raises(CoreException) as ei:
            await call()
        assert ei.value.code == "graph_filter_key_invalid"


@pytest.mark.asyncio
async def test_filter_identifier_keys_accepted(ctx: ExecutionContext) -> None:
    spec = _spec()
    await _seed(ctx.graph.command(spec))
    q = ctx.graph.query(spec)

    assert await q.count_vertices("User", property_filter={"name": "Ana"}) == 2
    assert await q.count_edges("RATED", property_filter={"_score2": 1}) == 0
