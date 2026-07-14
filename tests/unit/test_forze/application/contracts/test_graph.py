"""Unit tests for :mod:`forze.application.contracts.graph`.

Covers the declarative graph specs (lookup + structural validation), the
read/write value objects and their defaults, the direction enums, and the
dependency keys/ports. The ports themselves are ``runtime_checkable``
protocols, so they are exercised via ``isinstance`` structural checks.
"""

from enum import StrEnum

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import DepKey
from forze.application.contracts.graph import (
    BaseGraphModulePort,
    EdgeRef,
    GraphCommandDepKey,
    GraphCommandPort,
    GraphDirection,
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphQueryDepKey,
    GraphQueryPort,
    GraphRawQueryDepKey,
    GraphRawQueryPort,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
    resolve_query_directions,
    validate_graph_module_spec,
)
from forze.base.exceptions import CoreException

# ----------------------- #
# Test fixtures (read DTOs)


class _PersonRead(BaseModel):
    id: str
    name: str


class _TagRead(BaseModel):
    id: str
    label: str


class _KnowsRead(BaseModel):
    since: int


# ....................... #


def _person_node(name: str = "person") -> GraphNodeSpec[_PersonRead]:
    return GraphNodeSpec(name=name, read=_PersonRead)


def _tag_node(name: str = "tag") -> GraphNodeSpec[_TagRead]:
    return GraphNodeSpec(name=name, read=_TagRead)


def _knows_edge(
    *,
    name: str = "knows",
    endpoints: tuple[GraphEdgeEndpoint, ...] = (
        GraphEdgeEndpoint(from_kind="person", to_kind="person"),
    ),
    directionality: GraphEdgeDirectionality = GraphEdgeDirectionality.DIRECTED,
) -> GraphEdgeSpec[_KnowsRead]:
    return GraphEdgeSpec(
        name=name,
        read=_KnowsRead,
        identity="endpoints",
        endpoints=endpoints,
        directionality=directionality,
    )


# ----------------------- #
# Enums


class TestGraphEnums:
    def test_direction_values(self) -> None:
        assert GraphDirection.OUT == "out"
        assert GraphDirection.IN == "in"
        assert GraphDirection.BOTH == "both"
        assert set(GraphDirection) == {
            GraphDirection.OUT,
            GraphDirection.IN,
            GraphDirection.BOTH,
        }

    def test_directionality_values(self) -> None:
        assert GraphEdgeDirectionality.DIRECTED == "directed"
        assert GraphEdgeDirectionality.SYMMETRIC == "symmetric"


# ----------------------- #
# Specs


class TestGraphNodeSpec:
    def test_defaults(self) -> None:
        node = _person_node()
        assert node.read is _PersonRead
        assert node.create is None
        assert node.update is None

    def test_with_create_and_update(self) -> None:
        node = GraphNodeSpec(
            name="person",
            read=_PersonRead,
            create=_PersonRead,
            update=_PersonRead,
        )
        assert node.create is _PersonRead
        assert node.update is _PersonRead


class TestGraphEdgeSpec:
    def test_defaults(self) -> None:
        edge = _knows_edge()
        assert edge.read is _KnowsRead
        assert edge.directionality is GraphEdgeDirectionality.DIRECTED
        assert edge.query_directions is None
        assert edge.endpoints[0].from_kind == "person"

    def test_query_directions_can_be_set(self) -> None:
        edge = GraphEdgeSpec(
            name="knows",
            read=_KnowsRead,
            endpoints=(GraphEdgeEndpoint(from_kind="person", to_kind="person"),),
            directionality=GraphEdgeDirectionality.SYMMETRIC,
            query_directions=frozenset({GraphDirection.BOTH}),
        )
        assert edge.query_directions == frozenset({GraphDirection.BOTH})


class TestGraphModuleSpecLookup:
    def test_node_by_kind_found_and_missing(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(), _tag_node()),
            edges=(),
        )
        assert spec.graph_node_by_kind("person") is spec.nodes[0]
        assert spec.graph_node_by_kind("tag") is spec.nodes[1]
        assert spec.graph_node_by_kind("missing") is None

    def test_edge_by_kind_found_and_missing(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(),),
            edges=(_knows_edge(),),
        )
        assert spec.graph_edge_by_kind("knows") is spec.edges[0]
        assert spec.graph_edge_by_kind("nope") is None

    def test_lookup_matches_str_enum_name(self) -> None:
        class Kind(StrEnum):
            PERSON = "person"

        spec = GraphModuleSpec(
            name="social",
            nodes=(GraphNodeSpec(name=Kind.PERSON, read=_PersonRead),),
            edges=(),
        )
        # Lookup uses the string value of the StrEnum name.
        assert spec.graph_node_by_kind("person") is spec.nodes[0]


# ----------------------- #
# validate_graph_module_spec


class TestValidateGraphModuleSpec:
    def test_valid_spec_passes(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(), _tag_node()),
            edges=(
                _knows_edge(),
                _knows_edge(
                    name="tagged",
                    endpoints=(
                        GraphEdgeEndpoint(from_kind="person", to_kind="tag"),
                    ),
                ),
            ),
        )
        # Should not raise.
        validate_graph_module_spec(spec)

    def test_empty_nodes_rejected_by_default(self) -> None:
        spec = GraphModuleSpec(name="empty", nodes=(), edges=())
        with pytest.raises(CoreException, match="must be non-empty"):
            validate_graph_module_spec(spec)

    def test_empty_nodes_allowed_when_opted_out(self) -> None:
        spec = GraphModuleSpec(name="empty", nodes=(), edges=())
        validate_graph_module_spec(spec, require_non_empty_nodes=False)

    def test_duplicate_node_kind_rejected(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(), _person_node()),
            edges=(),
        )
        with pytest.raises(CoreException, match="Duplicate graph node kind"):
            validate_graph_module_spec(spec)

    def test_duplicate_edge_kind_rejected(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(),),
            edges=(_knows_edge(), _knows_edge()),
        )
        with pytest.raises(CoreException, match="Duplicate graph edge kind"):
            validate_graph_module_spec(spec)

    def test_edge_without_endpoints_rejected(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(),),
            edges=(_knows_edge(endpoints=()),),
        )
        with pytest.raises(CoreException, match="at least one GraphEdgeEndpoint"):
            validate_graph_module_spec(spec)

    def test_unknown_from_kind_rejected(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(),),
            edges=(
                _knows_edge(
                    endpoints=(
                        GraphEdgeEndpoint(from_kind="ghost", to_kind="person"),
                    ),
                ),
            ),
        )
        with pytest.raises(CoreException, match="unknown from_kind 'ghost'"):
            validate_graph_module_spec(spec)

    def test_unknown_to_kind_rejected(self) -> None:
        spec = GraphModuleSpec(
            name="social",
            nodes=(_person_node(),),
            edges=(
                _knows_edge(
                    endpoints=(
                        GraphEdgeEndpoint(from_kind="person", to_kind="ghost"),
                    ),
                ),
            ),
        )
        with pytest.raises(CoreException, match="unknown to_kind 'ghost'"):
            validate_graph_module_spec(spec)


# ----------------------- #
# Value objects


class TestRefs:
    def test_vertex_ref_frozen(self) -> None:
        ref = VertexRef(kind="person", key="p1")
        assert (ref.kind, ref.key) == ("person", "p1")
        with pytest.raises(attrs_frozen_error()):
            ref.kind = "tag"  # type: ignore[misc]

    def test_edge_ref_by_key(self) -> None:
        ref = EdgeRef.by_key("knows", "e1")
        assert (ref.kind, ref.key) == ("knows", "e1")
        assert ref.is_keyed
        assert ref.from_ref is None and ref.to_ref is None

    def test_edge_ref_by_endpoints(self) -> None:
        a = VertexRef(kind="person", key="p1")
        b = VertexRef(kind="person", key="p2")
        ref = EdgeRef.by_endpoints("knows", a, b)
        assert not ref.is_keyed
        assert ref.from_ref == a and ref.to_ref == b
        assert ref.key is None

    def test_edge_ref_requires_exactly_one_mode(self) -> None:
        with pytest.raises(CoreException, match="key or a"):
            EdgeRef(kind="knows")  # neither mode

        a = VertexRef(kind="person", key="p1")
        with pytest.raises(CoreException, match="not both"):
            EdgeRef(kind="knows", key="e1", from_ref=a, to_ref=a)  # both modes

        with pytest.raises(CoreException, match="both from_ref and to_ref"):
            EdgeRef(kind="knows", from_ref=a)  # partial endpoints

    def test_endpoint(self) -> None:
        ep = GraphEdgeEndpoint(from_kind="person", to_kind="tag")
        assert (ep.from_kind, ep.to_kind) == ("person", "tag")


class TestScopedWalkParams:
    def test_valid(self) -> None:
        from forze.application.contracts.graph import GraphPathStep, ScopedWalkParams

        params = ScopedWalkParams(
            steps=[GraphPathStep(edge_kinds=frozenset({"knows"}), min_hops=1, max_hops=3)],
            target_kind="person",
        )
        assert len(params.steps) == 1
        assert params.limit == 100

    def test_step_rejects_bad_hop_bounds(self) -> None:
        from forze.application.contracts.graph import GraphPathStep

        with pytest.raises(CoreException, match="graph_path_step_bounds"):
            GraphPathStep(min_hops=3, max_hops=1)

        with pytest.raises(CoreException, match="graph_path_step_bounds"):
            GraphPathStep(min_hops=-1, max_hops=2)

    def test_rejects_empty_steps(self) -> None:
        from forze.application.contracts.graph import ScopedWalkParams

        with pytest.raises(CoreException, match="graph_scoped_walk_steps"):
            ScopedWalkParams(steps=[], target_kind="person")

    def test_rejects_nonpositive_limit(self) -> None:
        from forze.application.contracts.graph import GraphPathStep, ScopedWalkParams

        with pytest.raises(CoreException, match="graph_scoped_walk_limit"):
            ScopedWalkParams(
                steps=[GraphPathStep()],
                target_kind="person",
                limit=0,
            )

    def test_step_rejects_non_integer_hops(self) -> None:
        from forze.application.contracts.graph import GraphPathStep

        with pytest.raises(CoreException, match="graph_path_step_bounds"):
            GraphPathStep(min_hops="1", max_hops=2)  # type: ignore[arg-type]

        with pytest.raises(CoreException, match="graph_path_step_bounds"):
            GraphPathStep(min_hops=1, max_hops="3]->() //")  # type: ignore[arg-type]

    def test_rejects_non_integer_limit(self) -> None:
        from forze.application.contracts.graph import GraphPathStep, ScopedWalkParams

        with pytest.raises(CoreException, match="graph_scoped_walk_limit"):
            ScopedWalkParams(
                steps=[GraphPathStep()],
                target_kind="person",
                limit="10",  # type: ignore[arg-type]
            )


class TestShortestPathParams:
    def test_valid(self) -> None:
        params = ShortestPathParams(max_hops=4)
        assert params.max_hops == 4
        assert params.weight_property is None

    def test_zero_max_hops_allowed(self) -> None:
        # 0 is a legitimate degenerate bound: only a zero-length path can qualify.
        assert ShortestPathParams(max_hops=0).max_hops == 0

    def test_rejects_negative_max_hops(self) -> None:
        with pytest.raises(CoreException, match="graph_shortest_path_bounds"):
            ShortestPathParams(max_hops=-3)

    def test_rejects_non_integer_max_hops(self) -> None:
        with pytest.raises(CoreException, match="graph_shortest_path_bounds"):
            ShortestPathParams(max_hops="5]->() DETACH DELETE n //")  # type: ignore[arg-type]

        with pytest.raises(CoreException, match="graph_shortest_path_bounds"):
            ShortestPathParams(max_hops=2.5)  # type: ignore[arg-type]


class TestKeyField:
    def test_node_key_field_default_and_override(self) -> None:
        assert _person_node().key_field == "id"
        assert GraphNodeSpec(name="x", read=_PersonRead, key_field="name").key_field == "name"

    def test_keyed_edge_requires_existing_key_field(self) -> None:
        spec = GraphModuleSpec(
            name="m",
            nodes=(_person_node(),),
            edges=(
                GraphEdgeSpec(
                    name="knows",
                    read=_KnowsRead,
                    identity="key",
                    key_field="missing",
                    endpoints=(GraphEdgeEndpoint(from_kind="person", to_kind="person"),),
                    directionality=GraphEdgeDirectionality.DIRECTED,
                ),
            ),
        )
        with pytest.raises(CoreException, match="key_field"):
            validate_graph_module_spec(spec)


class TestResolveQueryDirections:
    def test_directed_default(self) -> None:
        edge = _knows_edge(directionality=GraphEdgeDirectionality.DIRECTED)
        assert resolve_query_directions(edge) == frozenset(
            {GraphDirection.OUT, GraphDirection.IN}
        )

    def test_symmetric_default(self) -> None:
        edge = _knows_edge(directionality=GraphEdgeDirectionality.SYMMETRIC)
        assert resolve_query_directions(edge) == frozenset({GraphDirection.BOTH})

    def test_explicit_override(self) -> None:
        edge = GraphEdgeSpec(
            name="knows",
            read=_KnowsRead,
            identity="endpoints",
            endpoints=(GraphEdgeEndpoint(from_kind="person", to_kind="person"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
            query_directions=frozenset({GraphDirection.OUT}),
        )
        assert resolve_query_directions(edge) == frozenset({GraphDirection.OUT})


class TestWalkValueObjects:
    def test_walk_params_defaults(self) -> None:
        params = GraphWalkParams(max_depth=3, max_results=100)
        assert params.direction is GraphDirection.BOTH
        assert params.edge_kinds == frozenset()

    def test_walk_params_overrides(self) -> None:
        params = GraphWalkParams(
            max_depth=2,
            max_results=10,
            direction=GraphDirection.OUT,
            edge_kinds=frozenset({"knows"}),
        )
        assert params.direction is GraphDirection.OUT
        assert params.edge_kinds == frozenset({"knows"})

    def test_walk_params_reject_bad_bounds(self) -> None:
        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(max_depth=0, max_results=10)

        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(max_depth=-1, max_results=10)

        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(max_depth=3, max_results=0)

    def test_walk_params_reject_non_integer_bounds(self) -> None:
        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(
                max_depth="3] MATCH (x) DETACH DELETE x //",  # type: ignore[arg-type]
                max_results=10,
            )

        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(max_depth=True, max_results=10)

        with pytest.raises(CoreException, match="graph_walk_params_bounds"):
            GraphWalkParams(max_depth=3, max_results=2.5)  # type: ignore[arg-type]

    def test_walk_step_root(self) -> None:
        step = GraphWalkStep(
            depth=0,
            vertex=_PersonRead(id="p1", name="Ada"),
            from_parent=None,
            parent_ref=None,
        )
        assert step.depth == 0
        assert step.from_parent is None
        assert step.parent_ref is None

    def test_walk_step_child(self) -> None:
        step = GraphWalkStep(
            depth=1,
            vertex=_PersonRead(id="p2", name="Bob"),
            from_parent=_KnowsRead(since=2020),
            parent_ref=VertexRef(kind="person", key="p1"),
        )
        assert step.depth == 1
        assert step.parent_ref == VertexRef(kind="person", key="p1")

    def test_neighbor_row(self) -> None:
        row = NeighborRow(
            other=_PersonRead(id="p2", name="Bob"),
            via_edge=_KnowsRead(since=2021),
            direction=GraphDirection.OUT,
        )
        assert row.direction is GraphDirection.OUT
        assert isinstance(row.other, _PersonRead)


class TestShortestPath:
    def test_params_defaults(self) -> None:
        params = ShortestPathParams(max_hops=5)
        assert params.max_hops == 5
        assert params.edge_kinds == frozenset()
        assert not hasattr(params, "max_paths")

    def test_result_parallel_sequences(self) -> None:
        v0 = _PersonRead(id="p1", name="Ada")
        v1 = _PersonRead(id="p2", name="Bob")
        e0 = _KnowsRead(since=2019)
        result = ShortestPathResult(vertices=(v0, v1), edges=(e0,))
        assert len(result.vertices) == len(result.edges) + 1


# ----------------------- #
# Deps keys and ports


class TestGraphDeps:
    def test_dep_keys_are_named(self) -> None:
        assert isinstance(GraphQueryDepKey, DepKey)
        assert isinstance(GraphCommandDepKey, DepKey)
        assert GraphQueryDepKey.name == "graph_query"
        assert GraphCommandDepKey.name == "graph_command"

    def test_dep_keys_distinct(self) -> None:
        assert GraphQueryDepKey != GraphCommandDepKey
        assert GraphRawQueryDepKey != GraphQueryDepKey
        assert GraphRawQueryDepKey.name == "graph_raw_query"


async def _empty_stream():  # noqa: ANN202
    """An async generator yielding nothing — the stream methods' shape, not their behaviour."""

    return
    yield  # pragma: no cover  (makes this a generator)


class TestGraphPortProtocols:
    """``runtime_checkable`` structural checks for the port protocols."""

    def test_query_port_structural_match(self) -> None:
        spec = GraphModuleSpec(name="g", nodes=(_person_node(),), edges=())

        class _Query:
            def __init__(self) -> None:
                self.spec = spec

            async def get_vertex(self, ref):  # noqa: ANN001, ANN202
                return None

            async def get_vertices(self, refs):  # noqa: ANN001, ANN202
                return []

            async def get_edge(self, ref):  # noqa: ANN001, ANN202
                return None

            async def get_edges(self, refs):  # noqa: ANN001, ANN202
                return []

            async def vertex_exists(self, ref):  # noqa: ANN001, ANN202
                return False

            async def edge_exists(self, ref):  # noqa: ANN001, ANN202
                return False

            async def count_vertices(self, node_kind, *, property_filter=None):  # noqa: ANN001, ANN202
                return 0

            async def count_edges(self, edge_kind, *, property_filter=None):  # noqa: ANN001, ANN202
                return 0

            async def neighbors(  # noqa: ANN202, PLR0913
                self,
                origin,  # noqa: ANN001
                direction,  # noqa: ANN001
                edge_kinds,  # noqa: ANN001
                *,
                limit,  # noqa: ANN001
                to_vertex_kinds=None,  # noqa: ANN001
            ):
                return []

            async def incident_edges(  # noqa: ANN202
                self,
                origin,  # noqa: ANN001
                direction,  # noqa: ANN001
                edge_kinds,  # noqa: ANN001
                *,
                limit,  # noqa: ANN001
            ):
                return []

            async def expand(self, start, params):  # noqa: ANN001, ANN202
                return []

            async def shortest_path(self, from_ref, to_ref, params):  # noqa: ANN001, ANN202
                return None

            async def scoped_walk(self, anchor, params):  # noqa: ANN001, ANN202
                return []

            async def k_shortest_paths(self, from_ref, to_ref, params, *, k):  # noqa: ANN001, ANN202
                return []

            async def find_vertices(  # noqa: ANN202
                self,
                node_kind,  # noqa: ANN001
                *,
                property_filter=None,  # noqa: ANN001
                limit=100,  # noqa: ANN001
                offset=0,  # noqa: ANN001
            ):
                return []

            async def find_edges(  # noqa: ANN202
                self,
                edge_kind,  # noqa: ANN001
                *,
                property_filter=None,  # noqa: ANN001
                limit=100,  # noqa: ANN001
                offset=0,  # noqa: ANN001
            ):
                return []

            def find_vertices_stream(  # noqa: ANN202
                self,
                node_kind,  # noqa: ANN001
                *,
                property_filter=None,  # noqa: ANN001
                chunk_size=500,  # noqa: ANN001
            ):
                return _empty_stream()

            def find_edges_stream(  # noqa: ANN202
                self,
                edge_kind,  # noqa: ANN001
                *,
                property_filter=None,  # noqa: ANN001
                chunk_size=500,  # noqa: ANN001
            ):
                return _empty_stream()

            async def vertex_degree(self, ref, *, direction=GraphDirection.BOTH, edge_kinds=None):  # noqa: ANN001, ANN202
                return 0

            async def count_neighbors(self, ref, *, direction=GraphDirection.BOTH, edge_kinds=None):  # noqa: ANN001, ANN202
                return 0

        assert isinstance(_Query(), GraphQueryPort)
        assert isinstance(_Query(), BaseGraphModulePort)

    def test_incomplete_query_port_not_match(self) -> None:
        class _Partial:
            spec = None

        assert not isinstance(_Partial(), GraphQueryPort)

    def test_command_port_structural_match(self) -> None:
        spec = GraphModuleSpec(name="g", nodes=(_person_node(),), edges=())

        class _Command:
            def __init__(self) -> None:
                self.spec = spec

            async def create_vertex(self, node_kind, cmd, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def update_vertex(self, ref, cmd):  # noqa: ANN001, ANN202
                return cmd

            async def delete_vertex(self, ref):  # noqa: ANN001, ANN202
                return None

            async def create_edge(self, edge_kind, cmd, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def update_edge(self, ref, cmd):  # noqa: ANN001, ANN202
                return cmd

            async def delete_edge(self, ref):  # noqa: ANN001, ANN202
                return None

            async def create_vertices(self, items, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def create_edges(self, items, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def ensure_vertex(self, node_kind, cmd, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def ensure_edge(self, edge_kind, cmd, *, return_new=True):  # noqa: ANN001, ANN202
                return None

            async def delete_vertices(self, refs):  # noqa: ANN001, ANN202
                return None

            async def delete_edges(self, refs):  # noqa: ANN001, ANN202
                return None

        assert isinstance(_Command(), GraphCommandPort)

    def test_raw_query_port_structural_match(self) -> None:
        spec = GraphModuleSpec(name="g", nodes=(_person_node(),), edges=())

        class _Raw:
            def __init__(self) -> None:
                self.spec = spec

            async def run(self, query, params=None):  # noqa: ANN001, ANN202
                return []

        assert isinstance(_Raw(), GraphRawQueryPort)


# ----------------------- #
# Helpers


def attrs_frozen_error() -> type[Exception]:
    """``attrs`` raises ``FrozenInstanceError`` on assignment to frozen slots."""

    import attrs

    return attrs.exceptions.FrozenInstanceError
