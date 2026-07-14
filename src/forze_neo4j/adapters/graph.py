"""Neo4j graph adapter implementing the graph query/command/raw ports.

**Full ``GraphQueryPort`` / ``GraphCommandPort`` coverage**, plus the raw escape hatch:
vertex/edge CRUD and bulk (``create_vertices``/``create_edges``/``delete_vertices``/
``delete_edges``, ``ensure_vertex``/``ensure_edge``, ``update_edge``/``delete_edge``),
read-introspection (``get_vertices``/``get_edges``/``edge_exists``, ``count_*``,
``vertex_degree``/``count_neighbors``/``incident_edges``, ``find_*``), and traversal/paths
(``neighbors``/``expand``/``scoped_walk``/``shortest_path``/``k_shortest_paths`` — native
``SHORTEST k`` plus weighted via GDS). A **multi-endpoint edge kind** (a spec declaring more
than one ``(from, to)`` label pair) is supported too: its create/ensure command names the
pair via ``from_kind`` / ``to_kind`` (see :func:`~forze.application.integrations.graph.\
resolve_write_endpoint`).

**Schema provisioning** is available via :meth:`ensure_schema` (the ``GraphManagementPort``):
it creates node key-uniqueness constraints (composite with the tenant property under tagged
tenancy), keyed-edge key-uniqueness constraints — so a concurrent ``ensure_edge`` cannot
create duplicate keyed edges, not just the in-query ``MERGE`` — and tenant-property indexes.
It is opt-in (run it at startup); the constraints are Community-edition uniqueness (a NODE KEY
constraint, which also enforces existence, is Enterprise-only).

Tenancy uses property partition: a ``tenant_property`` is stamped on writes and constrains
anchor-node matches.
"""

from forze_neo4j._compat import require_neo4j

require_neo4j()

# ....................... #

from collections.abc import AsyncGenerator, Sequence
from typing import Any, Literal, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.graph import (
    EdgeRef,
    GraphDirection,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    GraphReadCapabilities,
    GraphWalkParams,
    GraphWalkStep,
    NeighborRow,
    ScopedWalkParams,
    ShortestPathParams,
    ShortestPathResult,
    VertexRef,
    validate_property_filter_keys,
)
from forze.application.contracts.resolution import (
    NamedResourceSpec,
    is_static_named_resource,
    resolve_scoped_namespace,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.graph import (
    GraphCodecs,
    GraphKindCipher,
    assert_edge_streamable,
    assert_vertex_streamable,
    resolve_write_endpoint,
    stream_keyset_pages,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict, OnceCell, uuid4
from forze.base.serialization import default_model_codec

from ..kernel.client import Neo4jClientPort
from ..kernel.cypher import builders
from ..kernel.relation import resolve_neo4j_database
from ._logger import logger

# ----------------------- #

# Initial extra Yen's candidates fetched beyond the requested ``k`` for weighted paths, so the
# common case (few over-long cheaper paths) resolves in one round-trip. This is only a head-start,
# not a cap: ``_weighted_paths`` grows the window until it has ``k`` paths within ``max_hops`` or
# Yen's is exhausted, so a valid bounded path is never dropped for hiding behind more than the
# buffer's worth of cheaper over-long ones.
_WEIGHTED_HOP_CANDIDATE_BUFFER = 32

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Neo4jGraphAdapter(TenancyMixin):
    """Neo4j-backed adapter for a single :class:`GraphModuleSpec`.

    Inherits ``tenant_aware`` / ``tenant_provider`` from :class:`TenancyMixin`; tenancy
    is enforced by stamping/matching :attr:`tenant_property` on anchor nodes.
    """

    spec: GraphModuleSpec
    client: Neo4jClientPort
    codecs: GraphCodecs | None = None
    """Per-kind property-map ciphers (one :class:`GraphKindCipher` per node/edge kind). The
    factory resolves these from the wired keyring; ``None`` falls back to plaintext codecs (so
    a module with no ``encryption`` policy needs no crypto wiring)."""
    tenant_property: str = "tenant_id"
    database: NamedResourceSpec | None = None
    """Target Neo4j database — a static name, a per-tenant resolver (``namespace`` tier:
    per-tenant database on a shared cluster), or ``None`` (client default)."""

    _database_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    traversal_isolation: Literal["anchor", "full-path"] = "full-path"
    """How far tenant scoping reaches on traversals when ``tenant_aware``.

    ``full-path`` (default) constrains every node on a ``neighbors``/``expand``/
    ``shortest_path`` result, so a cross-tenant edge cannot surface a foreign node.
    ``anchor`` constrains only the start/endpoint nodes — cheaper, but safe only under the
    invariant that no edge ever crosses a tenant boundary.
    """

    allow_raw_query: bool = False
    """Whether the whole-query raw hatch :meth:`run` is permitted.

    The raw hatch is a **trusted-caller** escape: the caller writes the entire Cypher, so a
    buggy or hostile query can read cross-tenant even though ``$tenant`` is bound. It is
    therefore **disabled by default** (fail closed, code ``graph_raw_disabled``); set this
    ``True`` to opt in where trusted raw Cypher is genuinely needed — otherwise use the
    structured ports (full-path scoped) and :meth:`scoped_walk` instead.
    """

    graph_algorithms: bool = False
    """Whether weighted-path queries may use the GDS engine (opt-in); see
    :attr:`~forze_neo4j.execution.deps.configs.Neo4jGraphConfig.graph_algorithms`."""

    # ....................... #
    # tenancy / database resolution

    async def _resolved_database(self) -> str | None:
        """Target Neo4j database for the current tenant (``None`` = client default).

        A static name (or ``None``) resolves without a tenant; a per-tenant resolver scopes
        each query to the tenant's own database (the ``namespace`` tier).
        """

        spec = self.database

        if spec is None or is_static_named_resource(spec):
            return spec

        return await resolve_scoped_namespace(
            spec,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._database_cell,
            resolver=resolve_neo4j_database,
        )

    # ....................... #
    # spec / codec helpers

    def _node(self, kind: str) -> GraphNodeSpec[BaseModel]:
        node = self.spec.graph_node_by_kind(kind)

        if node is None:
            raise exc.configuration(
                f"Unknown graph node kind {kind!r} in module {self.spec.name!r}",
                code="graph_unknown_node_kind",
            )

        return node

    # ....................... #

    def _edge(self, kind: str) -> GraphEdgeSpec[BaseModel]:
        edge = self.spec.graph_edge_by_kind(kind)

        if edge is None:
            raise exc.configuration(
                f"Unknown graph edge kind {kind!r} in module {self.spec.name!r}",
                code="graph_unknown_edge_kind",
            )

        return edge

    # ....................... #

    def _node_kind_from_labels(self, labels: Sequence[str]) -> str:
        for label in labels:
            if self.spec.graph_node_by_kind(label) is not None:
                return label

        raise exc.infrastructure(
            f"Returned node has no label matching a known node kind: {list(labels)!r}",
            code="graph_unmapped_node_labels",
        )

    # ....................... #

    def _strip_internal(self, props: JsonDict) -> JsonDict:
        """Drop adapter-internal properties (the tenant tag) not in the read model."""

        if self.tenant_aware and self.tenant_property in props:
            return {k: v for k, v in props.items() if k != self.tenant_property}

        return props

    # ....................... #

    def _node_cipher(self, kind: str) -> GraphKindCipher:
        """Per-kind property-map cipher for a node kind (plaintext when unwired)."""

        node = self._node(kind)

        if self.codecs is not None:
            return self.codecs.node(kind)

        return GraphKindCipher(read_codec=default_model_codec(node.read), cipher=None)

    # ....................... #

    def _edge_cipher(self, kind: str) -> GraphKindCipher:
        """Per-kind property-map cipher for an edge kind (plaintext when unwired)."""

        edge = self._edge(kind)

        if self.codecs is not None:
            return self.codecs.edge(kind)

        return GraphKindCipher(read_codec=default_model_codec(edge.read), cipher=None)

    # ....................... #

    async def _vertex_model(self, kind: str, props: JsonDict) -> BaseModel:
        return await self._node_cipher(kind).open(self._strip_internal(props))

    # ....................... #

    async def _edge_model(self, kind: str, props: JsonDict) -> BaseModel:
        return await self._edge_cipher(kind).open(self._strip_internal(props))

    # ....................... #
    # property-filter helpers (count / find)

    @staticmethod
    def _sealed_fields(encryption: Any) -> frozenset[str]:
        if encryption is None:
            return frozenset()

        return encryption.encrypted | encryption.searchable

    def _filter_params(self, property_filter: JsonDict | None, sealed: frozenset[str]) -> JsonDict:
        """Validate an equality filter and render it to ``$pf_<key>`` params.

        Rejects a filter on a sealed (encrypted) property — its stored value is ciphertext,
        so an equality match against a plaintext value can never be correct. Also rejects a
        non-identifier key: it is embedded in the ``$pf_<key>`` parameter name, which cannot
        be backtick-quoted, so anything else must fail closed before a query is built.
        """

        if not property_filter:
            return {}

        validate_property_filter_keys(property_filter)

        blocked = sorted(k for k in property_filter if k in sealed)

        if blocked:
            raise exc.precondition(
                f"Cannot filter on encrypted graph properties {blocked} (sealed at rest); "
                "filter on a plaintext property instead.",
                code="graph_filter_on_encrypted_field",
            )

        return {f"pf_{k}": v for k, v in property_filter.items()}

    # ....................... #
    # tenancy helpers

    @property
    def _tenant_field(self) -> str | None:
        return self.tenant_property if self.tenant_aware else None

    # ....................... #

    @property
    def _interior_scope(self) -> bool:
        """Whether traversals also constrain interior/terminal nodes to the tenant."""

        return self.tenant_aware and self.traversal_isolation == "full-path"

    # ....................... #

    def _tenant_str(self) -> str | None:
        tid = self.require_tenant_if_aware()
        return str(tid) if tid is not None else None

    # ....................... #

    def _params(self, **extra: Any) -> JsonDict:
        params: JsonDict = dict(extra)

        if self.tenant_aware:
            params["tenant"] = self._tenant_str()

        return params

    # ....................... #

    async def _encode(
        self,
        cmd: BaseModel,
        cipher: GraphKindCipher,
        *,
        record_id: Any = None,
    ) -> JsonDict:
        data: JsonDict = cmd.model_dump(mode="json", exclude_none=True)
        data = await cipher.seal(data, record_id=record_id)

        if self.tenant_aware:
            data[self.tenant_property] = self._tenant_str()

        return data

    # ....................... #
    # GraphQueryPort

    async def get_vertex(self, ref: VertexRef) -> BaseModel | None:
        node = self._node(ref.kind)
        query = builders.get_vertex(ref.kind, node.key_field, tenant_field=self._tenant_field)
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

        if not rows:
            return None

        return await self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def vertex_exists(self, ref: VertexRef) -> bool:
        node = self._node(ref.kind)
        query = builders.vertex_exists(ref.kind, node.key_field, tenant_field=self._tenant_field)
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

        return bool(rows and rows[0]["exists"])

    # ....................... #

    async def get_edge(self, ref: EdgeRef) -> BaseModel | None:
        database = await self._resolved_database()

        if ref.is_keyed:
            edge = self._edge(ref.kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {ref.kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.get_edge_by_key(
                ref.kind, edge.key_field, tenant_field=self._tenant_field
            )
            params = self._params(key=ref.key)

        else:
            query, params = self._endpoints_edge_query(ref, builders.get_edge_by_endpoints)

        rows = await self.client.run(query, params, database=database)

        if not rows:
            return None

        return await self._edge_model(ref.kind, rows[0]["r"])

    def _endpoints_edge_query(self, ref: EdgeRef, builder: Any) -> tuple[str, JsonDict]:
        """Build an endpoints-mode edge query (shared by get_edge / edge_exists / delete)."""

        if ref.from_ref is None or ref.to_ref is None:
            raise exc.configuration(
                f"Endpoints EdgeRef for {ref.kind!r} must carry from_ref and to_ref",
                code="graph_edge_missing_endpoints",
            )

        from_node = self._node(ref.from_ref.kind)
        to_node = self._node(ref.to_ref.kind)
        query = builder(
            edge_type=ref.kind,
            from_label=ref.from_ref.kind,
            from_key_field=from_node.key_field,
            to_label=ref.to_ref.kind,
            to_key_field=to_node.key_field,
            tenant_field=self._tenant_field,
        )

        return query, self._params(from_key=ref.from_ref.key, to_key=ref.to_ref.key)

    # ....................... #

    async def neighbors(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
        to_vertex_kinds: frozenset[str] | None = None,
    ) -> Sequence[NeighborRow]:
        node = self._node(origin.kind)
        query = builders.neighbors(
            label=origin.kind,
            key_field=node.key_field,
            direction=direction,
            edge_types=edge_kinds,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(key=origin.key, limit=limit),
            database=await self._resolved_database(),
        )

        out: list[NeighborRow] = []

        for row in rows:
            other_kind = self._node_kind_from_labels(row["other_labels"])

            if to_vertex_kinds is not None and other_kind not in to_vertex_kinds:
                continue

            out.append(
                NeighborRow(
                    other=await self._vertex_model(other_kind, row["other"]),
                    via_edge=await self._edge_model(row["via_type"], row["via_edge"]),
                    direction=direction,
                )
            )

        return out

    # ....................... #

    async def expand(
        self,
        start: VertexRef,
        params: GraphWalkParams,
    ) -> Sequence[GraphWalkStep]:
        node = self._node(start.kind)
        query = builders.expand(
            label=start.kind,
            key_field=node.key_field,
            direction=params.direction,
            edge_types=params.edge_kinds,
            max_depth=params.max_depth,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(key=start.key, max_results=params.max_results),
            database=await self._resolved_database(),
        )

        out: list[GraphWalkStep] = []

        for row in rows:
            vertex_kind = self._node_kind_from_labels(row["vertex_labels"])
            parent_labels: list[str] = row.get("parent_labels") or []
            parent_ref: VertexRef | None = None
            from_parent: BaseModel | None = None

            if parent_labels and row.get("parent"):
                parent_kind = self._node_kind_from_labels(parent_labels)
                parent_props = row["parent"]
                parent_ref = VertexRef(
                    kind=parent_kind,
                    key=str(parent_props[self._node(parent_kind).key_field]),
                )

            if row.get("from_parent") and row.get("from_parent_type"):
                from_parent = await self._edge_model(row["from_parent_type"], row["from_parent"])

            out.append(
                GraphWalkStep(
                    depth=row["depth"],
                    vertex=await self._vertex_model(vertex_kind, row["vertex"]),
                    from_parent=from_parent,
                    parent_ref=parent_ref,
                )
            )

        return out

    # ....................... #

    async def shortest_path(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
    ) -> ShortestPathResult | None:
        if params.weight_property is not None:
            # Weighted single shortest path = Yen's with k=1.
            weighted = await self._weighted_paths(from_ref, to_ref, params, k=1)
            return weighted[0] if weighted else None

        from_node = self._node(from_ref.kind)
        to_node = self._node(to_ref.kind)
        query = builders.shortest_path(
            from_label=from_ref.kind,
            from_key_field=from_node.key_field,
            to_label=to_ref.kind,
            to_key_field=to_node.key_field,
            direction=GraphDirection.OUT,
            edge_types=params.edge_kinds,
            max_hops=params.max_hops,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(from_key=from_ref.key, to_key=to_ref.key),
            database=await self._resolved_database(),
        )

        if not rows:
            return None

        return await self._map_path_row(rows[0])

    # ....................... #

    async def _map_path_row(self, row: JsonDict) -> ShortestPathResult:
        """Materialize one path row (parallel vertex/edge property lists) into models."""

        vertices = tuple(
            [
                await self._vertex_model(self._node_kind_from_labels(labels), props)
                for props, labels in zip(row["vertices"], row["vertex_labels"], strict=True)
            ]
        )
        edges = tuple(
            [
                await self._edge_model(edge_type, props)
                for props, edge_type in zip(row["edges"], row["edge_types"], strict=True)
            ]
        )

        return ShortestPathResult(vertices=vertices, edges=edges)

    # ....................... #
    # Weighted paths via GDS

    def _weighted_edge_types(self, params: ShortestPathParams) -> frozenset[str]:
        """Relationship types to project — the requested kinds, or all module edge kinds."""

        if params.edge_kinds:
            return params.edge_kinds

        return frozenset(str(edge.name) for edge in self.spec.edges)

    async def _ensure_gds_available(self) -> None:
        """Fail closed unless the GDS engine is both opted in and actually installed."""

        if not self.graph_algorithms:
            raise exc.precondition(
                "Weighted paths need the graph-algorithms engine: set graph_algorithms=True "
                "on the Neo4j graph config and install Neo4j GDS.",
                code="graph_algorithm_unavailable",
            )

        try:
            await self.client.run(
                "CALL gds.version()", None, database=await self._resolved_database()
            )

        except CoreException as err:
            raise exc.precondition(
                "graph_algorithms is enabled but Neo4j GDS is not installed on the server.",
                code="graph_algorithm_unavailable",
            ) from err

    async def _weighted_paths(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
        *,
        k: int,
    ) -> list[ShortestPathResult]:
        await self._ensure_gds_available()

        weight = params.weight_property

        if weight is None:  # dispatched only for weighted params; narrows the type
            raise exc.internal("weighted path requested without a weight_property")

        from_node = self._node(from_ref.kind)
        to_node = self._node(to_ref.kind)
        edge_types = self._weighted_edge_types(params)
        database = await self._resolved_database()

        # A per-call named projection (GDS catalog graphs are DB-global, so a unique name
        # avoids collisions and the ``finally`` drop keeps the catalog from leaking).
        graph_name = f"forze_gds_{uuid4().hex}"

        project = builders.gds_project_weighted(
            edge_types=edge_types,
            weight_property=weight,
            tenant_field=self._tenant_field,
        )
        query = builders.gds_weighted_paths(
            from_label=from_ref.kind,
            from_key_field=from_node.key_field,
            to_label=to_ref.kind,
            to_key_field=to_node.key_field,
            edge_types=edge_types,
            weight_property=weight,
            tenant_field=self._tenant_field,
        )

        # Yen's ranks by cost with no hop limit, so the cheapest candidates may all exceed
        # ``max_hops``. Fetch a cost-ordered window (each row reports its ``hops``) and grow it
        # until we have ``k`` paths within the bound or Yen's is exhausted — a *fixed* over-fetch
        # would drop a valid bounded path hiding behind more than the buffer's worth of cheaper
        # over-long ones. The buffer is only the initial head-start (one round-trip in the common
        # case), not a cap.
        candidate_k = k + _WEIGHTED_HOP_CANDIDATE_BUFFER
        bounded: list[JsonDict] = []

        try:
            await self.client.run(project, self._params(graph_name=graph_name), database=database)
            while True:
                rows = await self.client.run(
                    query,
                    self._params(
                        from_key=from_ref.key,
                        to_key=to_ref.key,
                        graph_name=graph_name,
                        candidate_k=candidate_k,
                        max_hops=params.max_hops,
                    ),
                    database=database,
                )
                # Rows are cost-ordered; keep the ones within the hop bound.
                bounded = [row for row in rows if row["hops"] <= params.max_hops]

                # Enough within the bound, or Yen's returned fewer candidates than asked (so no
                # further paths exist to grow into) — either way we have the cheapest bounded set.
                if len(bounded) >= k or len(rows) < candidate_k:
                    break

                candidate_k *= 2

        finally:
            try:
                await self.client.run(
                    builders.gds_drop(), {"graph_name": graph_name}, database=database
                )
            except CoreException:
                # Best-effort cleanup — never mask the real result/error with a drop failure.
                logger.debug("Failed to drop GDS projection %s", graph_name)

        return [await self._map_path_row(row) for row in bounded[:k]]

    # ....................... #

    async def scoped_walk(
        self,
        anchor: VertexRef,
        params: ScopedWalkParams,
    ) -> Sequence[BaseModel]:
        anchor_node = self._node(anchor.kind)
        self._node(params.target_kind)  # validate the target kind is in the spec

        segments = [
            (step.direction, step.edge_kinds, step.min_hops, step.max_hops) for step in params.steps
        ]
        query = builders.scoped_walk(
            anchor_label=anchor.kind,
            anchor_key_field=anchor_node.key_field,
            segments=segments,
            target_label=params.target_kind,
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query,
            self._params(key=anchor.key, limit=params.limit),
            database=await self._resolved_database(),
        )

        return [await self._vertex_model(params.target_kind, row["m"]) for row in rows]

    # ....................... #
    # GraphCommandPort

    async def create_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        query = builders.create_vertex(node_kind)
        props = await self._encode(cmd, self._node_cipher(node_kind))
        rows = await self.client.run(
            query,
            {"props": props, **self._params()},
            database=await self._resolved_database(),
        )

        if not return_new:
            return None

        return await self._vertex_model(node_kind, rows[0]["n"])

    # ....................... #

    async def update_vertex(self, ref: VertexRef, cmd: BaseModel) -> BaseModel:
        node = self._node(ref.kind)
        query = builders.update_vertex(ref.kind, node.key_field, tenant_field=self._tenant_field)
        props = await self._encode(cmd, self._node_cipher(ref.kind), record_id=ref.key)
        rows = await self.client.run(
            query,
            {"props": props, **self._params(key=ref.key)},
            database=await self._resolved_database(),
        )

        if not rows:
            raise exc.not_found(
                f"Vertex {ref.kind}:{ref.key} not found",
                code="graph_vertex_not_found",
            )

        return await self._vertex_model(ref.kind, rows[0]["n"])

    # ....................... #

    async def delete_vertex(self, ref: VertexRef) -> None:
        node = self._node(ref.kind)
        query = builders.delete_vertex(ref.kind, node.key_field, tenant_field=self._tenant_field)
        await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )

    # ....................... #

    async def create_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(edge_kind, cmd, merge=False, return_new=return_new)

    # ....................... #

    async def ensure_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        return await self._write_edge(edge_kind, cmd, merge=True, return_new=return_new)

    # ....................... #

    async def _write_edge(
        self,
        edge_kind: str,
        cmd: BaseModel,
        *,
        merge: bool,
        return_new: bool,
    ) -> BaseModel | None:
        edge = self._edge(edge_kind)

        data = await self._encode(cmd, self._edge_cipher(edge_kind))
        from_key = data.pop("from_key", None)
        to_key = data.pop("to_key", None)
        # Resolve the endpoint pair (single kinds are implicit; multi-endpoint kinds name it
        # via from_kind/to_kind in the command). Pops the routing hints from ``data``.
        endpoint = resolve_write_endpoint(edge, data)
        from_node = self._node(endpoint.from_kind)
        to_node = self._node(endpoint.to_kind)

        if from_key is None or to_key is None:
            raise exc.validation(
                f"Edge create command for {edge_kind!r} must include 'from_key' and 'to_key'",
                code="graph_edge_endpoints_required",
            )

        # A keyed edge kind is identified by its key property: an ``ensure`` (MERGE)
        # must match on that key so two distinct keyed edges between the same pair
        # stay separate. A keyless MERGE matches any edge of the type and collapses
        # them.
        edge_key = None

        if merge and edge.key_field is not None:
            edge_key = data.get(edge.key_field)

            if edge_key is None:
                raise exc.validation(
                    f"Keyed edge command for {edge_kind!r} must include "
                    f"{edge.key_field!r} to ensure a stable identity",
                    code="graph_edge_key_required",
                )

        query = builders.create_edge(
            from_label=endpoint.from_kind,
            from_key_field=from_node.key_field,
            to_label=endpoint.to_kind,
            to_key_field=to_node.key_field,
            edge_type=edge_kind,
            merge=merge,
            tenant_field=self._tenant_field,
            key_field=edge.key_field if merge else None,
        )
        params = {"props": data, **self._params(from_key=from_key, to_key=to_key)}

        if edge_key is not None:
            params["edge_key"] = edge_key

        rows = await self.client.run(
            query,
            params,
            database=await self._resolved_database(),
        )

        if not rows:
            raise exc.not_found(
                f"Edge endpoints for {edge_kind!r} not found ({from_key} -> {to_key})",
                code="graph_edge_endpoints_not_found",
            )

        if not return_new:
            return None

        return await self._edge_model(edge_kind, rows[0]["r"])

    # ....................... #
    # GraphRawQueryPort

    async def run(self, query: str, params: JsonDict | None = None) -> Sequence[JsonDict]:
        # The whole-query raw hatch is trusted-caller by construction; a deployment that
        # requires enforced tenancy disables it (``allow_raw_query=False``) and uses the
        # structured ports / ``scoped_walk`` instead.
        if not self.allow_raw_query:
            raise exc.configuration(
                f"Raw graph queries are disabled for module {self.spec.name!r} "
                "(allow_raw_query=False); use the structured ports or scoped_walk.",
                code="graph_raw_disabled",
            )

        # Tenant-aware raw queries fail closed: ``_tenant_str`` →
        # ``require_tenant_if_aware`` raises if no tenant is bound (was: silent
        # cross-tenant access). The framework tenant is bound as ``$tenant`` (authoritative
        # over any caller-supplied key) so the query can ``MATCH (... {tenant_id: $tenant})``.
        merged = dict(params or {})

        if self.tenant_aware:
            merged["tenant"] = self._tenant_str()

        return await self.client.run(
            query, merged or None, database=await self._resolved_database()
        )

    # ....................... #
    # GraphManagementPort — schema provisioning

    def _schema_name(self, kind: str, subject: str, field: str) -> str:
        # Deterministic (no hashing — a name must match across processes for drop_schema):
        # sanitize to Neo4j-safe identifier chars and scope by module so two modules sharing
        # a label on one database do not collide.
        raw = f"{self.spec.name}_{kind}_{subject}_{field}"
        safe = "".join(c if c.isalnum() else "_" for c in raw)
        return f"forze_{safe}"

    def _schema_plan(self) -> list[tuple[str, str]]:
        """``(create_cypher, drop_cypher)`` for every constraint/index this module needs."""

        tenant_field = self._tenant_field
        plan: list[tuple[str, str]] = []

        for node in self.spec.nodes:
            label = str(node.name)
            nk = self._schema_name("nk", label, node.key_field)
            plan.append(
                (
                    builders.node_uniqueness_constraint(
                        nk, label, node.key_field, tenant_field=tenant_field
                    ),
                    builders.drop_constraint(nk),
                )
            )

            if tenant_field is not None:
                nt = self._schema_name("nt", label, tenant_field)
                plan.append(
                    (
                        builders.property_index(nt, label, tenant_field),
                        builders.drop_index(nt),
                    )
                )

        for edge in self.spec.edges:
            if edge.identity == "key" and edge.key_field is not None:
                etype = str(edge.name)
                ek = self._schema_name("ek", etype, edge.key_field)
                plan.append(
                    (
                        builders.edge_uniqueness_constraint(
                            ek, etype, edge.key_field, tenant_field=tenant_field
                        ),
                        builders.drop_constraint(ek),
                    )
                )

        return plan

    async def ensure_schema(self) -> None:
        # Schema commands run one per statement in their own auto-commit (Neo4j forbids
        # mixing schema and data in one transaction) — never inside a caller transaction.
        database = await self._resolved_database()

        for create_cypher, _drop in self._schema_plan():
            await self.client.run(create_cypher, None, database=database)

    async def drop_schema(self) -> None:
        database = await self._resolved_database()

        for _create, drop_cypher in self._schema_plan():
            await self.client.run(drop_cypher, None, database=database)

    # ....................... #
    # Deferred GraphQueryPort methods

    async def get_vertices(self, refs: Sequence[VertexRef]) -> Sequence[BaseModel]:
        if not refs:
            return []

        database = await self._resolved_database()
        by_kind: dict[str, list[str]] = {}

        for ref in refs:
            by_kind.setdefault(ref.kind, []).append(ref.key)

        found: dict[tuple[str, str], BaseModel] = {}

        for kind, keys in by_kind.items():
            node = self._node(kind)
            query = builders.get_vertices_by_keys(
                kind, node.key_field, tenant_field=self._tenant_field
            )
            rows = await self.client.run(
                query, self._params(keys=list(set(keys))), database=database
            )

            for row in rows:
                found[(kind, str(row["_key"]))] = await self._vertex_model(kind, row["n"])

        # Input order, found-only (missing refs omitted — batch-get semantics).
        return [
            found[(ref.kind, str(ref.key))] for ref in refs if (ref.kind, str(ref.key)) in found
        ]

    async def get_edges(self, refs: Sequence[EdgeRef]) -> Sequence[BaseModel]:
        if not refs:
            return []

        database = await self._resolved_database()
        indexed: list[tuple[int, BaseModel]] = []
        keyed_by_kind: dict[str, list[tuple[int, str]]] = {}

        for i, ref in enumerate(refs):
            if ref.is_keyed and ref.key is not None:
                keyed_by_kind.setdefault(ref.kind, []).append((i, ref.key))
            else:
                # Endpoints-mode edges are matched one pair at a time.
                model = await self.get_edge(ref)
                if model is not None:
                    indexed.append((i, model))

        for kind, items in keyed_by_kind.items():
            edge = self._edge(kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.get_edges_by_keys(
                kind, edge.key_field, tenant_field=self._tenant_field
            )
            rows = await self.client.run(
                query,
                self._params(keys=[k for _, k in items]),
                database=database,
            )
            by_key = {str(row["_key"]): row["r"] for row in rows}

            for i, key in items:
                props = by_key.get(str(key))
                if props is not None:
                    indexed.append((i, await self._edge_model(kind, props)))

        indexed.sort(key=lambda pair: pair[0])
        return [model for _, model in indexed]

    async def edge_exists(self, ref: EdgeRef) -> bool:
        database = await self._resolved_database()

        if ref.is_keyed:
            edge = self._edge(ref.kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {ref.kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.edge_exists_by_key(
                ref.kind, edge.key_field, tenant_field=self._tenant_field
            )
            params = self._params(key=ref.key)

        else:
            query, params = self._endpoints_edge_query(ref, builders.edge_exists_by_endpoints)

        rows = await self.client.run(query, params, database=database)
        return bool(rows and rows[0]["exists"])

    async def count_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
    ) -> int:
        node = self._node(node_kind)
        params = self._filter_params(property_filter, self._sealed_fields(node.encryption))
        query = builders.count_vertices(
            node_kind,
            tenant_field=self._tenant_field,
            filter_keys=list(property_filter or {}),
        )
        rows = await self.client.run(
            query, self._params(**params), database=await self._resolved_database()
        )
        return int(rows[0]["c"]) if rows else 0

    async def count_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
    ) -> int:
        edge = self._edge(edge_kind)
        params = self._filter_params(property_filter, self._sealed_fields(edge.encryption))
        query = builders.count_edges(
            edge_kind,
            tenant_field=self._tenant_field,
            filter_keys=list(property_filter or {}),
        )
        rows = await self.client.run(
            query, self._params(**params), database=await self._resolved_database()
        )
        return int(rows[0]["c"]) if rows else 0

    async def incident_edges(
        self,
        origin: VertexRef,
        direction: GraphDirection,
        edge_kinds: frozenset[str],
        *,
        limit: int,
    ) -> Sequence[BaseModel]:
        node = self._node(origin.kind)
        query = builders.incident_edges(
            origin.kind,
            node.key_field,
            direction,
            edge_kinds,
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query,
            self._params(key=origin.key, limit=limit),
            database=await self._resolved_database(),
        )

        return [await self._edge_model(row["t"], row["r"]) for row in rows]

    async def k_shortest_paths(
        self,
        from_ref: VertexRef,
        to_ref: VertexRef,
        params: ShortestPathParams,
        *,
        k: int,
    ) -> Sequence[ShortestPathResult]:
        if k <= 0:
            return []

        if params.weight_property is not None:
            return await self._weighted_paths(from_ref, to_ref, params, k=k)

        from_node = self._node(from_ref.kind)
        to_node = self._node(to_ref.kind)
        query = builders.k_shortest_paths(
            from_label=from_ref.kind,
            from_key_field=from_node.key_field,
            to_label=to_ref.kind,
            to_key_field=to_node.key_field,
            direction=GraphDirection.OUT,
            edge_types=params.edge_kinds,
            max_hops=params.max_hops,
            k=k,
            tenant_field=self._tenant_field,
            interior=self._interior_scope,
        )
        rows = await self.client.run(
            query,
            self._params(from_key=from_ref.key, to_key=to_ref.key),
            database=await self._resolved_database(),
        )

        return [await self._map_path_row(row) for row in rows]

    async def find_vertices(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[BaseModel]:
        node = self._node(node_kind)
        params = self._filter_params(property_filter, self._sealed_fields(node.encryption))
        query = builders.find_vertices(
            node_kind,
            node.key_field,
            tenant_field=self._tenant_field,
            filter_keys=list(property_filter or {}),
        )
        rows = await self.client.run(
            query,
            self._params(offset=offset, limit=limit, **params),
            database=await self._resolved_database(),
        )
        return [await self._vertex_model(node_kind, row["n"]) for row in rows]

    async def find_edges(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Sequence[BaseModel]:
        edge = self._edge(edge_kind)
        params = self._filter_params(property_filter, self._sealed_fields(edge.encryption))
        query = builders.find_edges(
            edge_kind,
            order_field=edge.key_field,  # stable order for keyed edges; unordered otherwise
            tenant_field=self._tenant_field,
            filter_keys=list(property_filter or {}),
        )
        rows = await self.client.run(
            query,
            self._params(offset=offset, limit=limit, **params),
            database=await self._resolved_database(),
        )
        return [await self._edge_model(edge_kind, row["r"]) for row in rows]

    # ....................... #

    def read_capabilities(self) -> GraphReadCapabilities:
        # Both streams are a keyset seek over an indexed key field, which Cypher expresses
        # directly — so Neo4j supports both.
        return GraphReadCapabilities(
            supports_vertex_streaming=True,
            supports_edge_streaming=True,
        )

    # ....................... #

    def find_vertices_stream(
        self,
        node_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        node = self._node(node_kind)
        key_field = assert_vertex_streamable(
            node, kind=node_kind, capabilities=self.read_capabilities()
        )
        filter_params = self._filter_params(property_filter, self._sealed_fields(node.encryption))
        filter_keys = list(property_filter or {})

        async def _fetch(after: Any | None, limit: int) -> Sequence[tuple[Any, BaseModel]]:
            query = builders.find_vertices_keyset(
                node_kind,
                key_field,
                after=after is not None,
                tenant_field=self._tenant_field,
                filter_keys=filter_keys,
            )
            params = self._params(limit=limit, **filter_params)

            if after is not None:
                params["after"] = after

            rows = await self.client.run(query, params, database=await self._resolved_database())

            # The bookmark is read off the **raw** property map, before decoding — the stored
            # value is what the next query's seek predicate is compared against.
            return [
                (row["n"].get(key_field), await self._vertex_model(node_kind, row["n"]))
                for row in rows
            ]

        return stream_keyset_pages(_fetch, chunk_size=chunk_size)

    # ....................... #

    def find_edges_stream(
        self,
        edge_kind: str,
        *,
        property_filter: JsonDict | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[BaseModel]]:
        edge = self._edge(edge_kind)
        key_field = assert_edge_streamable(
            edge, kind=edge_kind, capabilities=self.read_capabilities()
        )
        filter_params = self._filter_params(property_filter, self._sealed_fields(edge.encryption))
        filter_keys = list(property_filter or {})

        async def _fetch(after: Any | None, limit: int) -> Sequence[tuple[Any, BaseModel]]:
            query = builders.find_edges_keyset(
                edge_kind,
                key_field,
                after=after is not None,
                tenant_field=self._tenant_field,
                filter_keys=filter_keys,
            )
            params = self._params(limit=limit, **filter_params)

            if after is not None:
                params["after"] = after

            rows = await self.client.run(query, params, database=await self._resolved_database())

            return [
                (row["r"].get(key_field), await self._edge_model(edge_kind, row["r"]))
                for row in rows
            ]

        return stream_keyset_pages(_fetch, chunk_size=chunk_size)

    async def vertex_degree(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        node = self._node(ref.kind)
        query = builders.vertex_degree(
            ref.kind,
            node.key_field,
            direction,
            edge_kinds or frozenset(),
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )
        return int(rows[0]["c"]) if rows else 0

    async def count_neighbors(
        self,
        ref: VertexRef,
        *,
        direction: GraphDirection = GraphDirection.BOTH,
        edge_kinds: frozenset[str] | None = None,
    ) -> int:
        node = self._node(ref.kind)
        query = builders.count_neighbors(
            ref.kind,
            node.key_field,
            direction,
            edge_kinds or frozenset(),
            tenant_field=self._tenant_field,
        )
        rows = await self.client.run(
            query, self._params(key=ref.key), database=await self._resolved_database()
        )
        return int(rows[0]["c"]) if rows else 0

    # ....................... #
    # Deferred GraphCommandPort methods

    async def update_edge(self, ref: EdgeRef, cmd: BaseModel) -> BaseModel:
        props = await self._encode(
            cmd,
            self._edge_cipher(ref.kind),
            record_id=ref.key if ref.is_keyed else None,
        )
        # An update patches the edge's own properties — it can't move endpoints or rekey.
        props.pop("from_key", None)
        props.pop("to_key", None)

        if ref.is_keyed:
            edge = self._edge(ref.kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {ref.kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.update_edge_by_key(
                ref.kind, edge.key_field, tenant_field=self._tenant_field
            )
            params = {"props": props, **self._params(key=ref.key)}

        else:
            query, base = self._endpoints_edge_query(ref, builders.update_edge_by_endpoints)
            params = {"props": props, **base}

        rows = await self.client.run(query, params, database=await self._resolved_database())

        if not rows:
            raise exc.not_found(f"Edge {ref.kind!r} not found", code="graph_edge_not_found")

        return await self._edge_model(ref.kind, rows[0]["r"])

    async def delete_edge(self, ref: EdgeRef) -> None:
        if ref.is_keyed:
            edge = self._edge(ref.kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {ref.kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.delete_edge_by_key(
                ref.kind, edge.key_field, tenant_field=self._tenant_field
            )
            params = self._params(key=ref.key)

        else:
            query, params = self._endpoints_edge_query(ref, builders.delete_edge_by_endpoints)

        await self.client.run(query, params, database=await self._resolved_database())

    async def create_vertices(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Sequence[BaseModel] | None:
        if not items:
            return [] if return_new else None

        database = await self._resolved_database()
        by_kind: dict[str, list[tuple[int, JsonDict]]] = {}

        for i, (kind, cmd) in enumerate(items):
            props = await self._encode(cmd, self._node_cipher(kind))
            by_kind.setdefault(kind, []).append((i, props))

        results: dict[int, BaseModel] = {}

        for kind, entries in by_kind.items():
            query = builders.create_vertices(kind)
            rows = await self.client.run(
                query,
                {"rows": [props for _, props in entries], **self._params()},
                database=database,
            )

            # Skip the per-row decode/decrypt unless the caller wants the models back — the
            # insert above is the only work a ``return_new=False`` bulk create needs.
            if return_new:
                for (idx, _props), row in zip(entries, rows, strict=True):
                    results[idx] = await self._vertex_model(kind, row["n"])

        if not return_new:
            return None

        return [results[i] for i in range(len(items))]

    async def create_edges(
        self,
        items: Sequence[tuple[str, BaseModel]],
        *,
        return_new: bool = True,
    ) -> Sequence[BaseModel] | None:
        # Edges need per-item endpoint matching (encode + tenant + not-found), so this reuses
        # single-edge create rather than one UNWIND — order-preserving, not one round-trip.
        if not items:
            return [] if return_new else None

        created: list[BaseModel] = []

        for kind, cmd in items:
            edge = await self.create_edge(kind, cmd, return_new=return_new)
            if return_new and edge is not None:
                created.append(edge)

        return created if return_new else None

    async def ensure_vertex(
        self,
        node_kind: str,
        cmd: BaseModel,
        *,
        return_new: bool = True,
    ) -> BaseModel | None:
        node = self._node(node_kind)
        props = await self._encode(cmd, self._node_cipher(node_kind))
        key = str(props[node.key_field])  # the key field is the plaintext identity
        query = builders.ensure_vertex(node_kind, node.key_field, tenant_field=self._tenant_field)
        rows = await self.client.run(
            query,
            {"props": props, **self._params(key=key)},
            database=await self._resolved_database(),
        )

        if not return_new:
            return None

        return await self._vertex_model(node_kind, rows[0]["n"])

    async def delete_vertices(self, refs: Sequence[VertexRef]) -> None:
        if not refs:
            return

        database = await self._resolved_database()
        by_kind: dict[str, list[str]] = {}

        for ref in refs:
            by_kind.setdefault(ref.kind, []).append(ref.key)

        for kind, keys in by_kind.items():
            node = self._node(kind)
            query = builders.delete_vertices(kind, node.key_field, tenant_field=self._tenant_field)
            await self.client.run(query, self._params(keys=list(set(keys))), database=database)

    async def delete_edges(self, refs: Sequence[EdgeRef]) -> None:
        if not refs:
            return

        database = await self._resolved_database()
        keyed_by_kind: dict[str, list[str]] = {}

        for ref in refs:
            if ref.is_keyed and ref.key is not None:
                keyed_by_kind.setdefault(ref.kind, []).append(ref.key)
            else:
                await self.delete_edge(ref)  # endpoints, one pair at a time

        for kind, keys in keyed_by_kind.items():
            edge = self._edge(kind)

            if edge.key_field is None:
                raise exc.configuration(
                    f"Edge kind {kind!r} has no key_field but a keyed EdgeRef was used",
                    code="graph_edge_missing_key_field",
                )

            query = builders.delete_edges_by_keys(
                kind, edge.key_field, tenant_field=self._tenant_field
            )
            await self.client.run(query, self._params(keys=keys), database=database)
