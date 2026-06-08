# Neo4j (graph)

`forze_neo4j` implements the Forze graph contracts
([`forze.application.contracts.graph`](../core-package/contracts.md)) on Neo4j via the
official async Bolt driver. The graph model is the canonical property graph (vertices
with a kind + key + properties; directed edges with a kind + endpoints + properties),
so the same ports are intended to back other openCypher engines and ArangoDB in future.

!!! note "Status"
    The graph contracts are **pre-1.0 and evolving**, and the Neo4j adapter ships a
    **focused vertical slice**: vertex/edge CRUD, both edge-identity modes,
    `neighbors` / `expand` / `shortest_path`, tenant-property isolation, and the raw
    escape hatch. Other port methods raise a clear `NotImplementedError` until
    subsequent releases.

Install with the extra:

    :::bash
    pip install "forze[neo4j]"

## Defining a graph module

A `GraphModuleSpec` is a bounded bundle of vertex and edge kinds. Each node names the
`read` DTO and the `key_field` that supplies a `VertexRef.key` (defaults to `id`). Each
edge declares its `identity` mode and allowed `endpoints`.

    :::python
    from pydantic import BaseModel
    from forze.application.contracts.graph import (
        GraphModuleSpec, GraphNodeSpec, GraphEdgeSpec,
        GraphEdgeEndpoint, GraphEdgeDirectionality,
    )

    class UserRead(BaseModel):
        id: str
        name: str | None = None

    class FollowsRead(BaseModel):
        weight: int | None = None

    social = GraphModuleSpec(
        name="social",
        nodes=(GraphNodeSpec(name="User", read=UserRead, create=UserRead),),
        edges=(
            GraphEdgeSpec(
                name="FOLLOWS",
                read=FollowsRead,
                identity="endpoints",   # at-most-one FOLLOWS per (from, to)
                endpoints=(GraphEdgeEndpoint(from_kind="User", to_kind="User"),),
                directionality=GraphEdgeDirectionality.DIRECTED,
            ),
        ),
    )

### Edge identity

Edge addressing is per kind (`GraphEdgeSpec.identity`):

| Mode | Address via | Maps to | Use when |
|------|-------------|---------|----------|
| `"endpoints"` | `EdgeRef.by_endpoints(kind, from_ref, to_ref)` | Cypher `MERGE` by endpoints | at-most-one edge of a kind per pair (the common relationship case) |
| `"key"` | `EdgeRef.by_key(kind, key)` | a business-key property (set `key_field`) | edges with their own identity, or multigraphs |

## Wiring

    :::python
    from forze.application.execution import DepsRegistry, LifecyclePlan
    from forze_neo4j import Neo4jClient, Neo4jDepsModule, Neo4jGraphConfig, neo4j_lifecycle_step

    deps = DepsRegistry.from_modules(
        lambda: Neo4jDepsModule(
            client=Neo4jClient(),
            graphs={"social": Neo4jGraphConfig()},
        )(),
    )

    lifecycle = LifecyclePlan.from_steps(
        neo4j_lifecycle_step(uri="neo4j://localhost:7687", auth=("neo4j", "password")),
    )

`Neo4jDepsModule` registers `Neo4jClientDepKey` plus the graph query, command, and raw
dep keys for each configured route. The client driver is opened by the lifecycle step.

### What gets registered

| Key | Capability |
|-----|-----------|
| `Neo4jClientDepKey` | Raw Neo4j client |
| `GraphQueryDepKey` | Read operations (`get_vertex`, `neighbors`, `expand`, `shortest_path`, …) |
| `GraphCommandDepKey` | Write operations (`create_vertex`, `create_edge`, `ensure_edge`, …) |
| `GraphRawQueryDepKey` | Opt-in raw Cypher escape hatch |

## Using the graph ports

    :::python
    # In a handler factory: resolve query/command/raw for a module spec.
    q = ctx.graph.query(social)
    c = ctx.graph.command(social)

    await c.create_vertex("User", UserCreate(id="a", name="Alice"))
    await c.create_vertex("User", UserCreate(id="b"))
    await c.create_edge("FOLLOWS", FollowsCreate(from_key="a", to_key="b", weight=7))

    out = await q.neighbors(
        VertexRef(kind="User", key="a"),
        GraphDirection.OUT,
        frozenset({"FOLLOWS"}),
        limit=20,
    )

Edge create/ensure commands must carry `from_key` and `to_key` (the endpoint vertex
keys) alongside the edge properties.

### Raw escape hatch

For power features the neutral ports do not express (path predicates, GDS, APOC), use
the opt-in raw port. It is engine-specific and **bypasses the adapter's neutral
guarantees** — tenancy filtering and codec materialization are your responsibility:

    :::python
    rows = await ctx.graph.raw(social).run(
        "MATCH (n:User) RETURN count(n) AS total"
    )

## Multi-tenancy

With `Neo4jGraphConfig(tenant_aware=True)`, the adapter stamps a `tenant_property`
(default `tenant_id`) on writes and constrains anchor-node matches by it, isolating
tenants within a single database. Bind a `TenantIdentity` at the request boundary.
Database-per-tenant routing is a planned follow-up.

## Scope of the integration

Forze handles graph CRUD, traversal, codec materialization of read DTOs, error mapping
to `CoreException`, and tenant-property isolation. It does **not** manage Neo4j
schema/constraints, index creation, clustering, or backups — those are operational
concerns.
