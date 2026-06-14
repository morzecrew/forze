---
title: Neo4j
icon: lucide/share-2
summary: Graph nodes, edges, and traversals on Neo4j
---

`forze[neo4j]` implements the graph contracts on Neo4j (openCypher over the async
Bolt driver) — nodes, edges, and traversals behind the graph ports.

## Install

```bash
uv add 'forze[neo4j]'
```

Needs a Neo4j server.

## The client

```python
from forze_neo4j import Neo4jClient

neo4j = Neo4jClient()
```

Neo4j spans the full isolation ladder (see Notes): a tenant **property partition**
(`tagged`), a per-tenant **database** (`namespace`), or a **routed client**
(`dedicated`) — `RoutedNeo4jClient` resolves per-tenant Bolt URI/credentials from secrets.

## Wire it

Graphs are keyed by their module spec name:

```python
from forze.application.execution import DepsRegistry, LifecyclePlan
from forze_neo4j import Neo4jClient, Neo4jGraphConfig, neo4j_lifecycle_step, Neo4jDepsModule

deps = DepsRegistry.from_modules(
    Neo4jDepsModule(client=neo4j, graphs={"social": Neo4jGraphConfig()}),
)
lifecycle = LifecyclePlan.from_steps(
    neo4j_lifecycle_step(uri="neo4j://localhost:7687", auth=("neo4j", "password")),
)
```

## What it provides

| Contract | Keyed by |
|----------|----------|
| Graph query / command / raw Cypher | `GraphModuleSpec.name` (bundling `GraphNodeSpec` + `GraphEdgeSpec`) |

## Notes

- **Vertical slice.** The common operations are implemented — `get_vertex`,
  `create_vertex`, `neighbors`, `expand`, `shortest_path`, `create_edge`,
  `ensure_edge`, raw `run` — while several others (bulk ops, some edge queries)
  still raise `NotImplementedError`.
- **Tenancy spans three tiers.** `tagged` — with `tenant_aware`, a `tenant_property`
  (default `tenant_id`) is stamped on writes and matched on reads (raw `run` fails closed
  without a bound tenant). `namespace` — set `Neo4jGraphConfig.database` to a
  `(tenant_id) -> str` resolver to route each query to the tenant's own database (Neo4j 4+
  multi-database). `dedicated` — wire `RoutedNeo4jClient` (+ `routed_neo4j_lifecycle_step`)
  for a per-tenant instance/credentials resolved from secrets. Declare the minimum you
  require with `Neo4jDepsModule(required_tenant_isolation=...)`.
- Credentials are passed at lifecycle time (`auth=`), not held on `Neo4jConfig`.
