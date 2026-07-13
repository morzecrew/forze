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
| Graph query / command / raw Cypher / management | `GraphModuleSpec.name` (`graphs`, bundling `GraphNodeSpec` + `GraphEdgeSpec`) |
| Transactions | route in the module `tx` set |

## Notes

- **Full graph surface.** All graph ports are implemented — vertex/edge CRUD
  (single and batch), `neighbors`, `expand`, `find_vertices` / `find_edges`,
  counts and degrees, `shortest_path` and `k_shortest_paths` (weighted variants
  need the Graph Data Science plugin), raw `run`, plus `ensure_schema` /
  `drop_schema` on the management port.
- **Transactions.** Bind an operation's `tx_route` to a name in `tx` so a
  handler's graph writes commit or roll back as a unit; Neo4j is not
  co-transactional with other backends.
- **Tenancy spans three tiers.** `tagged` — with `tenant_aware`, a `tenant_property`
  (default `tenant_id`) is stamped on writes and matched on reads (raw `run` fails closed
  without a bound tenant). `namespace` — set `Neo4jGraphConfig.database` to a
  `(tenant_id) -> str` resolver to route each query to the tenant's own database (Neo4j 4+
  multi-database). `dedicated` — wire `RoutedNeo4jClient` (+ `routed_neo4j_lifecycle_step`)
  for a per-tenant instance/credentials resolved from secrets. Declare the minimum you
  require with `Neo4jDepsModule(required_tenant_isolation=...)`.
- Credentials are passed at lifecycle time (`auth=`), not held on `Neo4jConfig`.
