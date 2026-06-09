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

Neo4j has no routed-client variant — multi-tenancy is a **property partition**
inside the adapter (see Notes).

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
- **Tenancy is property partitioning:** with `tenant_aware`, a `tenant_property`
  (default `tenant_id`) is stamped on writes and matched on reads; raw `run`
  fails closed without a bound tenant.
- Credentials are passed at lifecycle time (`auth=`), not held on `Neo4jConfig`.
