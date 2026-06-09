---
title: Integrations
summary: One optional package per backend, each behind a stable contract
---

Every integration is an optional package — `forze[postgres]`, `forze[redis]`,
and so on — that implements Forze contracts for one backend. They share a common
shape: a client and ports in `kernel/`, concrete `adapters/`, and `execution/`
deps + lifecycle modules you wire exactly as shown in
[Wiring](../in-depth/wiring.md). Your handlers never import them; they resolve
ports from the context.

Per-backend pages cover the specifics — configuration, schema expectations, and
caveats. The authoritative, always-current set is the `[project.optional-dependencies]`
extras in `pyproject.toml`; each `forze[<name>]` maps to the `<name>` extra.

## Available integrations

| Area | Extras |
|------|--------|
| **Documents** | `postgres` · `mongo` · `firestore` · `arango` |
| **Graph** | `neo4j` · `arango` |
| **Cache, queues & streams** | `redis` · `rabbitmq` · `sqs` |
| **Search & analytics** | `meilisearch` · `bigquery` · `clickhouse` |
| **Object storage** | `s3` · `gcs` |
| **Workflows & durable execution** | `temporal` · `inngest` |
| **Transport** | `fastapi` · `socketio` · `mcp` |
| **Identity & secrets** | `authn` · `oidc` · `vault` |
| **Outbound** | `http` |

Install one or several at once — `uv add 'forze[fastapi,postgres,redis]'`.
</content>
