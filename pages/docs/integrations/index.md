---
title: Overview
icon: lucide/plug
summary: One optional package per backend, each behind a stable contract
---

Every integration is an optional package — `forze[postgres]`, `forze[redis]`,
and so on — that implements Forze contracts for one backend. They share a common
shape: a client and ports in `kernel/`, concrete `adapters/`, and `execution/`
deps + lifecycle modules you wire exactly as shown in
[Wiring](../writing-operation/wiring.md). Your handlers never import them; they resolve
ports from the context.

Per-backend pages cover the specifics — configuration, schema expectations, and
caveats. The authoritative, always-current set is the `[project.optional-dependencies]`
extras in `pyproject.toml`; each `forze[<name>]` maps to the `<name>` extra.

## Available integrations

| Area | Extras |
|------|--------|
| **Data** | `postgres` · `mongo` · `firestore` · `neo4j` · `redis` · `s3` · `gcs` · `meilisearch` · `bigquery` · `clickhouse` · `duckdb` |
| **Messaging** | `kafka` · `rabbitmq` · `sqs` |
| **Workflows** | `temporal` · `inngest` |
| **Inbound** | `fastapi` · `socketio` · `mcp` |
| **Identity** | `authn` · `oidc` |
| **Secrets & keys** | `vault` · `kms-aws` · `kms-gcp` · `kms-yc` |
| **Outbound** | `http` · `inference-http` · `inference-sagemaker` |

Each row maps to a `forze[<extra>]` package; their precise contract coverage is
on each integration's page. Two key backends need no extra at all: the
[self-hosted local KMS](kms.md) and the in-memory mock.

The remaining extras are not backend integrations: `dst` and `cli` are tooling
([deterministic simulation testing](../dst/overview.md) and the `forze`
command-line tool), and `zstd` adds the zstd codec for
[portable archives](../running-in-prod/portability.md).

Install one or several at once — `uv add 'forze[fastapi,postgres,redis]'`.
