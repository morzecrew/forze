# Forze examples

Runnable, **test-backed** examples. Each is a normal module you can run, and is executed by a
test under `tests/` so it stays correct as the framework evolves (an example that doesn't run
is worse than no example). Most run fully in-process on `forze_mock` — no Docker.

Layout:

- **`quickstart/`** — the Get Started app (in-memory User CRUD on FastAPI).
- **`recipes/<name>/`** — one directory per [docs recipe](../pages/docs/recipes/). Each has an
  `app.py`; backend-coupled recipes also carry a `compose.yaml` + `justfile` (`just run`).

The docs pull their code straight from these files via snippet includes, so the snippets can't
drift from what actually runs.

## Headliner examples

```bash
uv run python -m examples.recipes.order_fulfillment.app          # the whole stack, one story
uv run python -m examples.recipes.order_fulfillment.app debug    # also show debug lines
uv run python -m examples.recipes.analytics_duckdb.app           # DuckDB analytics over a lake
uv run python -m examples.recipes.mcp_server.app                 # serve an aggregate over MCP
```

### `recipes/order_fulfillment/` — the whole stack in one story

Shows how the DDD + orchestration pieces **compose** end to end, in-process:

1. A **checkout saga** (`reserve` → `confirm` pivot) orchestrates two aggregates.
2. Confirming the **`Order` aggregate** trips its event emitter, which dispatches
   `OrderConfirmed` **inside the saga step's transaction**.
3. The **outbox bridge** stages an `order.confirmed` integration event; the step flushes it.
4. A **relay** (standing in for a broker + the relay worker) delivers it to the consumer.
5. The consumer processes it **exactly-once via the inbox** and creates a `Shipment`.

It also demonstrates **compensation**: if the pivot fails, the saga releases the reserved
inventory and stages nothing downstream. Recipe:
[end-to-end: saga → outbox → inbox](../pages/docs/recipes/end-to-end-saga-outbox-inbox.md).

### `recipes/analytics_duckdb/` — analytics over a data lake

An in-process DuckDB engine runs a named, typed analytics query over a local Parquet file
(standing in for object storage) — no warehouse, no Docker. The handler names only a
`query_key` and an output type; the lake source binds in the config and lifecycle step.
Recipe: [analytics over a data lake](../pages/docs/recipes/analytics-over-a-data-lake.md).

### `recipes/mcp_server/` — a mock aggregate exposed over MCP

Requires the `mcp` extra (`uv sync --extra mcp`). Wires an in-memory "Notes" aggregate into a
`FastMCP` server via `forze_mcp` — every document operation becomes an MCP tool that runs
through the normal Forze pipeline. Inspect it with the
[MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
uv run python -m examples.recipes.mcp_server.app   # Streamable HTTP on http://127.0.0.1:8000/mcp
npx -y @modelcontextprotocol/inspector             # choose "Streamable HTTP" + the URL above
```

`include_writes=True` exposes `notes.create` / `update` / `kill` alongside the reads, so you can
list, fetch, create, and mutate notes from the Inspector and watch each call flow through a real
operation. Recipe:
[expose an aggregate over MCP](../pages/docs/recipes/expose-an-aggregate-over-mcp.md).

## The rest of the recipes

| Directory | Shows | Recipe |
|-----------|-------|--------|
| `recipes/crud_fastapi/` | CRUD over Postgres on FastAPI | [crud-fastapi-postgres](../pages/docs/recipes/crud-fastapi-postgres.md) |
| `recipes/read_only/` | A read-only document API | [read-only-document-api](../pages/docs/recipes/read-only-document-api.md) |
| `recipes/cache_reads/` | Read-through caching with Redis | [cache-reads-with-redis](../pages/docs/recipes/cache-reads-with-redis.md) |
| `recipes/idempotency/` | Idempotent operations | [add-idempotency](../pages/docs/recipes/add-idempotency.md) |
| `recipes/outbox/` | Transactional outbox | [transactional-outbox](../pages/docs/recipes/transactional-outbox.md) |
| `recipes/notifications/` | Transactional notifications | [transactional-notifications](../pages/docs/recipes/transactional-notifications.md) |

Backend-coupled recipes (`crud_fastapi`, `read_only`, `cache_reads`) include a `compose.yaml` +
`justfile`; `just run` in that directory spins the service, runs the app, and tears it down.
