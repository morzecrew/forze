# Forze examples

Runnable, **test-backed** examples. Each example is a normal module you can run, and is
executed by a test under `tests/unit/test_examples/` so it stays correct as the framework
evolves (an example that doesn't run is worse than no example).

Run one:

```bash
uv run python -m examples.order_fulfillment        # info-level narrative
uv run python -m examples.order_fulfillment debug  # also show debug lines
uv run python -m examples.mcp_server               # serve a mock aggregate over MCP
```

Each demo configures logging so it prints its own readable narrative instead of the
framework's verbose trace logs, and runs **both** the happy path and the compensation path.

## `order_fulfillment.py` — the whole stack in one story

Shows how the DDD + orchestration pieces **compose** end to end, in-process (`forze_mock`,
no Docker):

1. A **checkout saga** (`reserve` → `confirm` pivot) orchestrates two aggregates.
2. Confirming the **`Order` aggregate** trips its `@event_emitter`, which dispatches
   `OrderConfirmed` **inside the saga step's transaction** (the command flow).
3. The **outbox bridge** stages an `order.confirmed` integration event; the step flushes it
   (transactional outbox).
4. A **relay** (standing in for a broker + the outbox relay worker) claims the staged event
   and delivers it to the consumer.
5. The consumer processes it **exactly-once via the inbox** and creates a `Shipment`.

It also demonstrates **compensation**: if the pivot fails, the saga releases the reserved
inventory and stages nothing downstream.

See [tests/unit/test_examples/test_order_fulfillment.py](../tests/unit/test_examples/test_order_fulfillment.py)
for the assertions (happy path, idempotent redelivery, compensation).

## `mcp_server.py` — a mock aggregate exposed over MCP

Requires the `mcp` extra (`uv sync --extra mcp`). Wires an in-memory `forze_mock` "Notes"
document aggregate into a `FastMCP` server via `forze_mcp` — every document operation becomes
an MCP tool that runs through the normal Forze pipeline. Inspect it with the official
[MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
uv run python -m examples.mcp_server        # Streamable HTTP on http://127.0.0.1:8000/mcp
npx -y @modelcontextprotocol/inspector      # choose "Streamable HTTP" + the URL above
```

Two notes are seeded at startup, and `include_writes=True` exposes `notes.create` /
`update` / `kill` alongside the reads — so you can list, fetch, create, and mutate notes from
the Inspector and watch each call flow through a real operation. (Switch the transport at the
bottom of the module to `"stdio"` to launch it as an Inspector stdio command instead.)

See [tests/unit/test_examples/test_mcp_server_example.py](../tests/unit/test_examples/test_mcp_server_example.py)
for the assertions (operations preserved as tools; seed/list and create→list round-trips).
