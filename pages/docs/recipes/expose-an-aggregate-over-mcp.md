---
title: Expose an aggregate over MCP
icon: lucide/plug
summary: Turn a Forze aggregate into MCP tools an AI client can call — every operation runs through the normal governed pipeline
---

The [Model Context Protocol](../integrations/mcp.md) lets AI clients call your
application as tools. With Forze that's almost free: every operation on an
aggregate is already a named, validated, governed unit — `forze_mcp` exposes each
one as an MCP tool, so a model lists notes, fetches one, or creates one by calling
the *same* pipeline your HTTP routes would.

The runnable server lives at `examples/recipes/mcp_server/` and runs fully
in-process on the mock — point the
[MCP Inspector](https://github.com/modelcontextprotocol/inspector) at it and poke.

## The aggregate

A minimal `Notes` document aggregate — nothing MCP-specific about it. The create
payload carries only the domain fields a caller supplies; identity and timestamps
are assigned by the server, so they never appear in the tool schema:

```python
--8<-- "recipes/mcp_server/app.py:aggregate"
```

## Expose its operations as tools

`build_mcp_server` walks the operation registry and registers one MCP tool per
operation. Reads are exposed by default; `include_writes=True` opts the mutations
in too, tagged destructive:

```python
--8<-- "recipes/mcp_server/app.py:server"
```

That's the whole inbound adapter. A tool call now flows exactly like any other
entry point: `forze_mcp` validates the input DTO, runs the operation through the
pipeline (hooks, transaction, authorization), and returns the typed result — no
bespoke tool handlers to write or keep in sync.

## Run it

```bash
uv run python -m examples.recipes.mcp_server.app   # Streamable HTTP on http://127.0.0.1:8000/mcp
npx -y @modelcontextprotocol/inspector             # choose "Streamable HTTP" + the URL above
```

## Notes

- **Reads are read-only by default.** `include_writes=True` is the explicit opt-in
  for `create` / `update` / `kill`; without it the server exposes only queries.
- **Discovery is built in.** The example also registers the querying-DSL guidance
  prompts and a field-schema resource, so an agent can learn which fields are
  filterable/sortable before building a `notes.list` query.
- **Bring your own `FastMCP` in production** for auth and transport;
  `build_mcp_server` is the convenience wrapper. The wiring surface is in the
  [MCP integration page](../integrations/mcp.md).
