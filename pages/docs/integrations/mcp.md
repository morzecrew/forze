---
title: MCP
icon: lucide/plug-zap
summary: Expose a frozen operation registry as Model Context Protocol tools
---

`forze[mcp]` is an inbound transport — like [FastAPI](fastapi.md) or
[Socket.IO](socketio.md), but for AI agents. It projects a **frozen operation
registry** onto an MCP server: each operation becomes a tool that runs through
the normal Forze pipeline (DTO validation → operation → result).

## Install

```bash
uv add 'forze[mcp]'
```

Built on FastMCP v3; no backing service of its own.

## Expose operations as tools

The batteries-included path builds a server from a registry and a context
factory:

```python
from forze_mcp import build_mcp_server

server = build_mcp_server(
    registry,                       # a frozen OperationRegistry
    ctx_factory=lambda: runtime.get_context(),
    name="orders",
    include_writes=False,           # read-only by default; True adds command tools
)
```

Each tool's input schema is flattened to the operation's DTO fields, so agents
see a natural flat contract. A plan-declared
[deadline](../in-depth/deadlines.md) adds a time-budget sentence to the tool's
description, so an agent can set its client timeout instead of retrying a call
that died of budget exhaustion. For a custom FastMCP server, use
`register_tools(...)` instead of `build_mcp_server`.

## What it provides

| Surface | What it does |
|---------|--------------|
| `register_tools` / `build_mcp_server` | operations → MCP tools (reads only unless `include_writes=True`) |
| `register_resource_templates` | get-by-id operations → resource templates (`scheme://{id}`) |
| `register_schema_resources` | per-aggregate field schemas as MCP resources |
| `register_dsl_query_prompts` | prompts teaching the [Query DSL](../reference/query-syntax.md) |

## Notes

- **No authorization at this boundary** — governance stays in the engine.
  Identity is supplied by a resolver: `StaticIdentityResolver` (dev) or
  `DelegatedIdentityResolver` (run on behalf of a resolved subject, agent as
  actor).
- Reads are read-only by default; `include_writes=True` opts in and tags command
  tools as destructive.
- For production, bring your own `FastMCP` (auth, transport); `build_mcp_server`
  is a convenience. See the
  [expose-an-aggregate-over-MCP recipe](../recipes/expose-an-aggregate-over-mcp.md)
  for a runnable walkthrough.
