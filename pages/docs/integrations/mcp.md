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

## Protect it with API-key auth

The MCP server is a **Resource Server**: it validates an inbound bearer and binds
the principal — no OAuth flow. The bearer is a forze_identity **API key** the caller
already holds (a controlled agent's secret, or a key the user minted in your web UI
and pasted into the agent host), verified by the **same** authn brain as your HTTP
edge:

```python
from forze.application.contracts.authn import AuthnSpec
from forze_mcp import (
    AccessTokenIdentityResolver,
    ForzeApiKeyVerifier,
    build_mcp_server,
)

spec = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))

server = build_mcp_server(
    registry,
    ctx_factory,
    name="my-service",
    # FastMCP validates the bearer and 401s an invalid/missing key:
    auth=ForzeApiKeyVerifier(ctx_factory=ctx_factory, authn_spec=spec),
    # ...and the verified principal is bound per call, with a fixed agent as actor:
    identity=AccessTokenIdentityResolver(agent=AGENT_PRINCIPAL),
)
```

`ForzeApiKeyVerifier` runs the key through `authenticate_with_api_key` (same as the
FastAPI edge), resolves the tenant, and hands FastMCP an `AccessToken` — an unknown
key returns `None` → a clean `401`, while a misconfiguration fails loud.
`AccessTokenIdentityResolver` reads that verified token and binds the principal,
attaching the delegation **actor**. The engine then enforces the least-privilege
intersection of the user's and the agent's grants.

The agent can come from the key itself. Mint a **delegation key** bound to a
user→agent pair — `issue_api_key(identity, actor_principal_id=agent)` — and the agent
travels on the credential: the resolver attaches *that* agent, so a user's ChatGPT
connection and Claude connection (each its own key, same `AGENT_PRINCIPAL` type or a
distinct one) attribute and revoke independently. The `agent=AGENT_PRINCIPAL` on
`AccessTokenIdentityResolver` is the **fallback** for plain keys that carry none —
pass it to give them a fixed agent, or omit it to bind the bare user.

Pass **both** `auth` and `identity` — `auth` rejects bad credentials, `identity`
binds the good one. Read-only stays the default (`include_writes=False`); a
write-capable agent needs `include_writes=True` **and** write grants on the agent
principal. OAuth-based hosts (that can't paste a key) are a later, external concern —
forze stays the Resource Server and never runs an authorization server.

## Notes

- **No authorization at this boundary** — authentication can be wired (see above),
  but *authorization* governance stays in the engine. Identity is supplied by a
  resolver: `StaticIdentityResolver` (dev), `AccessTokenIdentityResolver` (API-key
  auth), or `DelegatedIdentityResolver` (run on behalf of a resolved subject, agent
  as actor).
- Reads are read-only by default; `include_writes=True` opts in and tags command
  tools as destructive.
- For production, bring your own `FastMCP` (auth, transport); `build_mcp_server`
  is a convenience. See the
  [expose-an-aggregate-over-MCP recipe](../recipes/expose-an-aggregate-over-mcp.md)
  for a runnable walkthrough.
