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

Built on FastMCP v4 and MCP SDK v2; no backing service of its own. FastMCP 4 is still a
pre-release, and it is what speaks the SDK's v2 wire types — code reading protocol fields
off a tool or resource wants their snake_case spellings (`input_schema`, `uri_template`),
not the v1 camelCase ones.

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
[deadline](../running-in-prod/deadlines.md) adds a time-budget sentence to the tool's
description, so an agent can set its client timeout instead of retrying a call
that died of budget exhaustion. For a custom FastMCP server, use
`register_tools(...)` instead of `build_mcp_server`.

## Keep the tool list small

An agent reads every tool's schema on each turn, and a registry exposes more than one
agent needs. Three options, on both `build_mcp_server` and `register_tools`, trim it:

```python
server = build_mcp_server(
    registry,
    ctx_factory=lambda: runtime.get_context(),
    name="orders",
    instructions="Order lookups for the support team.",
    operations=["orders.list", "orders.get"],  # only these become tools
    shared_filter_grammar=True,                # state the filter grammar once
    output_schemas=False,                      # leave result schemas out
)
```

- `operations` is an allowlist. A key the registry does not have, or a command operation
  without `include_writes=True`, is refused when the server is built, so a typo never
  quietly exposes less than you reviewed.
- `shared_filter_grammar=True` takes the [filter grammar](../reference/query-syntax.md),
  most of a list tool's schema, out of every tool: each `filters` (and an aggregate's
  `$having`) becomes a plain object, and the grammar is added once to the server's
  instructions and published at `forze://filter-grammar` (`FILTER_GRAMMAR_URI`). Calls
  are validated against the full grammar either way.
- `output_schemas=False` drops each tool's output schema. A call still returns its result
  as text, but a list or scalar result then carries no structured content, and an object
  comes back as a plain mapping rather than a typed model.

Set the server's instructions before the tools are registered: pass `instructions=` to
`build_mcp_server`, or to your own `FastMCP` before `register_tools`. Assigning
`server.instructions` afterwards replaces the grammar; the tools still point at the
resource, but a client that reads only the instructions loses it.

Two cases keep the grammar where it was. If one of your models shares a name with a
grammar definition (`QueryConjunction`, say), pydantic qualifies both names and the
tools keep the full grammar. And a server mounted under a namespace
(`parent.mount(child, namespace=...)`) republishes the resource under a namespaced URI,
while the tool descriptions still name `forze://filter-grammar`.

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
intersection of the user's and the agent's grants: on an action guard, each must hold the
permission; on a scope-guarded list, each must be permitted and the agent's row filters narrow
the user's.

The agent can also ride the key itself: a **delegation key** minted for a
user→agent pair (`issue_api_key(identity, actor_principal_id=agent)`) carries that
agent, so a user's ChatGPT and Claude connections attribute and revoke
independently. The key's agent must be a principal authentication accepts, other than the
user; with `AuthnDepsModule(authz_route=...)` set, it must also be a registered `service`
principal (under `eligibility="allow_all"` without it, any other id is accepted). The
`agent=AGENT_PRINCIPAL` on `AccessTokenIdentityResolver` is the operator's agent: a plain
key binds it as the actor, and a delegation key's agent is chained under it (user ← key
agent ← operator agent), so a call gets the intersection of all three and a key cannot
drop the operator's ceiling. Omit it to bind the bare user.

With `enforce_delegation_grant` on the route's authz spec, every hop needs its own grant:
`may_act(key agent, user)` from the user, and `may_act(operator agent, key agent)` for each
agent the MCP server may run. A missing grant fails the call closed.

Pass **both** `auth` and `identity` — `auth` rejects bad credentials, `identity`
binds the good one. Read-only stays the default (`include_writes=False`); a
write-capable agent needs `include_writes=True` **and** write grants on every principal in
the chain — the user, the key's agent and the operator's agent. OAuth-based hosts (that can't paste a key) are a later, external concern —
forze stays the Resource Server and never runs an authorization server.

## What it provides

| Surface | What it does |
|---------|--------------|
| `register_tools` / `build_mcp_server` | operations → MCP tools (reads only unless `include_writes=True`) |
| `register_resource_templates` | get-by-id operations → resource templates (`scheme://{id}`) |
| `register_schema_resources` | per-aggregate field schemas as MCP resources |
| `register_dsl_query_prompts` | prompts teaching the [Query DSL](../reference/query-syntax.md) |

## Notes

- **No authorization at this boundary** — authentication can be wired (see above),
  but *authorization* governance stays in the engine. Identity is supplied by a
  resolver: `StaticIdentityResolver` (dev), `AccessTokenIdentityResolver` (API-key
  auth), or `DelegatedIdentityResolver` (run on behalf of a resolved subject, agent
  as actor).
- Reads are read-only by default; `include_writes=True` opts in and tags command
  tools as destructive.
- API-key auth is built in (`auth=` above); for custom transports or server
  policies, bring your own `FastMCP` — `build_mcp_server` is a convenience. See the
  [expose-an-aggregate-over-MCP recipe](../recipes/expose-an-aggregate-over-mcp.md)
  for a runnable walkthrough.
