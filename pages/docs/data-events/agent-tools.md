---
title: Agent tools
icon: lucide/wrench
summary: Project operations into an agent's tool palette and dispatch its calls back through the same governed pipeline every other caller uses
---

An agent that can only talk is easy to ship. The moment it has to *act* — read a
customer's orders, file a refund, answer a question from real data — something
has to decide what it may do, on whose behalf, and inside which tenant. Written
by hand, that decision ends up in two places that drift: a table of tool schemas
that mirrors your operations, and a dispatcher that re-implements the tenancy and
permission checks your operations already have.

The **agent-tool bridge** removes both. Your operations become the agent's tools,
and the agent's tool calls become ordinary governed invocations.

## What the bridge actually buys you

**Tools that cannot drift from the code they call.** A tool's input schema is the
operation's own input DTO — not a copy of it. Change the DTO and the agent's tool
changes with it, in the same commit, because there is nothing to keep in sync.

**Governance for free, and identically.** A tool call runs through
`run_operation` on your live context, so tenancy, permission keys, ownership
checks, the deadline, resilience and audit all apply exactly as they do to the
same operation called over HTTP. An agent cannot reach an operation the current
principal cannot reach. There is no second enforcement path to get wrong, because
there is no second path.

**A palette that is a capability grant.** The allowlist is required and there is
no "expose everything" convenience. Every tool is something a model can invoke,
so the set of them is reviewed the way a permission grant is reviewed — at
wiring, in the diff.

**Mutating power set by the palette, not by a denial.** A read-only toolset
projects only `QUERY` operations, so an agent answering questions has no write
tool to call: the capability is absent rather than refused. Read-only is the
default; an agent that must act is given a command-capable toolset on purpose.

## The shape in code

```python
from forze_kits.integrations.agent_tools import ToolUse, dispatch_tool_use, operation_tools

tools = operation_tools(registry, include=["notes.list", "notes.get"])
```

`tools.defs` is what your SDK needs — a name, a description and a JSON Schema per
tool. Adapt them to whichever tool type your provider uses; that adapter is a few
lines and it stays yours.

When the model asks for a tool, hand the call back:

```python
result = await dispatch_tool_use(
    ToolUse(id=block.id, name=block.name, input=block.input),
    ctx=ctx,
    tools=tools,
)
```

`result.content` is the operation's result as JSON. `result.is_error` marks a
governed failure the model can act on — a validation rejection, a permission
denial, a precondition that is not met — carrying the error code and the
sanitized field errors rather than anything from inside your process. Feed it
back and the agent takes another turn.

## Two failures, kept apart

A **governed** failure belongs to the conversation. Bad arguments, a denied
permission, a document that is not in a state the operation accepts: the model
can correct or give up, so it comes back as a `ToolResult` and the loop keeps
going.

An **infrastructure** failure does not. A dropped connection is not something a
language model can fix by rephrasing, so it propagates out of
`dispatch_tool_use` to your loop, where the application decides whether the turn
is retried. This is the same split `run_operation` already draws between a
declared domain outcome and a bug.

## What the bridge is not

**Not an agent loop.** The loop is yours — your SDK owns the conversation, the
turns, and the prompt. The bridge turns operations into tools and dispatches them
one at a time.

**Not a tool runtime.** The tools are your operations. Running arbitrary code is
a different plane with a different threat model; see
[Sandbox](sandbox.md).

**Not dynamic discovery.** The palette is projected from a frozen registry, and
nothing is looked up per turn. Wire it once at startup: a stable set of tool
names is what makes an agent run resumable and auditable.

**Not a prompt or description layer.** A tool's description is the one its
operation's descriptor already carries — the same description the HTTP and MCP
surfaces start from, though each adds its own notes around it. Tuning what the
model reads means editing the operation's catalog entry, where the rest of its
contract already lives.

## Sensitive operations are refused, not skipped

An operation whose read model carries credential material is marked `sensitive`
on its spec, and naming one in a palette fails the projection. Dropping it
quietly would be worse than failing: the app would believe it granted a
capability it did not, and nobody would find out until an agent needed it.

## Related

- [Inference](inference.md) — the seam your loop calls the model through
- [Sandbox](sandbox.md) — running code, as opposed to running operations
- [MCP](../integrations/mcp.md) — the same projection for an agent on the far end
  of a transport
