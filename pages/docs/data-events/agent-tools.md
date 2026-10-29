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

A tool is named after its operation, spelled the way providers accept it: OpenAI-style
function calling and Anthropic both take only `^[A-Za-z0-9_-]{1,64}$`, so `notes.list`
becomes `notes__list`, and any other character outside that set becomes `_`. Pass
`name=` — a function from operation key to tool name — to choose another spelling. The
toolset keeps each name's operation, so dispatch never has to reverse the spelling.

The operation key still dispatches too. It was the tool's name in 0.16.2 and earlier, so
a durable run or a conversation history that journaled `notes.list` replays after an
upgrade. A palette with a name outside that pattern, or one name for two operations —
an operation's key counts as one of its names — is refused when it is built rather than
by the provider on the first request.

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

An argument the operation has never heard of is one of those failures. A model
that invents `filter=…` on a tool without such a field is told so by name,
rather than having the key quietly dropped and being handed an unfiltered answer
it will report as filtered. That holds for an operation that takes no input at
all, and for a DTO that renames fields inbound. The one exception is a DTO
configured `extra="allow"`, which takes extension fields on purpose — there the
check steps aside and the DTO decides.

## A query tool says what may be queried

A filter-accepting operation's input schema says a filter may be sent; it cannot
say *which fields*, because the DSL's expression types are field-agnostic by
design. So the projected description carries the read model's filter surface —
the filterable fields with the operators each one accepts (grouped by type, so
each operator list is stated once), what may be sorted,
what may be grouped — taken from the spec's query policy rather than from the
model's field list. A field the policy withholds is not advertised, and a model
that reads the description gets its first filter right instead of discovering
the allow-set one refusal at a time.

A spec with no policy withholds nothing, so every read field is advertised,
bookkeeping columns included — a second reason to write one, since the
description a palette carries is paid for on every turn.

The operators are the **upper bound the field's type allows**, not a promise from
whichever engine serves the call. Nothing resolves the adapter at projection
time — a routed dependency picks one per invocation — so a backend with narrower
[capabilities](../reference/query-syntax.md) may compile fewer, and a filter it
will not take comes back as `query_feature_unsupported`: an error result the
model can act on, like any other governed refusal.

It is the same sentence the MCP surface gives an external agent, from one shared
builder, so the two surfaces cannot describe one read model differently.
[Build a question-answering agent](../recipes/query-agent.md) puts it to work.

A palette can advertise less than the spec allows. `query_policies` gives a tool its
own `QueryFieldPolicy`, the handful of fields an agent should filter on, while HTTP
keeps the spec's full [`query_policy`](../reference/contracts/document.md#spec):

```python
from forze.application.contracts.querying import QueryFieldPolicy
from forze_kits.integrations.agent_tools import operation_tools

tools = operation_tools(
    registry,
    include=["notes.list"],
    query_policies={"notes.list": QueryFieldPolicy(filterable={"title"}, sortable=set())},
)
```

The tool's description then names only `title`, and dispatch refuses a call that
filters or sorts on any other field (`field_not_filterable`, `field_not_sortable`), so
the tool accepts no more than it advertises. An axis left `None` keeps the spec's. A
policy can only narrow. The palette is refused when it is built if a policy names a
field outside the operation's own allow-set, an operation outside `include`, or an
operation that takes no filter. It is also refused if a policy narrows an axis the
operation's input does not carry, such as `aggregatable` on a plain list. The filter is
checked under the default [filter limits](../reference/query-syntax.md#limits), so a
spec that raises them has its larger filters refused by such a tool.

The [filter grammar](../reference/query-syntax.md) itself is most of a list tool's
schema, and it is the same in every filter-accepting tool, so a palette with ten of
them sends it ten times per request. `shared_filter_grammar=True` sends it once:

```python
from forze_kits.integrations.agent_tools import operation_tools

tools = operation_tools(
    registry, include=["notes.list", "notes.agg_list"], shared_filter_grammar=True
)
system_prompt = f"You answer questions about notes.\n\n{tools.filter_grammar}"
```

Each `filters` (and an aggregate's `$having`) becomes a plain object pointing at the
system prompt, the same stripping the [MCP](../integrations/mcp.md) option of that name
applies, and `tools.filter_grammar` is the text to put there. It is `None` when the
option is off or no tool in the palette takes a filter. The descriptions keep their
filter surface, and a call is validated against the full grammar either way.

## A tool call is simulable, because it is an operation

Nothing about the bridge is deterministic-simulation-aware, and it does not need to be. A
dispatch goes through `run_operation`, so a deterministic simulation drives an agent's
turns the way it drives any other caller: concurrent tool calls interleave under a forced
schedule, and an invariant declared on the aggregate is checked over the result.

`examples/recipes/agent_tools_dst/` is that run. One operation stands between the
simulation and the bridge — a *turn*, carrying whichever tool the run's seeded RNG picked
and the arguments it built — so the model's choice is part of what the search explores and
reproduces from the master seed. Everything below the turn is the real thing.

The contrast is what makes it evidence rather than decoration: under the same schedule and
the same compiled oracle, the same turns against a registry with no enforcement composed
into it double-book the aggregate's capacity, while the governed one holds. If the bridge
were bypassing the pipeline, the green run would look identical to the broken one.

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

The line is the per-kind egress policy the rest of the framework uses, not a
judgement the bridge makes, and two cases are worth knowing. A **throttle**
reaches the agent — backing off or narrowing a request is exactly what a model
can do about it. An **exhausted deadline** does not: the call ran out of its
budget, so retrying it inside the same turn is the one thing that cannot help,
and it propagates instead.

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

An operation whose result is a byte stream, like the stored-file kit's
`download_stream`, is refused the same way. Its descriptor sets `streams=True`, and a
tool result is JSON, so the model would get the stream object, never the bytes.

## Related

- [Inference](inference.md) — the seam your loop calls the model through
- [Sandbox](sandbox.md) — running code, as opposed to running operations
- [MCP](../integrations/mcp.md) — the same projection for an agent on the far end
  of a transport
- [Build a question-answering agent](../recipes/query-agent.md) — a palette, a
  declared egress and a loop, assembled
