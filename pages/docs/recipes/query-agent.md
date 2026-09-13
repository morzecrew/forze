---
title: Build a question-answering agent
icon: lucide/message-circle-question
summary: Give an agent your aggregate's read operations as tools, declare the hop that carries data to the model, and keep the loop in your own code
---

An agent that answers questions about your data needs two things your application
already governs: a way to *read* the data, and a way to *reach the model*. Written by
hand, both turn into parallel machinery — a table of tool schemas mirroring your
operations, and an outbound call nobody reviewed. This recipe assembles them out of
what is already there: the [agent-tool bridge](../data-events/agent-tools.md) for the
first, a declared [egress route](../integrations/http.md) for the second. The loop that
joins them stays yours.

The runnable version lives at `examples/recipes/query_agent/` and runs fully in-process
on the mock — the provider is answered by a registered handler, so there is no API key
and no network.

## The aggregate, and what may be queried

A `Notes` document aggregate, plus the one thing that makes it safe to point an agent
at: a query policy. The read model has three fields; only two of them may be filtered,
one may be sorted, and one may be grouped.

```python
--8<-- "recipes/query_agent/app.py:aggregate"
```

The policy is not decoration. It is the allow-set the list operation enforces, and — as
the next section shows — the one the agent is *told about*.

## The palette is the agent's whole world

`operation_tools` projects the operations you name into tool definitions. The allowlist
is required, so an agent's capabilities are a decision in your wiring rather than a
consequence of what happens to be registered:

```python
--8<-- "recipes/query_agent/app.py:palette"
```

Two read tools, and no write tool anywhere. An agent that only answers questions cannot
create, update or delete a note — not because a check denies it, but because there is
nothing in the palette to call.

Each definition carries a name, a JSON Schema taken from the operation's own input DTO,
and a description. For a filter-accepting operation that description also spells out the
filter surface:

> List documents by filters and sorts (offset pagination). Filterable fields — category
> (string: `$eq`, `$ilike`, `$in`, `$like`, `$neq`, `$nin`, `$null`, `$regex`); title
> (string: …). Sortable by: title. Aggregatable by: category.

That sentence is the read model's *policy*, not its field list — `body` is readable and
absent from it. It is the same sentence the [MCP](../integrations/mcp.md) surface hands
an external agent, from the same source, so the two cannot tell one model two different
stories. A model that reads it gets the field names and the operators right on its first
attempt instead of learning them one refusal at a time.

## The hop to the model is declared egress

The question and the rows retrieved for it leave your trust boundary when you call a
provider. Declare that route as a service and say so:

```python
--8<-- "recipes/query_agent/app.py:egress"
```

Two fields, deliberately separate: `egress_sensitive` is a fact about the data the route
carries, `acknowledge_data_egress` is an operator accepting it. Declare the first without
the second and wiring fails closed with `http_egress_unacknowledged` rather than shipping
the data and noting it in a comment. Calls through the route also tag their span
`forze.egress.sensitive`, so sensitive egress is queryable in
[observability](../running-in-prod/observability.md).

This is a governance marker and a conscious-choice gate — nothing inspects a payload.
And it covers the hops you *declare*: if you hand a bare `HttpClient` to a provider's own
SDK, that call has no service config and therefore nothing to gate. Declaring the hop as
a service is what puts it inside the marker.

The example itself never reaches a transport — the handler below answers the service call
in-process — so this config is what a deployment wires rather than something the run
exercises. Its test constructs it, and constructs it without the acknowledgement to see
the refusal, which is the part worth pinning.

## The loop is yours

One turn: ask the model, run the tool it picked, hand back what came out.

```python
--8<-- "recipes/query_agent/app.py:loop"
```

A production loop keeps going — feed the result back, let the model ask for another tool,
stop when it answers in prose. Nothing about that shape is the framework's business, and
the bridge does not try to own it: it dispatches one call at a time and returns.

`dispatch_tool_use` runs the call through `run_operation` on the context you pass, so
tenancy, permissions, the deadline and audit apply exactly as they would to an HTTP
request for the same operation. A governed failure — a denied permission, a field outside
the policy — comes back as `is_error` with the code, which is something the model can act
on. An infrastructure failure propagates to your loop instead, because retrying a dropped
connection is not the model's job.

## What stands in for the provider

The example registers a handler for the model service and keyword-matches the question:

```python
--8<-- "recipes/query_agent/app.py:stub"
```

Only the provider's *answer* is stubbed. The palette it chooses from, the dispatch its
choice triggers, and the governance around both are the real thing — which is what lets a
test drive the whole recipe:

```bash
uv run python -m examples.recipes.query_agent.app
uv run pytest tests/unit/test_examples/test_query_agent.py
```

Swapping in a real provider is a choice about which of the two things above you keep.
Point the service at the provider's real base URL and describe its endpoint as an
`HttpOperationSpec`, and the loop, the palette and the egress declaration all stay as they
are — you write the request and response models instead of importing an SDK. Reach for the
vendor's SDK instead and you own the transport: adapt its tool-call type to a `ToolUse` the
same way, but the hop is no longer a declared service, so the marker no longer covers it.
That is the trade, stated where it is made rather than discovered later in a review.

## Notes

- **A read-only palette is the default.** `operation_tools` refuses a command operation
  unless you ask for a command-capable toolset, so an agent that must act is a visible
  decision in the diff.
- **Streaming the answer** is a transport concern, not an agent one. The realtime
  recipes — [delivery to offline users](realtime-offline-delivery.md),
  [tenant-sharded realtime](tenant-sharded-realtime.md) — apply unchanged to an answer
  arriving a token at a time.
- **An operation whose read model is marked `sensitive` cannot be projected at all**, so
  credential material has no path into a tool palette.

## Related

- [Agent tools](../data-events/agent-tools.md) — what the bridge does and does not do
- [Expose an aggregate over MCP](expose-an-aggregate-over-mcp.md) — the same projection
  for an agent on the far end of a transport
- [Outbound HTTP](../integrations/http.md) — the egress declaration in full
- [Query syntax](../reference/query-syntax.md) — the DSL the agent writes filters in
