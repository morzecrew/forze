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
joins them stays yours, and the answer can stream back over the same route.

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

Each definition carries a name (`notes__list`: providers refuse the `.` in `notes.list`), a
JSON Schema taken from the operation's own input DTO, and a description. For a filter-accepting operation that description also spells out the
filter surface:

> List documents by filters and sorts (offset pagination). Filterable fields — `$eq`,
> `$ilike`, `$in`, `$like`, `$neq`, `$nin`, `$null`, `$regex` on every field; string:
> category, title. Sortable by: title. Aggregatable by: category.

The operators every field shares come first, then fields are grouped by the rest of their
operator set, so each list is stated once, not once per field. Each field stays under its
own type: `number (also $gt, $gte, $lt, $lte): price; temporal (as number): created_at`
on a wider model.

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
And it covers the hops you *declare*: a bare `HttpClient` handed to a provider's own SDK
has no service config and therefore nothing to gate. Declaring the hop as a service is what
puts it inside the marker, and the SDK can still make the call, through a
[transport over the service](../integrations/http.md#hand-a-vendor-sdk-the-service).

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

`invoke` is handed the operation rather than its name, and that is what makes `reply` a
`ModelReply` here: the declaration carries `args_type` and `return_type`, so both reach
your own variables. Naming the operation `"messages"` works too and hands back a bare
`BaseModel` — the right form when the operation comes from config and there is no
declaration to point at, and the wrong one when you meant to read a field off the answer.

`dispatch_tool_use` runs the call through `run_operation` on the context you pass, so
tenancy, permissions, the deadline and audit apply exactly as they would to an HTTP
request for the same operation. A governed failure — a denied permission, a field outside
the policy — comes back as `is_error` with the code, which is something the model can act
on. An infrastructure failure propagates to your loop instead, because retrying a dropped
connection is not the model's job.

## The answer streams over the same hop

A model that answers in prose usually streams it, a token at a time. `stream` takes the
operation `invoke` takes and yields the response body as it arrives, so the streamed
answer leaves through the declared route like everything else:

```python
--8<-- "recipes/query_agent/app.py:stream"
```

The request is the one `invoke` sends: the acknowledgement gates it, the credential and
the deadline budget ride on it as it opens, and its span carries `forze.egress.sensitive`. A rejected
request raises before the first token, with the error `invoke` would raise. Server-sent
events are lines, and a network cuts chunks wherever it likes, so lines come out of a
buffer rather than out of each chunk. `aclosing` closes the connection as soon as you stop
reading — on an early return as much as at the end.

## What stands in for the provider

The example registers a handler for the model service. It keyword-matches the question, and
answers a streaming request with events cut mid-line:

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

Swapping in a real provider means pointing the service at the provider's real base URL.
Then either describe its endpoint as an `HttpOperationSpec`, writing the request and
response models and parsing the provider's events yourself, or keep the provider's SDK. To
keep the SDK, give its `httpx.AsyncClient` the transport from
`http_service_transport(ctx, spec)` — see
[Hand a vendor SDK the service](../integrations/http.md#hand-a-vendor-sdk-the-service). The
SDK's requests then carry the same acknowledgement, egress tag, credential and deadline
budget, and a request that leaves the service's base URL is refused. The loop, the palette
and the egress declaration stay as they are either way.

## Notes

- **Without a query policy every read field is advertised**, bookkeeping columns
  included — around 600 characters for a two-field model, in every tool the palette
  carries. A policy keeps the description small as well as the surface narrow, which is
  the same reason to write one twice over. To narrow only the palette and leave HTTP the
  spec's allow-set, pass `query_policies` to `operation_tools` — see
  [Agent tools](../data-events/agent-tools.md).
- **The filter grammar is repeated in every filter tool's schema** unless the palette is
  built with `shared_filter_grammar=True`, which moves it to `tools.filter_grammar` for
  the system prompt — see [Agent tools](../data-events/agent-tools.md).

- **A read-only palette is the default.** `operation_tools` refuses a command operation
  unless you ask for a command-capable toolset, so an agent that must act is a visible
  decision in the diff.
- **Delivering the streamed answer to a user** is a transport concern, not an agent one.
  The realtime recipes — [delivery to offline users](realtime-offline-delivery.md),
  [tenant-sharded realtime](tenant-sharded-realtime.md) — apply unchanged to the tokens
  `stream_answer` yields.
- **An operation whose read model is marked `sensitive` cannot be projected at all**, so
  credential material has no path into a tool palette.

## Related

- [Agent tools](../data-events/agent-tools.md) — what the bridge does and does not do
- [Expose an aggregate over MCP](expose-an-aggregate-over-mcp.md) — the same projection
  for an agent on the far end of a transport
- [Outbound HTTP](../integrations/http.md) — the egress declaration in full
- [Query syntax](../reference/query-syntax.md) — the DSL the agent writes filters in
