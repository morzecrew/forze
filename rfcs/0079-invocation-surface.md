# RFC 0079 — Invocation surface: naming the boundary an invocation entered through

- **Status:** 📝 Draft. This is the surface field RFC 0077 deferred to its own RFC (§8,
  §10 Q2, decision 16, §12 P3). Four questions wait for the maintainer (§10). It builds on
  the RFC 0077 P3 ingress change (branch `refactor/ingress-fastapi-socketio-ws`, unpublished), which lands
  first.
- **Scope:** One field, `InvocationMetadata.surface`, naming the framework boundary an
  invocation entered through: `http`, `mcp`, `cli`, and so on. It settles:
  - the vocabulary, and who may set it: the boundary, never the caller;
  - where each framework boundary mints it, including the three that still bind no metadata
    in some case after the RFC 0077 P3 ingress change;
  - what happens across a hop: nothing is carried, and each hop names itself;
  - its log field and span attribute;
  - its field in `AuditEntry`, what each audit store needs, and the RFC 0060 amendment;
  - why integration events do not carry it.

  It does **not** add a metric label, record the surface on versioned corrections, carry the
  correlation id across the self-hosted durable hop, or make the surface an authorization
  input (§8, §5.8). There is no new port and no new package.
- **Related:**
  - [`src/forze/application/execution/context/invocation.py`](../src/forze/application/execution/context/invocation.py):
    `InvocationMetadata`, the log keys, `_metadata_log_fields`;
  - [`src/forze/application/execution/ingress/invoke.py`](../src/forze/application/execution/ingress/invoke.py):
    `mint_invocation_metadata`;
  - [`src/forze/application/execution/observability.py`](../src/forze/application/execution/observability.py):
    the operation span's attributes;
  - [`src/forze/application/contracts/audit/value_objects.py`](../src/forze/application/contracts/audit/value_objects.py),
    [`src/forze/application/hooks/audit/plans.py`](../src/forze/application/hooks/audit/plans.py),
    [`src/forze_kits/integrations/audit/record.py`](../src/forze_kits/integrations/audit/record.py):
    the audit entry, the hook that builds it, and the collection that stores it;
  - [`src/forze/application/contracts/envelope.py`](../src/forze/application/contracts/envelope.py):
    the broker headers and their trust model;
  - the boundaries:
    [`forze_fastapi/middlewares/invocation.py`](../src/forze_fastapi/middlewares/invocation.py),
    [`forze_fastapi/realtime/ws.py`](../src/forze_fastapi/realtime/ws.py),
    [`forze_socketio/routing.py`](../src/forze_socketio/routing.py),
    [`forze_mcp/dispatch.py`](../src/forze_mcp/dispatch.py),
    [`forze_cli/ops/call.py`](../src/forze_cli/ops/call.py),
    [`forze_kits/integrations/inbox/consumer.py`](../src/forze_kits/integrations/inbox/consumer.py),
    [`forze_temporal/interceptors/codecs.py`](../src/forze_temporal/interceptors/codecs.py),
    [`forze_inngest/execution/registration.py`](../src/forze_inngest/execution/registration.py),
    [`forze_kits/integrations/durable/runner.py`](../src/forze_kits/integrations/durable/runner.py);
  - [RFC 0060](0060-audit-spec.md) (the audit entry this RFC amends);
  - [RFC 0077](0077-operation-cli.md) (the CLI, the shared ingress core, and the question this
    RFC answers), and its P3 ingress change on branch `refactor/ingress-fastapi-socketio-ws`, which gives WS
    command frames and Socket.IO events their metadata;
  - [RFC 0052](0052-versioned-facts-correction-lineage.md) (corrections, which record an actor
    the same way, §8).
- **Origin:** RFC 0077 §10 Q2. Without a surface field, an audit row written by a cron job
  through `forze ops` looks like one written over HTTP. The maintainer answered that P2 need
  not wait, and that the field changes RFC 0060's entry for every surface, so it gets its own
  RFC.

---

## 1. Summary

- **`InvocationMetadata.surface`** is a short token naming the boundary that minted the
  metadata. The framework's values live in an `InvocationSurface` StrEnum. An application's
  own boundary may pass its own token. The default is `unknown`.
- **Only boundary code sets it, from a constant.** No framework code reads it from a header,
  frame, message header or event envelope. The constructors are public, so application code
  can still set any value; against that, the mitigation is documentation only. It is
  attribution, never authority: nothing branches on it.
- **Each hop names itself.** An activity started from an HTTP request records `temporal`.
  The request is found through the shared `correlation_id`. There is no `origin_surface`.
- **P1 mints it at the nine framework boundaries.** WS command frames and Socket.IO events
  bind no metadata on main; the RFC 0077 P3 ingress change (branch
  `refactor/ingress-fastapi-socketio-ws`) makes them mint it, and P1 only adds their constant.
  Three cases still bind none after that change: the self-hosted durable runner, the inbox when
  a message has no correlation header, and Inngest when its envelope has no metadata. P1 makes
  them bind it. Log lines gain `invocation_surface` and operation spans gain
  `forze.invocation_surface`, because bare `surface` is already the port-call log key. Both
  read `unknown` when no metadata is bound at all.
- **P2 adds `surface` to `AuditEntry` and the audit collection.** It proposes adding
  `execution_id` and `correlation_id` in the same migration, because today the audit row has
  no correlation id at all (§2). The columns are nullable, but Postgres refuses to start
  until the migration adds them; MongoDB and the mock need nothing. RFC 0060 gets one appended
  decision row.
- **Integration events and the outbox do not carry it.**

## 2. Motivation

**An audit row does not say how its operation was reached.** RFC 0077 §10 Q2 named the case
and accepted the gap on two grounds (`rfcs/0077-operation-cli.md:733-738`):
- with an API-key identity, the actor names the job or the operator;
- "the correlation id ties the row to the command's logs".

The second ground does not hold. `AuditEntry` has no correlation id and no execution id
(`src/forze/application/contracts/audit/value_objects.py:61-85`), and neither does the stored
row (`src/forze_kits/integrations/audit/record.py:28-37`). A row joins the logs only by its
timestamp and its actor. So today:
- an API-key job leaves its principal as the actor, and nothing else;
- a static identity (RFC 0077 §5.3) leaves one shared actor for every operator, and nothing
  else;
- an anonymous call leaves a row with no actor and no subject, and nothing else about the
  caller.

**Surfaces carry different trust.** Each boundary resolves identity by its own rule:

| Boundary | Where the identity comes from |
|---|---|
| HTTP | the security middleware's verified credential |
| WebSocket, Socket.IO | an identity stored on the connection at connect time (`forze_fastapi/realtime/ws.py:272-276`, `forze_socketio/routing.py:384-386`) |
| MCP, CLI | the ingress identity resolver; the CLI may run a static identity behind an acknowledgement |
| Temporal | principal and actor chain from workflow headers, bound whenever a principal header is present, with no trust gate (`forze_temporal/interceptors/codecs.py:118-140`) |
| Inngest | the `_forze` envelope, and only with `bind_identity_from_event=True` (`forze_inngest/execution/registration.py:143-148`) |
| inbox | a tenant from a broker header, behind an opt-in; no principal |
| self-hosted durable runner | a tenant, no principal (`forze_kits/integrations/durable/operation_bridge.py:12-16`, `runner.py:391-400`) |

An auditor reading an actor needs to know which rule produced it. The row cannot say.

**Logs and spans cannot answer it either.** The log fields bound from metadata are the three
ids (`src/forze/application/execution/context/invocation.py:232-243`). The span attributes are
the same ids plus tenant and principal (`src/forze/application/execution/observability.py:462-487`).
"Everything MCP did this week" has no field to filter on.

## 3. Current state

All of this was verified against the tree at `24fb3e228`, and every `path:line` points
there. The WS and Socket.IO citations predate the RFC 0077 P3 ingress change on branch
`refactor/ingress-fastapi-socketio-ws`, which is described where it changes them.

**The metadata.**
- `InvocationMetadata` is a frozen, keyword-only attrs class with `execution_id`,
  `correlation_id` and `causation_id` (`invocation.py:28-40`).
- It is public: `forze.application.execution` exports it (`execution/__init__.py:8`), and 48
  files under `tests/` construct it.
- `get_execution_id`'s docstring draws a trust line: the execution id is minted server-side,
  while the correlation id "arrives in a client header and could be forged"
  (`invocation.py:95-104`). That holds at every boundary except Inngest, which decodes the
  execution id from its event envelope along with the correlation id
  (`forze_inngest/adapters/context.py:111-122`).

**Where metadata is minted and bound.**

| Boundary | Mint | Bind |
|---|---|---|
| HTTP | `InvocationMetadataMiddleware._decode_metadata` (`forze_fastapi/middlewares/invocation.py:117-131`) | `bind_metadata` for every HTTP scope (`:209`) |
| MCP | `mint_invocation_metadata()` (`forze_mcp/dispatch.py:91`) | `invoke_at_ingress` (`ingress/invoke.py:66`) |
| CLI | `mint_invocation_metadata(correlation_id=, causation_id=)` (`forze_cli/ops/call.py:149`) | the same |
| inbox | from the correlation and event-id headers, **only when a correlation header is present** (`forze_kits/integrations/inbox/consumer.py:175-190`) | `bind_metadata` |
| Temporal | `TemporalContextBinder.bind`: a fresh execution id, the correlation id from the headers, the upstream execution id as causation (`codecs.py:118-130`) | `bind` (`forze_temporal/interceptors/context.py:262`, `:275`) |
| Inngest | decoded from the `_forze` envelope, keeping the upstream execution id verbatim (`forze_inngest/adapters/context.py:111-122`) | `bind`, or identity only when the envelope has no metadata (`registration.py:150-163`) |
| WS command frames | none | `bind_identity` only (`ws.py:272-276`, `:773-784`) |
| Socket.IO events | none | `bind_identity` only; the operation is called directly, not through `run_operation` (`routing.py:384-390`) |
| self-hosted durable runner | none | the tenant only (`runner.py:391-400`) |
| agent tools | none, by design | the context "already bound by whichever boundary is running the agent loop" (`forze_kits/integrations/agent_tools/dispatch.py:171-172`) |

The HTTP middleware binds metadata for HTTP scopes only. It refuses a raw websocket scope
unless the app allows it, and lets an allowed one through without binding anything
(`middlewares/invocation.py:168-178`). So on main, WS frames get no metadata from it.

**The RFC 0077 P3 ingress change** (branch `refactor/ingress-fastapi-socketio-ws`, unpublished, landing
before this RFC is accepted) changes three rows of that table:
- WS command frames run through `invoke_at_ingress` with `mint_invocation_metadata()`
  (`ws.py:779-786` on that branch);
- Socket.IO's `_dispatch_event` binds fresh metadata from `mint_invocation_metadata()` and the
  connection's tenant around each event (`routing.py:400-407` on that branch);
- the HTTP middleware's `_decode_metadata` mints through `mint_invocation_metadata`, keeping
  its advisory header parsing.

After it, three framework cases still run operations without metadata:
- the self-hosted durable runner;
- the inbox, for a message without a correlation header;
- Inngest, for an event whose envelope has no metadata. It binds identity only, so the
  operation runs with whatever metadata was bound around it, or with none
  (`registration.py:160-163`).

Two harnesses also run operations with none: the DST engine (`forze_dst/engines/context.py:276`,
`:304`) and `run_traced_operation` (`execution/tracing/harness.py:103`). They are not
boundaries. They show the default path: an operation run outside any boundary records
`unknown`, and they need no value of their own.

The durable scheduler only enqueues runs
(`forze_kits/integrations/durable/scheduler.py:150-156`); the runner executes them.

**"Surface" already means two other things.**
- **The port vocabulary.** `PortCall.surface` is the dependency surface, such as
  `document_command` (`contracts/interception.py:26-27`), and interceptor matchers filter on
  it (`:46-65`). `LoggingInterceptor` logs it under the event key `surface`
  (`execution/interception/logging.py:66-90`). Port spans carry `forze.port.surface`
  (`execution/tracing/otel_port_proxy.py:58-61`). The DST trace records it too.
- **The exposure label.** `select_operations` and `refuse_sensitive` take `surface: str`, a
  display noun for error messages (`ingress/exposure.py:23`, `:77`): "MCP tool"
  (`forze_mcp/projection.py:41`) and "ops command" (`forze_cli/ops/declaration.py:21`).

The prose already uses "surface" for this RFC's concept: "a key authenticates identically on
every surface" (`ingress/authn.py:3`).

structlog merges bound context variables with `setdefault` ✅ (structlog 26.1.0,
`merge_contextvars`), so an event's own key wins over a bound one. A bound `surface` would be
replaced by the port's surface on every port-call line.

**Log fields, span attributes and metrics.**
- The log keys are constants (`invocation.py:17-23`): `execution_id`, `correlation_id`,
  `causation_id`, `principal_id`, `actor_id`, `tenant_id`, `idempotency_key`.
- The operation span carries `forze.` plus the same names (`observability.py:462-487`). The
  rule is: span attribute = `forze.` + log key.
- `forze.operations` and `forze.operation.duration` are labelled `forze.operation`,
  `forze.operation.kind` and `forze.outcome` (`observability.py:420`, `:450`). The docs forbid
  unbounded metric labels (`pages/docs/running-in-prod/grafana-stack.md:174-196`).

**The audit trail.**
- `AuditEntry` is frozen and keyword-only: `action`, `outcome`, `actor_id`, `subject_id`,
  `actor_ids`, `at`, `object_ref`, `metadata` (`value_objects.py:51-85`).
- It has one constructor in `src/`: `Audited.entry` (`hooks/audit/plans.py:117-143`). It reads
  the identity from `ctx.inv_ctx.get_authn()` and nothing from the metadata.
- The shipped store is one document collection, `audit_events` (`record.py:24`). Its DDL is
  documented, not migrated (`record.py:3-7`, `pages/docs/identity-tenancy-enc/audit.md:141-156`).
  `DocumentAuditPort.record` copies the entry field by field (`forze_kits/integrations/audit/port.py:36-54`).
  The fields the trail is queried by cannot be sealed (`record.py:89-112`).
- Postgres startup schema validation refuses a table that lacks a column
  (`forze_postgres/kernel/catalog/validation/validate_schema.py:143`). Without that
  validation the insert fails, and the audited operation fails closed (`audit.md:159-161`).
- **Precedent.** `actor_ids` was added after RFC 0060 shipped (#488). It took a column with a
  default, an `ALTER TABLE … ADD COLUMN`, a MongoDB note that legacy rows lack the field, and a
  changelog entry marked **Required migration** (`CHANGELOG.md:678`, `audit.md:159-173`).
  RFC 0060's decision table got no row for it.

**Envelopes that carry metadata across a hop.**
- `IntegrationEvent` carries `tenant_id`, `execution_id`, `correlation_id`, `causation_id` and
  `traceparent` (`contracts/outbox/value_objects.py:48-57`, `:77`). `InvocationOutboxEnricher`
  fills them (`execution/outbox/enrichment.py:43-59`). The Postgres, MongoDB and mock outbox
  adapters persist the ids.
- Broker headers: `forze_correlation_id` and its siblings (`contracts/envelope.py:35-47`). Their
  trust model: "any producer with broker access can forge them" (`:24-28`).
- Temporal headers: `Forze-Execution-ID`, `Forze-Correlation-ID`, `Forze-Tenant-ID`,
  `Forze-Principal-ID`, `Forze-Actor-IDs` (`codecs.py:21-25`). The causation id is not encoded
  (`:61`).
- The Inngest `_forze` envelope is "plaintext, attacker-controllable event data". Its metadata
  "is tracing context, not an authority, so it always propagates" (`registration.py:143-148`).
- `DurableRunRecord` persists `tenant_id` and no invocation metadata
  (`contracts/durable/function/run_store.py:100-133`). A self-hosted durable run loses the
  correlation id of whatever enqueued it.

## 4. Goals / Non-goals

**Goals**

- An audit row, a log line and an operation span each say which boundary the invocation
  entered through.
- No caller can choose that value.
- Every framework boundary sets it. A boundary that does not shows up as `unknown`.
- No constructor of `InvocationMetadata` or `AuditEntry` breaks.

**Non-goals**

- **An authorization input.** Which operations a surface may call is exposure, already decided
  per surface (RFC 0077 §5.2). See §5.8.
- **A provenance chain.** The field names one hop. Following the chain is the correlation id's
  job.
- **A client fingerprint.** No user agent, address or MCP client name. Those are request data,
  client-controlled, and some are personal data.

## 5. Design

### 5.1 The vocabulary

```python
class InvocationSurface(StrEnum):
    """The framework boundaries that mint invocation metadata."""

    HTTP = "http"            # forze_fastapi InvocationMetadataMiddleware
    WEBSOCKET = "websocket"  # forze_fastapi realtime WS command frames
    SOCKETIO = "socketio"    # forze_socketio events
    MCP = "mcp"              # forze_mcp
    CLI = "cli"              # forze ops call
    INBOX = "inbox"          # process_with_inbox
    TEMPORAL = "temporal"    # forze_temporal interceptors
    INNGEST = "inngest"      # forze_inngest functions
    DURABLE = "durable"      # the self-hosted runner in forze_kits.integrations.durable
    UNKNOWN = "unknown"      # no boundary said
```

- **One value per boundary implementation.** Each boundary resolves identity by its own rule
  (§2). The value tells a reader which rule produced the row's actor. So `websocket` and
  `socketio` stay apart, and so do the three durable engines.
- **No value for nested calls.** Agent tools, a saga step inside a run, and an operation that
  calls another all run inside an invocation some boundary already minted (§5.4).
- **No `schedule` value.** The scheduler enqueues and the runner executes. A scheduled run
  records `durable`, like any other run.
- **`unknown`, not `internal`.** The default means "no boundary said". `internal` would claim
  something nobody checked: a hand-written HTTP route without the middleware would be recorded
  as internal.
- **Open, validated.** The field is a `str`, so an application's own boundary, such as a gRPC
  server or its own queue worker, passes its own token. The token must match
  `^[a-z][a-z0-9_]{0,31}$`, checked at construction. Anything else raises `configuration` with
  code `invocation_surface_invalid`. The pattern keeps the value short, safe to log and free of
  personal data. It bounds a value's shape, not the number of distinct values. The framework
  contributes the nine above; an application adds as many as its own boundaries pass. The
  docs say to pass literals, never a value computed from a request, and that is the only
  bound on the count.

A closed enum would refuse an application's own boundary, or force it to record `unknown`,
which is the gap this RFC closes.

### 5.2 Where it lives, and who sets it

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InvocationMetadata:
    execution_id: UUID
    correlation_id: UUID
    causation_id: UUID | None = None
    surface: str = InvocationSurface.UNKNOWN    # a validated token (§5.1)


def mint_invocation_metadata(
    *,
    surface: str = InvocationSurface.UNKNOWN,
    correlation_id: UUID | None = None,
    causation_id: UUID | None = None,
) -> InvocationMetadata: ...
```

- **Boundary code sets it, from a constant.** This is RFC 0077 decision 6's rule for
  `--tenant`, applied to a weaker field: nothing a caller sends is trusted for it. These are
  never read for the surface:
  - an HTTP header, whether `X-Forze-Surface` or any other;
  - a query parameter;
  - a WS frame field or a Socket.IO payload key;
  - a broker header, a Temporal header, or a key in the Inngest envelope.

  The value is a literal in the boundary's source.
- **Application code is not covered.** `InvocationMetadata` and
  `mint_invocation_metadata(surface=)` are public, so any code in the process can construct
  metadata with any valid token, `cli` included. Nothing in the framework can tell that from a
  real boundary. The mitigation is documentation only: the docs say a boundary passes its own
  literal, and never one it read from the caller.
- **A keyword with a default.** No existing constructor breaks. The default, `unknown`, is
  honest and easy to grep for. A P1 test asserts that every framework call site passes its own
  value (§6).
- **One accessor.** `ctx.inv_ctx.get_metadata().surface`. There is no new context variable:
  the surface lives and ends with the metadata.

### 5.3 Minting at each boundary (P1)

| Boundary | Value | Change |
|---|---|---|
| `InvocationMetadataMiddleware` | `http` | `_decode_metadata` passes it (`middlewares/invocation.py:127` on main; a `mint_invocation_metadata` call after the RFC 0077 P3 ingress change). This covers generated routes and SSE |
| WS command frame | `websocket` | pass it to the `mint_invocation_metadata()` call the RFC 0077 P3 ingress change adds (`ws.py:786` on branch `refactor/ingress-fastapi-socketio-ws`; `ws.py:773-784` on main binds none) |
| Socket.IO event | `socketio` | pass it to the `mint_invocation_metadata()` call the RFC 0077 P3 ingress change adds (`routing.py:407` on that branch; `routing.py:384-390` on main binds none) |
| MCP | `mcp` | `dispatch.py:91` |
| `forze ops call` | `cli` | `call.py:149` |
| `process_with_inbox` | `inbox` | always bind metadata: the correlation id from the header or a fresh one, the event id as causation (`consumer.py:175-190`). Today nothing is bound when the correlation header is missing |
| Temporal interceptors | `temporal` | `TemporalContextBinder.bind` (`codecs.py:125-129`) |
| Inngest function | `inngest` | the envelope decode passes it when it builds the metadata (`adapters/context.py:115`); the envelope is never read for it. `_bind_invocation` mints fresh metadata with it when the envelope has none (`registration.py:150-163`). Today that case keeps whatever metadata was bound around it, whichever boundary bound it |
| self-hosted durable runner | `durable` | mint per execution attempt, next to the tenant binding (`runner.py:391-400`). Today nothing is bound. Which ids an attempt gets is §10 Q4 |

Three rows, the durable runner, the inbox and Inngest without envelope metadata, bind metadata
where none was bound before, even after the RFC 0077 P3 ingress change. That has effects beyond
the surface, listed in §9. The same effects for WS and Socket.IO belong to that change, which
records them.

### 5.4 Hops: each boundary names itself

An operation run by a Temporal activity that an HTTP request started records `temporal`. The
HTTP request is found through its correlation id, which the hop carries.

| Hop | Correlation id | Surface |
|---|---|---|
| HTTP → outbox → broker → inbox | carried (`enrichment.py:53`, the broker header, `consumer.py:175`) | not carried; the consumer records `inbox` |
| caller → Temporal | carried (`codecs.py:59`, `:123`) | not carried; records `temporal` |
| caller → Inngest | carried in `_forze` | not carried; records `inngest` |
| caller → self-hosted durable run | **not carried** (`run_store.py:100-133`) | not carried; records `durable` |
| agent loop → tool call | the same invocation | the same; the agent appears in `actor_ids` |

There is no `origin_surface`, for three reasons:
- **No hop offers a channel the receiver can trust for it.** Broker headers
  (`envelope.py:24-28`) and the Inngest envelope (`registration.py:143-148`) are
  producer-controlled, and the Temporal codec binds its headers as sent (`codecs.py:118-140`).
  An origin read from any of them is the caller-chosen attribution decision 1 refuses.
- **One field cannot hold a chain.** HTTP → Temporal → outbox → inbox has three hops before the
  row. `origin_surface` would keep the first and drop the rest. The correlation id already
  reaches all of them.
- **It would change every envelope:** the Temporal codec, the Inngest envelope, the broker
  header set, and the outbox row on three backends.

The self-hosted durable hop drops the correlation id today. That gap is real and predates this
RFC. Closing it needs a `DurableRunRecord` column on every run store, which are among this
repository's most-fixed files. §8 names it.

### 5.5 Log field and span attribute

| Channel | Name | When |
|---|---|---|
| structlog context | `invocation_surface`, as the constant `SURFACE_KEY` beside `EXEC_ID_KEY` (`invocation.py:17-23`) | whenever metadata is bound, from `_metadata_log_fields` (`:232-243`), so `bind_metadata` and `bind` both carry it. With none bound, `unknown`, bound at a top-level operation's entry (below) |
| operation span | `forze.invocation_surface` | always: the metadata's value, or `unknown` when none is bound, in `_span_attributes` (`observability.py:468-475`) |

- **Not bare `surface`.** It is the port-call log key, and structlog keeps an event's own key
  over a bound one (§3). A bare key would vanish from exactly the port-call lines, and a query
  for `surface="http"` would mix two meanings. The span attribute follows the existing rule,
  `forze.` plus the log key. `forze.surface` beside `forze.port.surface` would invite the same
  confusion.
- **Emitted even when no metadata is bound.** Without metadata there is nothing for
  `_metadata_log_fields` to bind, so P1 adds one step: a top-level operation's entry
  (`execution/operations/run/invoke.py:179-190`, where the drain gate admits it) binds
  `invocation_surface="unknown"` for the call when `get_metadata()` returns `None`. Nested calls
  inherit it. The cost on the common path is one context-variable read. The default exists to
  be seen, including on the DST and harness paths (§3).
- **A log field, not a Loki label.** It stays inside the JSON line, like `correlation_id`
  (`grafana-stack.md:179-180`).
- **No metric label here.** A per-surface operation rate is plausible. But a new label changes
  the series of a shipped metric for every user, so it waits for a consumer (§8).

### 5.6 The audit entry and its stores (P2)

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuditEntry:
    ...                                    # the existing fields, unchanged
    surface: str | None = None             # from the bound InvocationMetadata
    execution_id: UUID | None = None       # §10 Q1
    correlation_id: UUID | None = None     # §10 Q1
```

- **The hook fills them, never the arguments.** `Audited.entry` reads
  `ctx.inv_ctx.get_metadata()` next to `get_authn()` (`plans.py:117-143`). With no metadata
  bound, it records `surface="unknown"` and no ids. This is RFC 0060 decision 2's rule, that
  the actor comes from the invocation context and never from the payload, extended to the new
  fields.
- **What `None` means.** The row was written before the field existed, or by an `AuditPort`
  writer that does not set it. `unknown` means a row written later, by an invocation no
  boundary named. A reader can tell the legacy gap from a live one.
- **The ids are tracing context, not evidence.** They join the row to the log lines that
  carry the same values, and prove nothing about the caller. Over HTTP the client chooses the
  correlation id (`middlewares/invocation.py:122-125`). On an `inngest` row both ids come from
  the event envelope, not from the server (`forze_inngest/adapters/context.py:111-122`). The
  audit records them as they were bound, on every surface; `surface` tells a reader where they
  came from. This RFC does not change how Inngest derives them.
- **The collection.** `AuditDoc`, `AuditCreate` and `AuditRecord` gain the fields
  (`record.py:28-81`). `DocumentAuditPort.record` copies them (`port.py:38-48`). They join
  `_QUERIED_FIELDS`, so sealing one is refused (`record.py:89-112`). A trail is filtered with
  the ordinary document query, for example `{"$values": {"surface": "cli"}}`.

**Per backend.**

| Backend | Change | Rows written before |
|---|---|---|
| Postgres | **a required migration:** `ALTER TABLE audit_events ADD COLUMN surface text, ADD COLUMN execution_id uuid, ADD COLUMN correlation_id uuid;`. Until it runs, startup schema validation refuses the table; without validation, every audited operation fails closed. For query-by-surface the docs suggest an index such as `CREATE INDEX ON audit_events (surface, at);`, with `tenant_id` first when the trail is tenant-aware | `NULL` |
| MongoDB | none | the fields are absent and read back as `None`. `{"$values": {"surface": {"$null": true}}}` matches them, as the `actor_ids` note shows (`audit.md:167-173`) |
| mock | none | — |
| a custom `AuditPort` | reads the new fields if it wants them | its own |

- **No backfill.** Nothing records how an old row's operation was reached, so any value written
  into it would be invented.
- **Additive, with a required migration.** The columns are nullable, so no API breaks and no
  row changes. What breaks is startup: a Postgres trail refuses to start until the migration
  runs. That is the `actor_ids` shape (`CHANGELOG.md:678`), and P2 ships the same way: a
  **Required migration** marker and the SQL in its changelog entry and in the docs (§10 Q3).

**The RFC 0060 amendment.** RFC 0060's prose stays as written (INDEX policy). When P2 lands,
its decision table gets one appended row, worded roughly like this:

> `AuditEntry` gains `surface` (and, if RFC 0079 §10 Q1 is answered yes, `execution_id` and
> `correlation_id`). The audit hook reads them from the bound `InvocationMetadata`, never from
> the operation's arguments, as row 2 reads the actor. A row with no metadata records
> `surface="unknown"`. See RFC 0079.

RFC 0060 §5.6's field list then stands superseded by that row.

### 5.7 Integration events and the outbox: not carried

`IntegrationEvent` gains no surface.
- The consumer's invocation records its own surface, `inbox`.
- The publisher's surface is reached through the event's correlation id, which the event
  already carries (`contracts/outbox/value_objects.py:54`).
- Carrying it would add a column to the outbox row on Postgres, MongoDB and the mock: a
  required migration for every outbox user, for a value no consumer could trust (§5.4).

### 5.8 Security and privacy

- **Not secret, not personal data.** The value is one of a few tokens fixed in code. It needs
  no scrubbing and no sealing.
- **Not spoofable by a caller.** No framework code reads the surface from input (§5.2), and
  the tests send the obvious forgeries (§6). Application code is a different matter: it can
  construct metadata with any valid token, and the only mitigation is documentation.
- **Attribution, never authority.** No framework hook, policy, guard or exposure check reads
  the surface, and the docs tell applications not to. An application's own boundary can set
  any valid token, so a guard such as "only if `surface == "cli"`" proves nothing. The control
  that answers "may this surface call this operation" exists already: exposure, an allowlist
  per surface (RFC 0077 §5.2).
- **Not echoed.** No response header, error object or CLI output carries it. A client has no
  use for it.

### 5.9 Compatibility

| Change | What breaks |
|---|---|
| `InvocationMetadata.surface`, default `unknown` | nothing; the 48 test files keep constructing it as before |
| `mint_invocation_metadata(surface=…)`, default `unknown` | nothing |
| the new log key and span attribute | nothing, unless a strict log schema rejects unknown keys |
| metadata bound at the durable runner, the inbox and Inngest without envelope metadata | nothing in the API; the behaviour changes are in §9 |
| the new `AuditEntry` fields, default `None` | nothing for constructors |
| the `audit_events` columns | nothing in the API; **a Postgres trail refuses to start until it is migrated** |
| persisted metadata: outbox rows, Temporal headers, the Inngest envelope, broker headers | nothing; none gains the field (§5.4, §5.7), and `DurableRunRecord` persists no metadata |

An application boundary that constructs `InvocationMetadata` itself keeps working, and records
`unknown` until it passes its own token.

### 5.10 Alternatives considered

- **Derive it from the correlation or causation id.** The ids are opaque uuid7 values. Only
  the logs map an id to a boundary, and logs are sampled, shipped elsewhere and kept for less
  time than an audit trail.
- **Put it in the actor**, as a principal per surface or a principal kind for the CLI. It
  conflates who acted with how they got in. `PrincipalKind` is `user | service`
  (`contracts/authz/types.py:5`). An anonymous call has no actor to hold it, and a static
  identity has one actor for everyone, which is the case §2 starts from.
- **One audit collection per surface.** It splits the trail. "Everything this actor did"
  becomes a union across tables, and every new surface needs a new table.
- **Store it in the row's `metadata`.** That field is the application's allowlisted data. The
  allowlist would refuse an undeclared key. The field may be sealed (`record.py:107-112`), and
  then it cannot be queried. And the application's own metadata callable could write any value
  into it.
- **A closed enum.** Bounded by construction, but it refuses an application's own boundary or
  forces it to record `unknown`. The open field gives up that bound: the pattern fixes a
  value's shape, not how many values exist. That is acceptable because no metric label
  depends on the field (§5.5), and logs and audit rows tolerate a handful of extra values.
- **A separate context variable** instead of a field on the metadata. It would let a surface
  outlive or disagree with the metadata it describes, for example when a hop rebinds the
  metadata and not the surface. Every boundary would need two binds, and the audit hook and the
  span would read two places. A field lives and ends with the ids it belongs to.
- **OpenTelemetry baggage, or a span attribute only.** Baggage crosses process hops in a W3C
  header, so a caller can set it and it propagates downstream: the opposite of a per-hop value
  only the boundary sets. A span-only attribute depends on tracing being on and sampled
  (`instrument_operations` is opt-in), and an audit row must not.
- **Trust a gateway's header,** say "this came through the partner API". Any client can send
  the same header. An application that needs two values mounts two apps, each with its own
  middleware value (§8).
- **Propagate an `origin_surface`.** See §5.4.
- **Name it `ingress` everywhere.** The field, the log key, the span attribute and the column
  would all be `ingress`, with no collision anywhere. It loses because "surface" is already the
  word the code and RFC 0077 use for HTTP, MCP and the CLI (`ingress/authn.py:3`), and
  `ingress="http"` reads like a Kubernetes ingress. Keeping "surface" costs the
  `invocation_surface` spelling in the two flat namespaces (§10 Q2).
- **A required `surface`, with no default.** It forces every boundary to say. It also breaks 48
  test files and every application boundary that builds metadata, for a rule the framework's
  own tests can enforce at its own call sites.

## 6. Tests

**P1**

1. **Vocabulary.** The default is `unknown`. Every `InvocationSurface` member matches the token
   pattern. A token outside it raises `configuration` with `invocation_surface_invalid`.
2. **Each boundary names itself.** One test per row of §5.3 asserts
   `get_metadata().surface` inside an operation that boundary runs. A sweep test fails when a
   framework call site of `mint_invocation_metadata` or `InvocationMetadata(` passes no
   `surface`. That includes the Inngest envelope decode (`forze_inngest/adapters/context.py:115`),
   which passes `surface="inngest"`. The sweep exempts nothing in `src/`.
3. **Forgeries are ignored.**
   - HTTP with `X-Forze-Surface: cli`, `Forze-Surface: cli` or `X-Invocation-Surface: cli`
     records `http`.
   - A broker message with `forze_surface` or `invocation_surface` headers records `inbox`.
   - An Inngest envelope with `invocation_surface: "cli"` records `inngest`, with and without
     `bind_identity_from_event`.
   - A Temporal header `Forze-Surface: cli` records `temporal`.
   - A WS frame and a Socket.IO payload with a `surface` key record `websocket` and `socketio`.
4. **Hops.** An HTTP operation publishes an event that `process_with_inbox` consumes. The
   consumer records `inbox` and the publisher's correlation id. A Temporal activity started
   from HTTP records `temporal` and the same correlation id.
5. **Nesting.** An agent-tools call inside an HTTP invocation records `http`.
6. **Logs.** A captured line inside an operation carries `invocation_surface`. A port-call line
   inside it carries both the port's `surface` and `invocation_surface`. An operation run with
   no metadata bound, through `run_traced_operation`, logs `invocation_surface="unknown"`.
7. **Spans.** The operation span carries `forze.invocation_surface`, and `unknown` when no
   metadata is bound.
8. **The new bindings.** Durable runs, inbox messages without a correlation header, and
   Inngest events without envelope metadata now carry `execution_id` and `correlation_id` in
   their log lines and in the events they publish. The durable-attempt idempotency test from
   §10 Q4 runs here. WS frames and Socket.IO events already gain metadata, with tests, in the
   RFC 0077 P3 ingress change; test 2 covers their surface value.

**P2**

9. **The row.** An audited operation records the surface and the ids of the bound metadata. A
   payload naming a surface is ignored, asserted the way RFC 0060 asserts it for the actor.
   With no metadata bound, the row records `unknown` and no ids.
10. **Postgres.** The integration battery's DDL gains the columns
    (`tests/integration/test_forze_postgres/test_pg_audit_trail.py:52-66`). A table without
    them is refused at startup with `missing columns [...]`. A row round-trips and is
    queryable by surface.
11. **MongoDB.** A legacy row without the fields reads back as `None` and matches the `$null`
    query. There is no MongoDB audit battery today; this adds the first.
12. **Sealing.** `audit_record_spec(encryption=…)` that seals `surface` is refused.

**Not tested:** that no application hook branches on the surface. That rule is for
applications, and the docs carry it.

## 7. Docs

- `running-in-prod/observability.md`: the span attribute in its table (`:36-40`), the log
  field, and the vocabulary: the values, their boundaries, and the token rule for an
  application's own. The other pages link here.
- `identity-tenancy-enc/audit.md`: the new fields, the DDL (`:141-156`), the `ALTER TABLE` and
  the MongoDB note next to the ones for `actor_ids` (`:159-173`), and one sentence: the surface
  is attribution, not authority.
- `running-in-prod/operations-cli.md`: what a row written through `forze ops` now records, next
  to "every operator shares one audit actor" (`:72`).
- `integrations/fastapi.md`: the middleware records `http` and reads no header for it.
- The realtime, Socket.IO, Temporal, Inngest and inbox pages: one line each naming the value,
  and the new metadata binding where there was none.

## 8. Out of scope

- **A metric label on `forze.operations`.** It waits for a dashboard that asks for
  per-surface rates.
- **Correlation across the self-hosted durable hop.** It needs a `DurableRunRecord` column on
  every run store. Without it, a durable run's audit rows cannot be joined to the request that
  enqueued it, whether or not this RFC ships. It is its own change.
- **Versioned corrections (RFC 0052).** A correction records its actor the way an audit row
  does (`forze_kits/domain/versioned/correction.py:33-46`). Adding the surface there is a second
  schema change, demand-gated.
- **A configurable value on the HTTP middleware.** One process serving a public API and an
  internal admin API may want two values. Demand-gated; the field already accepts any valid
  token.
- **Moving FastAPI, Socket.IO and the WS route onto `ingress`** (RFC 0077 §12 P3). That is the
  RFC 0077 P3 ingress change on branch `refactor/ingress-fastapi-socketio-ws`. This RFC depends on it for
  WS and Socket.IO metadata, and does not re-specify it.

## 9. Risks

- **The new bindings change behaviour.** Three cases gain metadata: the durable runner, the
  inbox without a correlation header, and Inngest without envelope metadata (§5.3):
  - their log lines and spans gain ids;
  - the events they publish gain `execution_id` and `correlation_id`;
  - their idempotency claims gain an owner, because the Postgres, MongoDB, Redis and mock
    stores fence on `get_execution_id` (e.g.
    `forze_postgres/execution/deps/factories/idempotency.py:40`). Fencing applies only when
    both the claim and the caller carry an owner (`contracts/idempotency/ownership.py:34-37`),
    so these claims were unfenced before. §10 Q4 settles what that means across durable
    attempts.

  Each of these is what HTTP already does. Mitigation: each boundary moves against its own
  suite, and the changelog names the change.
- **Read as authority.** Someone writes `if surface == "cli"` into a guard. Mitigation:
  decision 2, the docs, and no framework code that reads the field.
- **Forged by application code.** The constructors are public, so code in the process can
  record any valid surface. The mitigation is documentation only; nothing enforces it.
- **The Postgres migration.** Forgotten, it refuses startup, or fails every audited operation
  closed. Mitigation: users have walked the same path once for `actor_ids`, and the schema
  validation names the missing columns.
- **`unknown` everywhere.** An application whose own boundaries never pass a token sees
  `unknown` on every row. That is the field working. The docs say how to set it.
- **The hot path.** Validation runs on every construction. Decision 10 says how P1 bounds
  its cost.
- **The names ship.** `invocation_surface` and the `surface` column are public once released.
  §10 Q2 settles them before code.

## 10. Unresolved questions

1. **Should P2 also add `execution_id` and `correlation_id` to `AuditEntry`?** RFC 0077
   decision 16 assumed the row carries the correlation id, and it does not (§2). Adding them
   later means a second required migration on every Postgres trail. *Lean: yes, in the same
   migration.*
2. **The name.** `surface` on the object and the column, with `invocation_surface` in logs and
   spans (§5.5)? Or `ingress` everywhere (§5.10)? *Lean: `surface`, the word the code already
   uses.*
3. **How P2 ships.** With a required migration, as `actor_ids` did, or behind an opt-in
   `audit_record_spec(record_surface=True)` that keeps an unmigrated trail starting? *Lean:
   the required migration. A trail where some applications record the surface and others do
   not is the inconsistency the field removes, and the flag would outlive the migration.*
4. **Which ids does a self-hosted durable run attempt bind?** Each attempt is an invocation.
   - **Execution id.** A fresh one per attempt makes each attempt the owner of the idempotency
     claims it takes. Within one attempt nothing changes: the operation hook begins and
     commits in the same invocation, and the runner binds no idempotency key, so the hook takes
     no claim there unless application code binds one (`hooks/idempotency/plans.py:150-153`).
     Across attempts, a claim begun in one attempt and committed in a later one is now refused
     with `conflict` ("claim reclaimed"), where before it went unfenced. That happens only
     when a body calls the idempotency port directly across memoized steps and a recovery
     resumes it. The other side of the same change: a superseded attempt's late commit can no
     longer overwrite its successor's claim, which is the fence's purpose. The inbox is the
     same with one owner per delivery.
   - **Correlation id.** A fresh one per attempt, or the run id (a uuid7), so every attempt of
     one run shares it.

   *Lean: a fresh execution id per attempt and the run id as the correlation id. The docs say
   a claim must begin and end within one attempt.* P1 adds the test: a durable body begins a
   claim in one step, the run is recovered, and the next attempt's commit gets the documented
   outcome; and a superseded attempt's late commit does not replace its successor's claim.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Only boundary code sets the surface, from a constant in its source. No framework code reads it from a header, query parameter, frame field, payload key, broker header, Temporal header or event-envelope key. Attribution a caller can choose is worse than none, and reopening this would change what every row already written can be trusted for. Application code can still construct metadata with any valid token, since the constructors are public; against that, the mitigation is documentation only. |
| 2 | `LOCKED` | The surface is attribution, never authority. No framework hook, policy, guard or exposure check reads it, and the docs tell applications not to. An application's own boundary can set any valid token, so a check on it proves nothing; per-surface access is exposure (RFC 0077 §5.2). |
| 3 | `ASSUMED` | The field is an open `str`, validated against `^[a-z][a-z0-9_]{0,31}$` at construction (`configuration`, `invocation_surface_invalid`), with the framework's values in an `InvocationSurface` StrEnum. A closed enum would refuse an application's own boundary. |
| 4 | `ASSUMED` | One well-known value per framework boundary implementation, because each resolves identity by its own rule: `http`, `websocket`, `socketio`, `mcp`, `cli`, `inbox`, `temporal`, `inngest`, `durable` (the self-hosted runner), and the default `unknown`. There is no value for nested calls, for the scheduler, or for "internal". |
| 5 | `ASSUMED` | `InvocationMetadata.surface` and `mint_invocation_metadata(surface=)` are keywords that default to `unknown`, so no constructor breaks. A test asserts that every framework call site passes its own value. |
| 6 | `ASSUMED` | Each hop's boundary names itself, and no envelope carries the surface. There is no `origin_surface`; the origin is reached through `correlation_id`. Adding one later is additive, but it would read from forgeable envelopes (§5.4). |
| 7 | `ASSUMED` | A nested call keeps the enclosing invocation's surface: agent tools, an operation that calls another, a saga step inside a run. An agent is attributed by `actor_ids`, not by the surface. |
| 8 | `ASSUMED` | The log key is `invocation_surface` and the span attribute is `forze.invocation_surface`. Both are emitted on every operation: from the metadata, or as `unknown` when none is bound (a top-level operation's entry binds the log key; `_span_attributes` falls back). Bare `surface` is the port-call log key, and structlog lets the event's key win over a bound one. Subject to §10 Q2. |
| 9 | `ASSUMED` | P1 binds metadata in the three framework cases that still run operations without it after the RFC 0077 P3 ingress change: the self-hosted durable runner, inbox messages without a correlation header, and Inngest events whose envelope has no metadata (Inngest stops inheriting whatever was bound around it). WS command frames and Socket.IO events get their metadata from that change (branch `refactor/ingress-fastapi-socketio-ws`), and P1 only adds their constant. Which ids a durable attempt binds is §10 Q4. |
| 10 | `ASSUMED` | The token is validated when `InvocationMetadata` is constructed, by a set lookup for the well-known values and the pattern otherwise. P1 measures it with the perf suite (CONTRIBUTING → Performance regression gate). If it regresses the gate, validation moves to `mint_invocation_metadata` and the framework's own call sites, and direct construction goes unchecked. |
| 11 | `ASSUMED` | `AuditEntry` gains an optional `surface` string, `None` by default. The audit hook fills it from the bound metadata, never from the arguments, and records `unknown` when no metadata is bound. `None` is left for legacy rows and for writers that do not set it. This extends RFC 0060 decision 2's rule. |
| 12 | `ASSUMED` | `AuditEntry` also gains `execution_id` and `correlation_id` in the same change, unless the answer to §10 Q1 is no. The audit records them as tracing context, not evidence: over HTTP the client chooses the correlation id, and on an `inngest` row both come from the event envelope. |
| 13 | `ASSUMED` | The audit collection stores the new fields as nullable, queryable fields that cannot be sealed. The change is additive, but a Postgres trail refuses to start until a required migration runs (`ALTER TABLE … ADD COLUMN`, no backfill, an optional index on `surface`); MongoDB and the mock need none. P2 ships with a **Required migration** marker and the SQL, unless the answer to §10 Q3 is the opt-in flag. |
| 14 | `ASSUMED` | Integration events and the outbox do not carry the surface. The consumer records `inbox`, and the publisher's surface is reached through the event's correlation id. |
| 15 | `ASSUMED` | RFC 0060's prose stays. When P2 lands, its decision table gets one appended row that cites this RFC (§5.6). |
| 16 | `ASSUMED` | No metric label, no surface on versioned corrections, no correlation across the self-hosted durable hop, and no configurable HTTP value in this RFC. Each waits for a named consumer (§8). |

## 12. Phasing

- **P1: the field, the minting, logs and spans.** §5.1–§5.5, tests 1–8, and the observability
  and integration docs. It lands after the RFC 0077 P3 ingress change. Changelog: Added (the
  field, the vocabulary, the log key, the span attribute) and Changed (the durable runner, the
  inbox and Inngest now bind metadata where they bound none). It needs the answer to §10 Q4.
- **P2: the audit trail.** §5.6, tests 9–12, the audit and CLI docs, and the appended RFC 0060
  row. Changelog: Added, with a **Required migration** marker and the SQL. It waits for the
  answers to §10 Q1–Q3.
- **P3: demand-gated, each on a named consumer:**
  - a metric label on `forze.operations`;
  - correlation across the self-hosted durable hop;
  - the surface on versioned corrections;
  - a configurable value on the HTTP middleware.
