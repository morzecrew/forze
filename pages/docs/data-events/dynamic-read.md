---
title: Dynamic read
icon: lucide/scan-search
summary: Run statements another program wrote — under framework-owned read-only enforcement, tenancy confinement, limits and taxonomy
---

Some products decide *what to query* at runtime. A BI tool assembles a customer's
data model per project and stores widget SQL in a catalog; a semantic layer
compiles a measure into a statement; a report builder turns a saved definition
into a query. The statement is data by the time it reaches your handler, so no
amount of wiring can register it in advance — and the only path that ever
accepted it was the raw client, which is permitted precisely because it is
ungoverned: you own validation, tenancy, and portability.

That posture is fine for a migration script run twice a year. It is the wrong
one for the statement your product executes on every dashboard render. The
**dynamic read** plane is that hot path with the governance put back: the
framework, not the caller, owns read-only enforcement, tenancy confinement,
resource limits, and the error taxonomy for statements it cannot inspect.

## Where it sits

Four ways a handler reaches the database, sorted by when the statement is known:

| Approach | SQL known at | Output shape known at | Governance |
| --- | --- | --- | --- |
| Raw client | runtime | runtime | none — by documented policy |
| **Dynamic read** | **runtime** | **runtime** | **read-only enforced by the engine; tenancy = container confinement; limits, taxonomy, capture** |
| [Procedures](procedures.md) | wiring | wiring | full; command-only |
| [Analytics](analytics.md) | wiring | wiring (`select_run` moves it to runtime) | full |

The missing cell was *runtime statement text with framework governance*, and it
is deliberately a separate plane rather than a method on the analytics port.
Analytics promises that handlers never pass SQL strings; grafting a dynamic call
onto it would rot that promise for every analytics route and hide the dangerous
capability inside a familiar one. Kept apart, a reviewer can grep the wiring and
find every route that has it.

## The shape in code

A spec is one governed *surface*, not one statement. Every statement executed
through it shares these caps and the route's wiring:

```python
from forze.application.contracts.dynamic_read import DynamicReadSpec

WIDGETS = DynamicReadSpec(
    name="widgets",
    row_cap=10_000,            # exceeding it raises; there is no "unlimited"
    max_statement_bytes=65_536,
)
```

There are no input or output type parameters, because both shapes are runtime
data — that is the plane's definition. A handler resolves the port and passes
text plus bound parameters:

```python
port = ctx.dynamic_read.query(WIDGETS)

rows = await port.run(widget.sql, {"since": since})        # list of mapping rows
typed = await port.select(RevenueRow, widget.sql, {"since": since})
```

`run` returns mapping rows in the statement's column order — the shape a widget
renders from, and the only honest one when the columns are chosen at runtime.
`select` is the analytics `select_run` twin: the output type is a call-site
argument, because the caller that compiled the statement is the one that knows
what it selects. The port is **read-plane**, so a `QUERY` operation can use it;
that is the whole point, and it deliberately inverts the procedures plane's
command-only stance.

Parameters are bound by the engine, never formatted into the text. Two
consequences worth knowing up front: a literal `%` in the statement must be
doubled (`LIKE 'foo%%'`), and there is **no pagination** — a read that needs to
page past `row_cap` is a mis-authored statement, and offset paging over runtime
SQL invites exactly the fan-out costs the caps exist to surface.

## Who wrote the statement

This is the design input everything else follows from, and the wiring author has
to answer it because the framework cannot: it never sees the statement until it
is time to run it. `provenance` has no default, so a route cannot be wired
without naming its threat tier.

| Tier | Statement author | Example | Required confinement |
| --- | --- | --- | --- |
| **A — trusted** | your own release artifacts, selected at runtime | a shared visualization catalog; a compiler's output from reviewed templates | engine read-only + namespace routing + limits |
| **B — untrusted** | a program whose output nobody reviews per statement and that is not *crafted to escape* | generated SQL from your templates, user-configurable report definitions | tier A **plus** `SET LOCAL ROLE` to a schema-confined role, or a routed (dedicated) client |
| **C — adversarial** | an author who may deliberately construct escape gadgets | an end-user SQL console, a hostile tenant | **dedicated tier only** — separate credentials per tenant |

Tier C is a documentation stance, not a config value, and the reason is
structural rather than a missing feature. On a shared connection the statement
and the adapter wield the same identity: any privilege the adapter can invoke
mid-session, a hostile statement can invoke too. Postgres 16's `GRANT … WITH SET
FALSE` cannot fix it either — it disarms the adapter's own role switch
symmetrically. The only scoping key a statement cannot forge from inside the
session is the connection's login identity, which makes "adversarial" an
operator's choice of topology rather than a flag.

!!! warning "What role confinement is, exactly"

    A `NOLOGIN` role with `USAGE` on one schema blocks cross-schema reads for any
    statement that simply *references* the wrong relation — the entire mistake
    class a non-adversarial generator produces. Against a deliberately crafted
    statement it is porous: direct `FROM` references are permission-checked at
    executor startup, but dynamic-SQL builtins like `query_to_xml` check their
    inner query at *execution* time, so one statement can call
    `set_config('role', …)` and then read across schemas. Treat the role as
    mistake-proofing plus defence in depth, and reach for the dedicated tier when
    the author might be hostile.

    What survives every such gadget is `SET TRANSACTION READ ONLY`, which is
    sticky for the transaction's lifetime: writes stay impossible throughout.

## Mapping it to Postgres

One config maps a route to its container, its confinement, and its clock:

```python
from datetime import timedelta
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig

PostgresDynamicReadConfig(
    provenance="trusted",                            # mandatory — no default
    query_schema=lambda tid: f"project_{tid.hex}",   # SET LOCAL search_path
    role=None,                                       # SET LOCAL ROLE (tier B)
    statement_timeout=timedelta(seconds=5),          # always on
    tenant_aware=True,
)
```

Wire it on `PostgresDepsModule(dynamic_reads={"widgets": config})` and every
statement on the route runs as:

```text
BEGIN READ ONLY                        -- sticky; survives role games
SET LOCAL statement_timeout = …        -- always on
SET LOCAL search_path = …              -- when query_schema is set
SET LOCAL ROLE …                       -- when role is set
<statement>  via the extended protocol -- one command, server-enforced
```

Every one of those lines is a refusal **Postgres** makes. No SQL is parsed,
matched, or rewritten anywhere in the plane: `INSERT` is refused because the
transaction is read-only, `'SELECT 1; DROP …'` is refused because the extended
query protocol carries one command, and a cross-schema read is refused because
the role lacks the grant. A parser the framework maintains is a parser a
statement outgrows, so there is none.

The statement runs on its **own** connection and its own root transaction — a
read-only mode applied inside a caller's transaction would silently not apply,
and that is this plane's one load-bearing guarantee. It also means a dynamic read
does not see the caller's uncommitted writes, which is the right trade for a read
plane.

## Tenancy

Tenancy here is the **container**, not a predicate. A statement the framework
cannot read cannot be trusted to carry one, so the boundary is the schema, role,
or database it runs inside.

| Tier | Mechanism | Posture |
| --- | --- | --- |
| `namespace` | per-tenant schema via `query_schema` | supported — the statement runs in the tenant's own schema |
| `dedicated` | a routed client per tenant | supported — strongest isolation |
| `tagged` | a `%(tenant)s` predicate inside the statement | **refused at wiring** |

!!! danger "A tenant-aware route on the tagged tier fails at wiring"

    Wiring `tenant_aware=True` without a per-tenant `query_schema` or a routed
    client raises `dynamic_read_tagged_refused` at startup. The registered-SQL
    planes keep `tagged` because their compensating controls exist — frozen text,
    a freeze-time placeholder guard, review — and none of them survive a statement
    written at runtime. Worse, the failure mode is the bad one: a missing
    predicate does not error, it *succeeds*, with another tenant's rows in a
    correctly-rendered widget. On namespace or dedicated the identical mistake
    either fails loudly or stays inside the tenant's container.

    A route declaring `provenance="untrusted"` without a role or a routed client
    fails the same way, as `dynamic_read_untrusted_unconfined`.

The tenant id is still bound as `%(tenant)s` when the statement references it, so
a trusted statement can carry a predicate *in addition to* its container. That is
convenience, not the boundary — referencing a placeholder proves reference, never
scope. Set the schema up with the
[tenant provisioner](../identity-tenancy-enc/multi-tenancy.md#provisioning-per-tenant-infrastructure),
and see the [tenancy matrix](../reference/tenancy-matrix.md) for where the plane
sits against the rest.

## Limits and errors

Caps ship on with real values, and exceeding the row cap **raises** rather than
truncating — a truncated page reads as a complete one, and a dashboard rendered
from it is confidently wrong. Per-call options clamp down and never up:

```python
rows = await port.run(sql, params, options={"row_cap": 500, "timeout": timedelta(seconds=2)})
```

Every code below is caller-caused, because on this plane the statement *is* the
caller's input — an undefined relation here is a malformed request, not a broken
database, and none of it egresses as `internal`.

| Condition | Kind | Code |
| --- | --- | --- |
| A write or DDL inside the read-only transaction | `precondition` | `dynamic_read_write_refused` |
| Syntax error, unknown relation/column/function | `validation` | `dynamic_read_statement_invalid` |
| Multi-command string rejected by the protocol | `validation` | `dynamic_read_multi_statement` |
| Refused by the route's confinement (role grants) | `precondition` | `dynamic_read_permission_denied` |
| The route's statement timeout fired | `timeout` | `dynamic_read_timeout` |
| Result exceeded the effective row cap | `precondition` | `dynamic_read_row_cap_exceeded` |
| Statement above `max_statement_bytes` | `validation` | `dynamic_read_statement_too_large` |
| Rows do not fit `select`'s return type | `validation` | `dynamic_read_row_type_mismatch` |
| No tenant bound on a tenant-aware route | `authentication` | `tenant_required` |

The caps bound one statement. To bound the *fleet* of them — a dashboard that
fans out fifty widgets, a catalog change that makes every one of them slow — bind
a [resilience policy](../running-in-prod/resilience.md) to the dep key, which
covers every route on the plane:

```python
from forze.application.contracts.dynamic_read import DynamicReadDepKey
from forze.application.contracts.resilience import PortPolicy

ResilienceDepsModule(
    spec=my_policies,                                        # defines "catalog_reads"
    port_policies=(PortPolicy(key=DynamicReadDepKey, policy="catalog_reads"),),
)
```

A bulkhead is the one worth setting deliberately: each call holds its own pooled
connection for the statement's lifetime, so an unbounded fan-out of slow widgets
is how this plane starves the pool everything else shares.

## Statements in traces

Simulation value capture masks the statement text by default:

```python
WIDGETS = DynamicReadSpec(
    name="widgets",
    capture_statements=True,   # opt in to verbatim statements on captured traces
)
```

A compiled statement embeds the literals it was compiled with — filter values,
identifiers, sometimes user input — so a captured trace shows `"<redacted>"` in
its place unless you opt in. Masked rather than dropped, so a trace consumer can
tell "a statement ran and its text was withheld" from "nothing was recorded".
This affects runtime tracing and simulation only; production traces are id-only
regardless.

## The mock

`MockDepsModule` answers dynamic-read routes from a handler you register, which
receives the fully-governed request — statement, bound params, effective caps and
timeout, resolved tenant — and returns the rows the statement would have
produced:

```python
from forze_mock import MockDepsModule, MockDynamicReadRegistry

registry = MockDynamicReadRegistry().on(
    "widgets",
    lambda request, state: [{"revenue": 10}][: request.row_probe],
)
module = MockDepsModule(dynamic_reads=registry)
```

An unprogrammed route fails closed (`code="mock.dynamic_read.unprogrammed"`).
Unusually for this package, the mock is **not** a capability superset here: it
cannot see the statement as SQL, so a write, a second command, and a cross-schema
read all come back as whatever the handler returns. That is deliberate. The only
way to reproduce those refusals in memory would be to pattern-match SQL, and a
mock that did would certify statements a real gadget walks straight past. A
scenario that needs a refusal path raises the taxonomy from its handler instead,
declaring the outcome rather than deriving it.

## What this plane refuses to know

- **No field encryption.** A dynamic statement's output shape is unknowable, so
  a sealed column comes back as ciphertext — and a statement could even
  `ORDER BY` one, where ciphertext order is a silently wrong answer. A
  wiring-time check is impossible (there is no statement to inspect), so the
  stance is explicit instead: point this plane at analytics-shaped relations that
  carry no sealed columns. See [encryption](../identity-tenancy-enc/encryption.md).
- **No writes.** Runtime DDL and bulk loads stay on the raw client under the
  documented escape-hatch policy. A governed dynamic-*write* surface is
  foreclosed, not deferred.
- **No HTTP route generator, and no MCP tool that takes raw statement text.**
  Statements come from app code — catalog rows, compiler output — never from a
  request body. Generating an endpoint that forwards one would be an injection
  endpoint with a framework logo on it.

If your statements are known at wiring time, you want [analytics](analytics.md)
for reads or [procedures](procedures.md) for commands — both give you typed
shapes and an enforceable tenancy predicate, and neither asks you to reason about
threat tiers.
