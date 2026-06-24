---
title: Procedures
icon: lucide/cog
summary: Governed parametrized commands and compute — run one set-based statement, not per-row triggers
---

Some database work is neither a document write nor an analytical read: recomputing
a rollup over a freshly-ingested batch, calling a stored function, refreshing a
materialized view. You can always reach for the raw client — but then you own
correctness, with no parameter validation, no tenancy, no portability. The
**procedures** port is the governed middle ground: a spec-named, parametrized
command or compute step. It is the write-and-compute twin of the governed,
parametrized *reads* you run through [analytics](analytics.md), sharing the same
machinery and differing only in intent.

## Where it sits

A handler touches the database three ways, and procedures fills the gap between
the other two:

| Approach | What it does | Governed? |
| --- | --- | --- |
| Raw client | any SQL, inline | no — you own validation, tenancy, portability |
| **Procedures** | a named command / compute step | **yes** — validated params, tenancy, backend-portable |
| Analytics | named reads, pages of rows | yes |

Procedures promotes raw SQL to a *named, validated, tenant-aware, portable*
operation. Unlike analytics it mutates or computes rather than returning pages —
so it is **command-only**, and acquiring it inside a read-only operation fails
closed. The motivating case is a batch pipeline: ingest a large batch, then run
**one** set-based statement over it, instead of per-row triggers that overload
the database.

## The shape in code

One spec is one procedure: a typed params model in, and an optional typed result.
It names nothing about SQL or the backend — that lives in the wiring.

```python
--8<-- "recipes/procedures_recompute/app.py:spec"
```

A handler resolves the port off the context and runs it with typed params. The
recompute below replaces a swarm of per-row triggers with a single governed call:

```python
--8<-- "recipes/procedures_recompute/app.py:flow"
```

## What comes back

The `result` you declare on the spec drives the shape of the `ExecResult`. The
cardinality is deliberately narrow — a scalar, a single row, or an affected-row
count. Pages of rows are analytics' job, not this port's.

| `result` on the spec | `ExecResult` carries | Use for |
| --- | --- | --- |
| `None` | `affected_count` | a side effect — recompute, `REFRESH`, `CALL` |
| a scalar type (`int`, …) | `value` | a function returning one value |
| a Pydantic model | `value` | a function or `SELECT` returning one row |

## Mapping it to Postgres

The handler-facing spec stays backend-agnostic; the statement lives in the
integration config. On [Postgres](../integrations/postgres.md), one
`PostgresProcedureConfig` maps a route to a statement with `%(name)s`
placeholders:

```python
PostgresProcedureConfig(
    # A set-based statement; with result=None the rowcount is the affected count, so this is DML
    # (not `SELECT a_function(...)`, which returns one row — a count-returning function uses a
    # scalar result instead).
    sql=(
        "INSERT INTO region_totals (region, total) "
        "SELECT region, sum(amount) FROM sales WHERE since >= %(since)s GROUP BY region "
        "ON CONFLICT (region) DO UPDATE SET total = excluded.total"
    ),
    in_transaction=True,            # False for REFRESH MATERIALIZED VIEW CONCURRENTLY
    statement_timeout=None,         # SET LOCAL statement_timeout for long compute
    query_schema=None,              # per-tenant schema (namespace tier)
)
```

Set `in_transaction=False` for statements that cannot run inside a transaction
(`REFRESH MATERIALIZED VIEW CONCURRENTLY` and some maintenance); they take the
autocommit path. `statement_timeout` and `query_schema` both need a transaction,
so they are rejected at wiring when combined with `in_transaction=False`.

## Tenancy

Analytics and document writes can *enforce* tenant isolation because the framework
composes the predicate. A procedure runs author-supplied opaque SQL, so the
framework can only enforce isolation where the tenant boundary lives outside that
SQL:

| Tier | Mechanism | Posture |
| --- | --- | --- |
| `namespace` | per-tenant schema via `query_schema` | enforced — the SQL runs in the tenant's own schema |
| `dedicated` | a routed client per tenant | enforced — strongest isolation |
| `tagged` | `%(tenant)s` bound into the SQL | offered, then validated (below) |

At the tagged tier the framework binds the current tenant id as the `%(tenant)s`
parameter, but it cannot prove the SQL *uses* it to scope. So a `tenant_aware`
route is allowed, but checked.

!!! warning "A tenant-aware route fails closed at wiring"

    Wiring a `tenant_aware` procedure whose SQL never references `%(tenant)s`
    raises at startup, not at runtime — a route that declares tenant-scoping but
    forgets to bind the tenant is a configuration error, never a silent leak.
    This proves the parameter is *bound*; for genuinely enforced isolation, prefer
    the `namespace` or `dedicated` tier. See [multi-tenancy](../identity-tenancy-enc/multi-tenancy.md).

A procedure that is inherently cross-tenant — refreshing a shared materialized
view, a cross-tenant rollup — is simply not `tenant_aware`, the same as any other
non-tenant port.

## The batch-recompute case

The flow that motivates the port: a batch lands, then one set-based statement
recomputes over it. The compute is expressed once, in the registered statement —
here modelled in-process so the recipe runs without a warehouse:

```python
--8<-- "recipes/procedures_recompute/app.py:recompute-model"
```

In production that handler is a single Postgres statement; the calling code does
not change. Field encryption applies to a procedure's **params** when a keyring is
wired — the same field-level [encryption](../identity-tenancy-enc/encryption.md)
the document and analytics paths use.
