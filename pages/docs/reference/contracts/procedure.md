---
title: Procedures
icon: lucide/cog
summary: The procedures contract — one governed, parametrized DB command or compute step
---

A procedure is a **governed, parametrized command or compute** — a set-based recompute, a
`CALL`, a `REFRESH MATERIALIZED VIEW`, a compute function run for effect. It is analytics'
write/compute twin: where [analytics](analytics.md) reads registered queries, a procedure
runs one author-supplied statement with validated params. The port is **command-only** —
it cannot be acquired in a read-only operation. The concept and the Postgres mapping are in
[Procedures](../../data-events/procedures.md).

```python
result = await ctx.procedure.command(spec).run(params)
```

## Spec

`ProcedureSpec[In, Out]` — **one spec is one procedure** (not a map of queries):

| Field | Type | Default | Meaning |
|-------|------|---------|---------|
| `name` | `str \| StrEnum` | required | logical name / route; the wiring binds it to a statement |
| `params` | `type[In]` | required | Pydantic model of the bound parameters passed to `run` |
| `result` | `type[Out] \| None` | `None` | output shape — see the cardinality table below |
| `encryption` | `FieldEncryption \| None` | `None` | field [encryption](../../identity-tenancy-enc/encryption.md) on **params** (`binds_record_id` unsupported — params have no id) |
| `params_codec` | `ModelCodec \| None` | `None` | param codec override (auto-derived; rebuilt to apply `encryption`) |
| `description` | `str \| None` | `None` | human-readable description |

## Surface  (`ctx.procedure.command(spec)`)

| Method | Signature | Notes |
|--------|-----------|-------|
| `run` | `run(params)` | execute the procedure with the bound params; returns an `ExecResult[Out]` |

The `result` declared on the spec sets the `ExecResult` shape:

| `result` | `ExecResult` carries | For |
|----------|----------------------|-----|
| `None` | `affected_count` | a side effect — recompute, `REFRESH`, `CALL` |
| a scalar type | `value` | a function returning one value |
| a Pydantic model | `value` | a function or `SELECT` returning one row |

## Tenancy

A procedure runs author-supplied opaque SQL, so the framework enforces isolation only where
the boundary lives outside that SQL — the `namespace` (per-tenant schema) and `dedicated`
(routed client) tiers are enforced. A `tenant_aware` procedure at the `tagged` tier is
**allowed but validated**: the framework binds the current tenant as `%(tenant)s` and fails
closed at wiring if the SQL never references it. See
[Procedures → Tenancy](../../data-events/procedures.md#tenancy).

## Implemented by

| Backend | Notes | Integration |
|---------|-------|-------------|
| Postgres | a function / `CALL` / set-based statement / `REFRESH` | [Postgres](../../integrations/postgres.md) |

A programmable mock implements it for tests.
