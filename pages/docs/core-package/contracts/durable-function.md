# Durable function contracts

Durable function contracts cover **event-driven durable execution**: emit events from
handlers, run registered functions with memoized steps on a platform worker (future:
`forze_inngest`). They live under the [Durable](durable.md) family alongside
[durable workflow](durable-workflow.md) (Temporal).

There is no `start()` command port—runs are triggered by **events** or **cron**
declared on `DurableFunctionSpec`.

## `DurableFunctionEventSpec[M]`

| Section | Details |
|---------|---------|
| Purpose | Names a logical event and its payload model (e.g. `app/invoice.paid`). |
| Import path | `from forze.application.contracts.durable.function import DurableFunctionEventSpec` |
| Required fields | `name`, `codec`. |
| Common implementations | Future: Inngest client adapter. |
| Related dependency keys | `DurableFunctionEventCommandDepKey`. |

## `DurableFunctionEventCommandPort[M]`

| Method | Purpose |
|--------|---------|
| `send(payload, *, event_id?, occurred_at?)` | Emit an event; returns event id. `event_id` enables idempotent triggers. |

Resolve from HTTP handlers:

    :::python
    from forze.application.contracts.durable.function import (
        DurableFunctionEventCommandDepKey,
        DurableFunctionEventSpec,
    )

    events = ctx.deps.resolve_configurable(
        ctx,
        DurableFunctionEventCommandDepKey,
        invoice_paid_spec,
        route=invoice_paid_spec.name,
    )
    await events.send(InvoicePaidPayload(invoice_id="inv-1"))

## `DurableFunctionSpec[In, Out]`

| Field | Purpose |
|-------|---------|
| `name` | Function id / route key for registration. |
| `run` | `DurableFunctionInvokeSpec` (`args_type`, optional `return_type`). |
| `triggers` | One or more `DurableFunctionEventTrigger` and/or `DurableFunctionCronTrigger`. |

Triggers:

| Type | Field | Purpose |
|------|-------|---------|
| `DurableFunctionEventTrigger` | `event` | Event name that starts the function |
| `DurableFunctionCronTrigger` | `expression` | Cron expression (provider-specific) |

## `DurableFunctionStepPort`

Used **inside** a function run (worker scope), not from ordinary HTTP handlers.

| Method | Purpose |
|--------|---------|
| `run(step_id, fn)` | Execute `fn` as a durable, retriable step |

Resolve via `DurableFunctionStepDepKey` (simple dep, not spec-routed).

## Dependency keys

| Key | Purpose |
|-----|---------|
| `DurableFunctionEventCommandDepKey` | Routed factory → `DurableFunctionEventCommandPort` |
| `DurableFunctionStepDepKey` | Simple factory → `DurableFunctionStepPort` |

## Related pages

- [Durable](durable.md)
- [Durable workflow](durable-workflow.md)
- [Queue](queue.md) — fire-and-forget messages without step memo
- [Scheduled queue jobs](../../recipes/scheduled-queue-jobs.md)
