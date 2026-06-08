# Inbox contracts

Consumer-side deduplication — the symmetric half of the [outbox](outbox.md). At-least-once
transports (queue / stream / pubsub) can redeliver, so a consumer marks a message processed
and runs its handler **in the same transaction**: the dedup mark and the handler's writes
commit atomically (exactly-once *effect*), or roll back together. Distinct from
[idempotency](idempotency.md) (operation-level *result replay*) — the inbox is message-level
*seen / not-seen*, with no stored result.

## `InboxSpec`

Names a dedup store route.

| Field | Purpose |
|-------|---------|
| `name` | Logical store route. |
| `ttl` | Dedup window (advisory; entry cleanup is the store's responsibility). |

Resolve a store with `ctx.inbox(spec)`.

## `InboxPort`

A single atomic primitive (no check-then-mark race):

| Method | Parameters | Returns |
|--------|------------|---------|
| `mark_if_unseen` | `inbox`, `message_id` | `True` if newly recorded (process the message), `False` if already seen (skip). |

For exactly-once effect, call it inside the same transaction as the handler. Implementations:
Mock; Postgres (`PostgresInboxStore`).

## Consumer helper — `process_with_inbox`

`forze_kits.integrations.inbox.process_with_inbox` is the transactional consumer dedup helper:

    :::python
    from forze_kits.integrations.inbox import process_with_inbox

    processed = await process_with_inbox(
        ctx, message,
        inbox_spec=inbox_spec,
        handler=my_handler,        # async (message) -> None
        tx_route="database",
    )

It opens a transaction on `tx_route`, marks the message via `ctx.inbox(spec).mark_if_unseen`, and
runs `handler` in the **same** transaction. Returns `True` if processed, `False` if a
redelivery was skipped. The dedup id defaults to `message.key or message.id` (outbox relay
sets `key` to the integration `event_id`, so the same logical event dedups across transports;
pubsub has no `id`); pass a `message_id` extractor to override.

## Postgres adapter

`PostgresInboxStore` runs `INSERT ... ON CONFLICT DO NOTHING` on the tx-bound connection, so
the mark participates in the handler's transaction. The table is **app-provided** (not
auto-created); expected schema:

    :::sql
    CREATE TABLE inbox_processed (
        inbox_route  text NOT NULL,
        message_id   text NOT NULL,
        processed_at timestamptz NOT NULL,
        PRIMARY KEY (inbox_route, message_id)
    );

Wire it via `PostgresDepsModule(inboxes={"events": PostgresInboxConfig(relation="schema.inbox_processed")})`.

## Notes

- **Atomicity requires co-location** — the inbox table must be in the same database/transaction
  as the handler's writes, or the guarantee degrades to best-effort.
- TTL is not enforced inline; prune old rows with a scheduled job / partial index.
- Concurrent duplicates serialize on the primary key — one processes, the other skips.

Related: [Outbox](outbox.md), [Idempotency](idempotency.md).
