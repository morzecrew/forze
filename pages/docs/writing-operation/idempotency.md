---
title: Idempotency
icon: lucide/copy-check
summary: Make a retried operation a no-op that returns the first result
---

Client retries and at-least-once delivery mean the same operation can arrive
twice. **Idempotency** makes the duplicate a no-op that returns the *first*
attempt's result — the handler and its writes run exactly once.

## How it works

When a request carries an **idempotency key**, the engine wraps the operation as
the outermost wrap of its pipeline. On each call it:

1. fingerprints the arguments (a stable payload hash);
2. **claims** `(operation, key, payload hash)`;
3. if that claim already completed, **returns the stored result** — decoded back
   into the operation's typed result — and skips the handler *and its
   transaction* entirely;
4. otherwise runs the handler and stores the encoded result.

The `before` hooks run ahead of the wrap chain, so authentication and
authorization still run first — a replayed result is never an unauthorized one.
What a duplicate skips is everything *inside* the wrap: the inner wraps, the
transaction, and the handler.

## Keys and payloads

- The **key** comes from the caller. Over HTTP it's the `Idempotency-Key`
  header, bound to the context by the [FastAPI](../integrations/fastapi.md)
  middleware. No key → no dedup; the operation just runs.
- The **same key with a different payload** is a conflict — a key can't be reused
  for a different request.

## Wiring

Declare an `IdempotencySpec` (it carries a TTL for how long a result is
remembered) and register an idempotency adapter — commonly [Redis](../integrations/redis.md) —
under that name. The operation's result type must be a Pydantic model,
since the stored result is encoded and decoded. Wired end to end in the
[Add idempotency](../recipes/add-idempotency.md) recipe.

## Idempotency vs the inbox

Both dedupe, at different layers: idempotency dedupes **inbound operations** by
caller-supplied key; the [inbox](../data-events/events-sagas.md) dedupes **consumed events** by
event id. Same principle — exactly-once effects over an at-least-once world.
