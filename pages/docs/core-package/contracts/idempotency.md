# Idempotency contracts

Engine-level, interface-agnostic idempotency. A boundary supplies an **idempotency
key**; the execution engine replays a duplicate operation's stored **typed result**
instead of re-running it. The stored value is the operation's result — not an HTTP
response — and the boundary frames it for its transport.

## `IdempotencySpec`

Names an idempotency store route and its TTL.

| Field | Purpose |
|-------|---------|
| `name` | Logical store route. |
| `ttl` | Time-to-live for stored records (defaults to 30 seconds). |

Resolve a store with `ctx.idempotency(spec)`.

## `IdempotencyPort`

Stores and replays a completed operation's result, keyed by `(op, key, payload_hash)`.

| Method | Parameters | Returns |
|--------|------------|---------|
| `begin` | `op`, `key`, `payload_hash` | Stored `IdempotencyRecord` on replay, or `None` after a fresh claim. Raises on a payload-hash mismatch (`PRECONDITION` / `CONFLICT`) or an in-progress duplicate (`CONFLICT`). |
| `commit` | `op`, `key`, `payload_hash`, `record` | `None`. |

Implementations: Mock idempotency adapter, Redis / Valkey (`RedisIdempotencyAdapter`).

## `IdempotencyRecord`

A frozen value object holding `result: bytes` — the serialized operation result. The
engine wrap encodes and decodes it with the operation's declared result codec.

## Engine wrap — `IdempotencyWrap`

`forze.application.hooks.idempotency.IdempotencyWrap` is an operation-plan middleware.
Attach it per operation:

    :::python
    from forze.application.hooks.idempotency import IdempotencyWrap

    registry.bind("orders.create").bind_outer().wrap(
        IdempotencyWrap(
            op="orders.create",
            spec=idem_spec,
            result_type=OrderRead,
        ).to_step()
    )

On a duplicate key it returns the stored, typed result early — skipping the handler and
its transaction. It is a no-op when no idempotency key is bound. The engine computes
`payload_hash` from the operation arguments; the boundary supplies only the key.

## Boundary — the `Idempotency-Key` header

FastAPI's `InvocationMetadataMiddleware` reads the canonical `Idempotency-Key` header
(configurable via `idem_header`) into the invocation context, exposed as
`ctx.inv_ctx.get_idempotency_key()`. Other boundaries (message consumers, CLIs) bind it
with `ctx.inv_ctx.bind_idempotency(key)`.

## Notes

- This is **result-level** idempotency (re-serializes the typed result), not byte-identical
  HTTP response replay.
- The result is stored after the operation's transaction commits; a crash in that window
  re-executes on retry (standard at-least-once).

Related: [Add Idempotency](../../recipes/add-idempotency.md), [Outbox](outbox.md),
[FastAPI integration](../../integrations/fastapi.md).
