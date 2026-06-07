# Add idempotency

Use this recipe when clients may retry mutating requests and you need duplicate
submissions to resolve safely — by replaying the original result rather than re-running
the operation.

## Ingredients

- An idempotency store: [Redis / Valkey Integration](../integrations/redis.md) or the mock adapter (`IdempotencySpec`, `IdempotencyDepKey`)
- The engine wrap `IdempotencyWrap` ([Idempotency contracts](../core-package/contracts/idempotency.md))
- A boundary that supplies a stable client-provided idempotency key

## Steps

1. Register an idempotency store (`IdempotencySpec` + a `Configurable*Idempotency` factory).
2. Attach `IdempotencyWrap` to each unsafe operation, declaring its result type:

        :::python
        registry.bind("orders.create").bind_outer().wrap(
            IdempotencyWrap(op="orders.create", spec=idem_spec, result_type=OrderRead).to_step()
        )

3. Have the boundary supply the key. FastAPI reads the canonical `Idempotency-Key` header
   into the invocation context automatically; other boundaries call
   `ctx.inv_ctx.bind_idempotency(key)`.
4. Duplicate requests with the same key replay the stored typed result; a different
   payload under the same key is rejected as a conflict.

## Where it runs

Idempotency is an **engine-level** concern: the boundary supplies only the key, and the
operation-plan wrap does the dedup and early-return — handlers model business intent and
need not know the request was retried. The engine computes the payload hash from the
operation arguments.

## Notes

- This is result-level idempotency (re-serializes the typed result), not byte-identical
  HTTP response replay.
- The result is stored after the operation's transaction commits.

## Learn more

See [Idempotency contracts](../core-package/contracts/idempotency.md) and
[FastAPI Integration](../integrations/fastapi.md).
