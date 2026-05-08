# Add idempotency

Use this recipe when clients may retry mutating requests and you need duplicate submissions to resolve safely.

## Ingredients

- [FastAPI Integration](../integrations/fastapi.md) route features
- [Redis / Valkey Integration](../integrations/redis.md) idempotency storage
- A stable client-provided idempotency key

## Steps

1. Configure Redis idempotency dependencies.
2. Enable idempotency on the FastAPI endpoint or route helper.
3. Require callers to send an idempotency key for unsafe operations.
4. Return the stored result for duplicate requests with the same key and compatible request body.

## Where to configure it

Keep idempotency at the interface/adapter boundary. Usecases should model business intent and should not need to know whether the HTTP request was retried.

## Learn more

See [FastAPI Integration](../integrations/fastapi.md#idempotency) and [Redis / Valkey Integration](../integrations/redis.md#idempotency).
