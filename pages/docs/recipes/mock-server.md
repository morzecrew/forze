---
title: Serve your app on the mock
icon: lucide/server-cog
summary: A stateful API for frontend and CI work — your real routes, in-memory backends, no infrastructure
---

A frontend team is blocked on a backend that isn't ready. The usual answer is a schema
mock: point a tool at the OpenAPI document and let it synthesize responses. That gets you
a stateless fake — `POST /products` returns something, and the list route never shows it.

A forze app has a better option. The in-memory mock is a faithful backend for every port:
documents with real filters, sort, cursors and `rev`, transactions, storage, search,
queues. So instead of faking the API, **serve the real one on the mock** — the same routes,
handlers, middleware and identity plane, with `MockDepsModule` where the backend modules
would be.

The property that makes this worth doing: **contract drift is structurally impossible**.
There is no second artifact to keep in sync, because the routes are generated from the
same frozen registry production serves.

## The swap

Everything below is an ordinary app. The only mock-specific line is the deps module:

```python
--8<-- "recipes/mock_server/app.py:wiring"
```

`MockDepsModule` registers every port as a *fallback* (see
[hybrid contexts](../testing/overview.md#hybrid-contexts-one-real-backend-mock-for-the-rest)),
so your real identity wiring composes on top of it in one registry — the local API-key
verifier and tenant resolver win their routes, the mock keeps the other planes. The same
mechanism lets you keep one real backend and mock the rest: add `PostgresDepsModule(...)`
to the list and documents go to Postgres while search, queues and storage stay in memory.

## The app is unchanged

The factory is the production one, parameterized by runtime:

```python
--8<-- "recipes/mock_server/app.py:app"
```

Routes come from the operation catalog, so `operationId` is the operation key
(`products.get`, `products.list`) and the served contract is the app's own. Identity is
your wiring too — here [local identity](local-identity.md) reading a JSON key file. That is
deliberate: the mock server has no way to mint a principal, so there is no dev-only auth
bypass to accidentally ship.

## Seed it

An API that answers `[]` is one no frontend can build against:

```python
--8<-- "recipes/mock_server/app.py:seed"
```

Seeds go through the **write path**, never into `MockState` directly, so `rev`, timestamps,
materialized fields and field encryption come from the same code that serves the reads.

!!! warning "Identity needs a principal document"

    The default eligibility gate reads a `policy_principal` document, so a dev key whose
    principal has no document fails authentication with `Principal not found`. Seed it as
    above — and note it must be the document write: the mock's
    `PrincipalRegistryPort.ensure_principal` is a no-op that returns a ref without storing
    anything the gate can read.

## Run it

```bash
cd examples/recipes/mock_server
just run                    # http://localhost:8000 — no compose file, no containers
just smoke                  # the seeded catalog through the generated list route
```

## What you get that a schema mock cannot give you

- `create` → `list` → `get` coherence, so a form, a table and a detail screen all work.
- Pagination cursors that terminate, because they are the real cursors.
- A stale `rev` that fails the way production fails — `revision_mismatch`, from the same
  gateway logic the real adapters run.
- Real error envelopes and status mapping, because `register_exception_handlers` is yours.

## When a schema mock is still the right answer

If the consumer has no Python and no access to your app — an external partner, a public
demo — export the document and hand it to the mature tooling:

```bash
cd examples/recipes/mock_server && just openapi > openapi.json
npx @stoplight/prism-cli mock openapi.json      # or MSW, orval, openapi-typescript
```

You lose statefulness and get schema-shaped responses; for a consumer who cannot run your
app, that is the correct trade.

## Limits

**Never serve this in production.** `MockDepsModule` keeps everything in memory and
enforces none of the durability, isolation or capability limits a real backend does. It is
a laptop and CI tool.

And the mock is *not the specification* — the conformance battery is. A behaviour your
frontend depends on should be one the [adapter conformance suite](../dst/overview.md) pins
across real backends, not one you discovered against the in-memory store.

## See also

- [Testing](../testing/overview.md) — the mock in unit tests, and hybrid contexts
- [Local identity](local-identity.md) — the API-key file this recipe authenticates with
- [CRUD over Postgres](crud-fastapi-postgres.md) — the same shape with a real backend
