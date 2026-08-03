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

Three hand-written rows is the right size for a recipe and the wrong size for a real screen.
`forze_mock.seeding` fills specs from a plan instead — generated rows for volume, fixtures
for the rows a demo actually shows, and references that point at documents the seed created:

```python
from forze_mock.seeding import SeedPlan, apply_seed, load_fixtures, spec_seed

plan = SeedPlan(
    specs=(
        spec_seed(project_spec, count=5),
        spec_seed(task_spec, count=40, fixtures=load_fixtures("tasks.json")),
    ),
    rng_seed=7,
)
result = await apply_seed(ctx, plan)   # result["tasks"] -> the created ids
```

A seeded `Task.project_id` names a seeded project — inferred from the field and spec names,
corrected by `SeedPlan.links` where the names don't line up. The plan is reproducible: one
`rng_seed` fixes the values, and `SeedPlan.instant` pins the clock the write path mints ids
and timestamps from, so two processes running the plan produce byte-identical documents.
Seeding needs `polyfactory` (it ships with the `dst` extra).

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

## Serve it with the CLI, and drive it

Composing the runtime by hand is fine for one app. `MockApp` is the declaration form — it
says which real modules to keep, what to seed, and nothing about the app itself:

```python
--8<-- "recipes/mock_server/served.py:declaration"
```

```bash
FORZE_MOCK_SERVER=1 forze mock serve examples.recipes.mock_server.served:mock_app
```

The gate is not decoration: `serve` refuses without it, and refuses a composition whose deps
contain no fallback-marked mock module — so a real runtime cannot be served here, and can
never grow the routes below. Install with the `mock-server` extra.

### The control plane

A frontend developer needs to *provoke* states, not wait for them. `/_mock` does that, and
it sits beside your app rather than inside it, so your own auth does not lock you out of it:

| Route | What it does |
|---|---|
| `POST /_mock/reset` | Back to the pristine seed — state cleared, faults disarmed |
| `POST /_mock/seed` | Re-apply the plan (`{"reset": true}` to wipe first) |
| `GET /_mock/state/{store}` | Peek at a mock store (`documents`, `queues`, `storage`, …) |
| `POST /_mock/fault` | Arm a failure for matching port calls |
| `POST /_mock/latency` | Delay matching calls — spinners and timeouts |
| `POST /_mock/disarm` | Clear every armed fault and delay |
| `POST /_mock/time` | `freeze` / `advance` / `resume` the server clock |
| `POST /_mock/emit` | Fire one realtime signal at one audience (needs `MockApp(on_emit=...)`) |
| `GET /_mock/health` | Readiness, the clock, and the loud "this is a mock" |

```bash
# every products call fails as a real 409, once
curl -X POST localhost:8000/_mock/fault \
     -d '{"route": "products", "op": "create", "kind": "conflict", "times": 1}'

# the expiry screen, without waiting a day
curl -X POST localhost:8000/_mock/time -d '{"action": "advance", "seconds": 86400}'
```

The fault's `kind` is a **real** `exc` kind, so your own exception handlers turn it into the
real status and error envelope — an armed `conflict` reaches the client exactly as a genuine
optimistic-concurrency failure would. A control plane that invented its own error shape
would be teaching the frontend a lie.

`route` is the spec name and `op` is the **port method** (`create`, `update`, `find_page`,
`get`, …) — omit `op` to match every call on that spec, which is usually what you want.

`emit` is where a notification badge gets built: one signal, at one principal, on demand —
which is what a traffic generator cannot give you. The realtime egress plane lives above
`forze_mock`, so the server hands the signal to your `on_emit` and your own mailbox decides
who receives it; see `examples/recipes/realtime_sse/served.py`. Note that `/_mock/reset`
clears the mock's stores and **not** state your app holds itself, like that mailbox.

!!! warning "The control plane is unauthenticated"

    Anyone who can reach the port can reset your data and arm faults. That is correct for a
    laptop and for CI, and unacceptable anywhere else — which is the same reason `serve`
    demands `FORZE_MOCK_SERVER=1`. Bind it to localhost; never expose it.

### In a container

A frontend team should not need a Python toolchain to run your backend:

```bash
cd examples/recipes/mock_server
just up            # docker compose up --build, health-checked
just down
```

The recipe's `Dockerfile` and `compose.yaml` build from the repo root and serve
`$MOCK_APP` — point that at your own declaration to serve your app. The image sets
`FORZE_MOCK_SERVER=1` deliberately: the container *is* the mock, so running it is the
opt-in. The port is published on `127.0.0.1` for the reason above.

## Multi-tenant data

If your specs are tenant-aware, the served mock partitions them the way a real relation's
tenant `WHERE` clause does. Two API keys on two tenants, one server, disjoint data — the
tenant comes from your identity wiring, so nothing about the app changes:

```python
mock = MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})
```

An unauthenticated request binds no tenant and is refused rather than shown everything.

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
