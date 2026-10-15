# T-pb-tenancy — lifecycle steps with no tenant bound, and floor-exempt routes

Fixes backlog items 2/3 and 7 from the pre-existing-bugs list, on `fix/pb-tenancy` (based on
`origin/main` at 5e654880e). No RFC: the backlog and the user's decisions of 2026-10-15 are the
spec. Items 2/3 are "guard + key steps"; item 7 is "floor-exempt kinds".

## Plan

Items 2/3 — a lifecycle step or background loop runs where no tenant is bound, so a port that
is `tenant_aware` or reaches its backend through a per-tenant routed client refuses every call
with `tenant_required`, an authentication error no retry clears and no supervisor treats as
terminal.

- `src/forze/application/contracts/tenancy/wiring.py` — `require_tenant_global(port, what=)`,
  generalized from `OutboxWake.require_tenant_global`; a `configuration` error coded
  `tenant_global_required`. Exported from `contracts.tenancy`.
- `src/forze/application/contracts/outbox/wake.py` — the wake check delegates to it.
- `src/forze_kits/lifecycle/singleton.py`, `leadership.py` — guard the lock ports at startup.
- `src/forze_kits/integrations/realtime/lifecycle.py` — group ensure always; stream trim and
  mailbox retention when `tenants=None`.
- `src/forze_kits/integrations/outbox/lifecycle.py` — the relay (and the realtime relay over
  it) when `tenants=None`; separately, stop swallowing its own configuration error.
- `src/forze_kits/integrations/durable/lifecycle.py` — recovery and the scheduler (cron ensure
  and tick) when `tenants=None`.
- `src/forze_kits/integrations/progress/lifecycle.py` — `job_staleness` when `tenants=None`.
- `src/forze_temporal/execution/lifecycle/pool.py` — the schedule bootstrap.
- `src/forze_s3/execution/lifecycle/pool.py` — `s3_bucket_lifecycle_step`.
- `src/forze_mongo/execution/document_indexes.py` — index validation.
- `src/forze_identity/authz/execution/providers.py` — `permission_providers_lifecycle_step`
  without `tenant=`.

Item 7 — tenant-global-by-design routes (fleet, outbox wake pub/sub, lifecycle locks) are left
out of the declared isolation floor, and a module whose only routes are those is accepted.

- `src/forze/application/contracts/tenancy/wiring.py` — `TenancyRouteSpec.tenant_global`,
  `TenancyRouteGroup.tenant_global`; the floor skips those routes; docstring updated.
- `src/forze_redis/execution/deps/configs.py`, `module.py` — `tenant_global` on lock and
  pub/sub routes; fleets passed as an always-tenant-global group.
- `pages/docs/reference/tenancy-matrix.md`, `pages/docs/running-in-prod/shutdown-and-fleets.md`
  — the exempt kinds, and the lock route a guarded step needs.

Commits, in order: guard + singleton + leadership; the other `forze_kits` steps; the relay's
swallowed configuration error; the integration steps (Temporal, S3, Mongo); the identity step;
item 7; the Kafka check if it holds and needs no design call.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T12:00:00Z
attempt: 1
claim: the guard must see a tenant-aware document port, but Postgres, Mongo and Firestore document adapters carry `tenant_aware` and `client` on their gateways, not on the adapter
evidence: src/forze/application/integrations/document/adapter.py:65-66 — `read_gw` / `write_gw`; src/forze_postgres/kernel/gateways/base.py:131 — `client` on the gateway
action: decided
proposal: ASSUMED — the guard reads `tenant_aware` and `client` on the port and on its `read_gw` / `write_gw` when present. A dynamic per-tenant namespace or relation is not refused: its resolver is called with `None`, and what that means (a default partition, or a refusal) is the app's.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T12:01:00Z
attempt: 1
claim: leadership resolves its lock ports in startup and campaigns in a supervised loop, so the guard can run in either place
evidence: src/forze_kits/lifecycle/leadership.py:1211-1225 — `_LeadershipStartup.__call__` resolves `cmd`/`query` before `run_supervised`
action: decided
proposal: ASSUMED — the guard runs in startup, beside the port resolution, so startup fails with the configuration error. A campaign that meets a configuration error later still stops through its existing terminal path.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T12:02:00Z
attempt: 1
claim: some apps started cleanly and failed every tick (or never led), and now refuse to start; others already failed at startup with `tenant_required` and now fail with a clearer configuration error
evidence: tests/unit/test_forze_kits/test_lifecycle_tenant_global.py:1-8 — run red before each fix: leadership started and never led; the `tenants=None` loops started and logged a failure each tick; singleton, group ensure, cron ensure, S3, Mongo and the identity step already failed at startup with `tenant_required`
action: decided
proposal: ASSUMED — the leadership commit and the `forze_kits` steps commit are breaking (`💥`, `!`): a process that started before now refuses to. The integration and identity commits are not: those steps failed at startup before, only the error kind changes.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T13:10:00Z
attempt: 1
claim: the mock stream-group admin adapter hides its tenancy inside the stream adapter it wraps, so the guard passes a tenant-aware mock route that the Redis admin adapter would refuse
evidence: src/forze_mock/adapters/stream.py:441-452 — `stream: MockStreamAdapter[M]`, no `tenant_aware`; src/forze_redis/adapters/stream.py:372 — `RedisStreamGroupAdminAdapter(AckStreamGroupAdminPort, TenancyMixin)`
action: decided
proposal: ASSUMED — the mock admin adapter exposes a read-only `tenant_aware` forwarded from its stream, as the Redis one carries it; no shape-specific lookup in the guard. The Redis leg is pinned by an integration test.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T13:11:00Z
attempt: 1
claim: eight retention-pairing tests wire tenant-aware mailbox and cursor routes and run the sweep with no tenants, the shape now refused; their sweeps had been failing every tick unseen
evidence: tests/unit/test_forze_kits/integrations/test_realtime_mailbox.py:40-46 — `_runtime()` marks both routes tenant-aware; the `TestRetentionIsPaired` steps omit `tenants`
action: decided
proposal: ASSUMED — those steps pass `tenants=lambda: [_T1]`, the configuration their mailbox needs; what they test (the marker) is unchanged.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T13:30:00Z
attempt: 1
claim: the relay swallows its own configuration error at three levels, not one — the batch, the tenant of a shard, and the tick — so fixing the batch alone leaves the loop retrying it every tick; the durable recovery and scheduler loops swallow it the same way, at the tenant and the tick
evidence: src/forze_kits/integrations/outbox/lifecycle.py:402 — at `origin/main`, the batch's `except Exception: logger.exception(...)`, and :489 (tenant) and :672 (tick) the same; `_relay_core.py:240-241` turns an unbound read into the configuration error `outbox_relay_tenant_unbound`; src/forze_kits/integrations/durable/lifecycle.py:143 and :362 (tenant), :159 and :397 (tick); found in the self-audit
action: decided
proposal: ASSUMED — a configuration error passes the batch and tenant levels and stops the loop at the tick, logged, as the stream-trim, mailbox-retention and job-staleness loops already do; the durable loops get the same, in the same commit. One tenant's configuration error stops a sharded loop: wiring is shared by every tenant. A run's own body failure is not affected: recovery records and swallows it before the loop sees it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T14:00:00Z
attempt: 1
claim: lifecycle locks and outbox wakes have no kind of their own — they are ordinary dlock and pub/sub routes — so only fleets can be exempted by kind, and nothing marks a lock or a wake as the deployment's
evidence: src/forze_redis/execution/deps/module.py:313-342 — `dlock` and `pubsub` route groups; fleets are left out of `validate_module_tenancy` entirely, so a floored fleets-only module derives `none` (scratchpad verify/item7_floor_no_route.py)
action: decided
proposal: ASSUMED — `TenancyRouteSpec.tenant_global` and `TenancyRouteGroup.tenant_global` mark a route the deployment's: the declared floor skips it, a module whose only routes are tenant-global meets any floor within its ceiling, and a module with no routes still derives `none`. A tenant-global route is refused (the module's validation code) when it is tenant-aware, resolves its namespace per tenant, or sits on a routed client. Redis passes fleets as an always-tenant-global group and adds `tenant_global: bool = False` to `RedisDistributedLockConfig` and `RedisPubSubConfig`. The mock has no floor, so it gets no flag. The refusal lives in the shared validation, not in each config, so every integration that maps the flag gets it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T14:40:00Z
attempt: 1
claim: the guard refuses a tenant-scoped port whether or not a tenant happens to be bound when the step starts, so an app that bound one around a step's startup would be refused too
evidence: src/forze/application/contracts/tenancy/wiring.py:822 — `require_tenant_global` reads the port, not the binding; no documented pattern binds a tenant around lifecycle startup (grep of src/ and pages/ for `bind_identity` near a startup or step)
action: decided
proposal: ASSUMED — the guard stays unconditional: the lifecycle runs startup with no tenant, and the steps that serve tenants take `tenants=` and bind them per pass. A per-tenant step bound by hand is not a shipped pattern.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T14:41:00Z
attempt: 1
claim: off-list, the Kafka adapters fail open on a tenant-aware route with no tenant bound — the consumer, and also the producer and the admin adapter — calling the namespace resolver with `None` where every other tenant-aware adapter refuses with `tenant_required`
evidence: src/forze_kafka/adapters/consumer.py:84-90 — `_tenant_id` returns `None` when no tenant is bound, as do producer.py:40-46 and admin.py:56-62
action: decided
proposal: ASSUMED — stop and report, not fixed here. Failing closed is one line per adapter, but the shipped commit-stream consumer runs unbound and takes no `tenants=`, so a tenant-aware group it consumes would go from reading unbound to failing every poll with an error its supervisor retries forever. Whether that consumer refuses such a group at startup, as the steps above now do, or gains a per-tenant mode, is a design call. A related Kafka tenancy finding was reported separately.
```

## Kafka — 2026-10-15

The user decided both Kafka questions after the report above.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T15:20:00Z
attempt: 1
claim: the user chose to fail closed — a tenant-aware Kafka route with no tenant bound raises `tenant_required` in the consumer, producer and admin adapters — and to have the commit-stream consumer refuse a tenant-aware group at startup
evidence: src/forze_kafka/adapters/consumer.py:84-90 — at the previous tip, `_tenant_id` returned `None` with no tenant bound; tests/unit/test_forze_kafka/test_kafka_tenancy.py:1 runs all three adapters and the consumer step
action: decided
proposal: ASSUMED — the three adapters take `TenancyMixin` and resolve through `require_tenant_if_aware`, the semantics every other tenant-aware adapter has, which drops their three copies of `_tenant_id`. The commit-stream consumer step guards its group port with `require_tenant_global`, so a group on a routed client is refused too; the mock commit-stream adapter exposes `tenant_aware` as the Kafka one does. Breaking: a setup that produced or consumed on such a route unbound now fails.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T15:40:00Z
attempt: 1
claim: the user chose that a tenant-aware Kafka route with a fixed namespace counts as no isolation
evidence: src/forze_kafka/execution/deps/module.py:72-93 — at the previous tip, each group derived its tier from `tenant_aware`
action: decided
proposal: ASSUMED — a Kafka route is `namespace` tier when its namespace is per-tenant; a fixed one derives `none`, as the Kafka docs' "no tagged tier" says, and the adapters resolve the namespace through `_tenant_id_for_resolve`, as the other backends do. The commit subject is `🔒️ fix(kafka)` as instructed; the commit skill would mark a breaking change `💥 fix(kafka)!`, so the break is named in the changelog entry.
```

## Self-audit — 2026-10-15

Scope: the whole branch against `origin/main` (5e654880e), 6 commits, 36 files, +1,695 −87
lines (mostly tests; source about +0.35k; docs and this log the rest). Passes run: spec fidelity,
wrapper × state, empty cases, drift against the surrounding loops, failure paths, prose, own
fixes, verification, and the user's decisions.

Findings, all fixed on the branch:

- **Durable recovery and the scheduler swallowed their own configuration error** at the
  tenant and tick levels, the same class as the relay. Folded into the relay commit, with a
  red-first test over both loops, unbound and sharded.
- **The tenancy matrix over-claimed**: it said an unbound step refuses any *isolated*
  resource, but a per-tenant namespace resolver is not refused (its resolver is called with
  `None`). Reworded to `tenant_aware` or a routed client.
- **`PostgresTenancyRouteSpec` could not express `tenant_global`**, so the converter ratchet
  (`test_the_converter_forwards_every_contract_field`) failed in the full unit run. Added and
  forwarded, and the ratchet's probe sets it; sabotaging the forward fails the ratchet.
- **The Redis config docstrings** for `tenant_global` left out the routed-client refusal.
- **Reading isn't proof** for documents: the kits tests run on mock documents, which carry
  `tenant_aware` on the adapter; Postgres keeps it on the gateways. Added a test that resolves
  a real `PostgresDocumentAdapter` and runs the guard both ways.

Verification:

- Red first, each run before its fix: singleton and leadership (unit, and on real Valkey);
  group ensure, stream trim, mailbox retention (mailbox and cursors), both relays, durable
  recovery, the scheduler with and without cron specs, `job_staleness`; the relay and durable
  configuration-error loops; Temporal, S3, Mongo, the identity step; the item-7 floor tests.
  The Redis item-7 unit file and the floored end-to-end test were red only by construction
  (`tenant_global` did not exist).
- Sabotage, 13 mutants, all killed: the guard's gateway lookup and routed-client branch;
  leadership's guard; the relay's tick and tenant re-raise; the durable tenant re-raise; the
  mailbox cursor guard; the identity guard; the floor's exempt filter and exempt-only branch;
  the routed-client refusal of a tenant-global route; Redis fleets as tenant-global; the
  Postgres converter forward.
- Patch coverage of the source lines: 100/100 added lines covered (unit profile).
- Unit: `just test tests/unit` — 18,023 passed, 1 failed (the Postgres ratchet above); after
  the fix, 18,024 passed, 16 skipped.
- Integration (Docker): Redis 234, S3 128 (2 skipped), Mongo 475 (serial; under `-n 4` the
  replica set's fixed port 27017 collided), Temporal 35, authz 11, mock 2, core 9, examples 3,
  Postgres 1,061 passed with 1 failure in `test_pg_isolation_conformance.py` under `-n 4`,
  which passed 3 of 3 alone; it does not touch this branch.
- `just quality -s`: every gate green.

Residue:

- The Kafka check is stopped and reported above, for a design call; a second Kafka finding was reported
  separately.
- `test_flush_skips_a_pubsub_destination` in `test_outbox_lifecycle.py` takes about 66 s on
  `origin/main` as well (a precondition error logged for 100 batches); not touched here.
- The lifecycle steps the earlier verification also named — consumers and a routed readiness
  probe — were not in this task's list; the review below guarded the queue consumers and
  records the rest as follow-ups.

**Drift count: 0.**

## Kafka follow-up checks — 2026-10-15

- Red first: the producer, consumer and admin refusals; the commit-stream consumer step over
  a mock and over a Kafka module; the two route shapes a floor now refuses.
- Sabotage, 5 mutants, all killed: the producer and admin resolving for no tenant again; the
  consumer step unguarded; the module deriving its tier from `tenant_aware` again; the mock
  commit-stream adapter hiding its tenancy.
- Unit: `just test tests/unit` — 18,040 passed, 16 skipped. Integration: Kafka 22, mock 2,
  core 9, examples 3. `just quality -s`: every gate green.

**Drift count: 0.**

## Review fixes — 2026-10-15

An independent review found one HIGH, five MEDIUM and three LOW issues. Each fix below was
reproduced red first.

```divergence
decision: unlisted
grade: UNLISTED
class: drift
at: 2026-10-15T17:00:00Z
attempt: 2
claim: stopping a loop on any configuration error stops every tenant's work on one tenant's: a tenant's key that is disabled or pending deletion is a configuration error too, and durable recovery unseals rows inside its claim
evidence: src/forze_kms/aws/kernel/client/errors.py:124-145 — key states mapped to configuration with the default code; src/forze/application/execution/background/supervise.py:40-43
action: decided
proposal: ASSUMED — supersedes the relay entry above. The relay and durable loops stop only on a wiring error, a configuration error whose code says so (`tenant_global_required`, `outbox_relay_tenant_unbound`, `core.crypto.payload_cipher_missing`); any other configuration error ends that tenant's pass and is retried next tick, and the rest of the shard goes on. Rejected: stopping when every tenant in a tick fails, because the unbound pass over a shared store is one "tenant" whose failure can still be one tenant's key. The cost is that a wiring error with the default code is logged every tick, as before this branch. Backlog: durable recovery unsealing per row inside `claim_abandoned` lets one bad row stall the claim for every tenant; the stream-trim, mailbox-retention and job-staleness loops stop on any configuration error at the tenant level, which predates this branch.
```

```divergence
decision: unlisted
grade: UNLISTED
class: drift
at: 2026-10-15T17:01:00Z
attempt: 2
claim: the providers check guarded the binding ports, which it reads only for a config provider, so a tenant-global catalog with tenant-aware bindings and any other provider was refused though it booted before
evidence: src/forze_identity/authz/services/grants.py:457-467 — bindings read only for keys a `ConfigGrantsProvider` owns
action: decided
proposal: ASSUMED — the catalog is guarded when a provider declares keys, the bindings only when a config provider owns some: the check refuses exactly the ports it reads.
```

```divergence
decision: unlisted
grade: UNLISTED
class: drift
at: 2026-10-15T17:02:00Z
attempt: 2
claim: the scheduler guarded its run store, but each fire enqueues under its schedule's tenant, so a shared schedule store beside a tenant-aware run store worked before and was refused
evidence: src/forze_kits/integrations/durable/scheduler.py:133-142 — the fire binds `schedule.tenant_id` before the run store is called
action: decided
proposal: ASSUMED — the unbound scheduler guards only its schedule store.
```

```divergence
decision: unlisted
grade: UNLISTED
class: drift
at: 2026-10-15T17:03:00Z
attempt: 2
claim: the widened wake check refused a wake on a routed client on every flush, though a flush runs bound; and the guard refused any routed client, though a deployment may point its provider at a default tenant
evidence: src/forze/application/execution/outbox/wiring.py:91-93 — the wake check runs when the outbox command is built, per operation
action: decided
proposal: ASSUMED — `OutboxWake.require_tenant_global` is back to its tenant-aware check, for the flush side; the relay checks its wake route with the shared guard when it starts. The guard refuses a routed client only when its own provider has no tenant here. Breaking for a relay whose wake sits on such a client; named in the changelog.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T17:04:00Z
attempt: 1
claim: the docs said every unbound lifecycle step or loop refuses a tenant-scoped port, but several have no guard
evidence: src/forze_kits/integrations/consumer/lifecycle.py:95 — the queue consumer startup, unguarded before this fix
action: decided
proposal: ASSUMED — the queue consumer step, and the notification consumer over it, get the shared guard in a new breaking commit; the tenancy matrix names the steps that check. Follow-ups, not changed: the unsharded SSE tail step, the socket.io gateway step, and a readiness probe over a routed client.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-15T17:05:00Z
attempt: 1
claim: the Kafka adapters did not resolve a per-tenant namespace the way the other backends do
evidence: src/forze/application/contracts/tenancy/mixins.py:91 — `_tenant_id_for_resolve`, the shared rule
action: decided
proposal: ASSUMED — the three adapters resolve through `_tenant_id_for_resolve`, still failing closed on a tenant-aware route with no tenant bound, and a per-tenant namespace is the `namespace` tier whatever `tenant_aware` says. Folded into the security commit.
```

Also fixed: the changelog no longer names the wrong error for the relays, and the tenancy
matrix says that `tenant_global=True` is the deployment's own word, and what a deployment on
a routed Redis client does for a lock or a wake.

Labels: the Temporal, S3 and Mongo commit and the identity commit stay `🐛`. With the guard
refusing a routed client only when it has no tenant to route by, and the identity check
guarding only the ports it reads, each refuses only what failed at startup before.

Verification:

- Sabotage, 13 mutants, all killed: the relay's tenant and tick levels stopping on any
  configuration error; durable recovery's tenant level the same; an empty wiring-code set;
  the identity check guarding the bindings always, or never; the scheduler guarding its run
  store again; the routed check ignoring its provider, or never refusing; the flush-side wake
  checking the client; the queue consumer unguarded; Kafka resolving only when tenant-aware;
  Kafka deriving its tier from `tenant_aware`.
- Unit: `just test tests/unit` — 18,061 passed, 16 skipped.
- Integration: Redis 234, Kafka 22 (one coordinator-not-ready error under `-n 4`, 22 of 22
  twice serially), S3 128 (2 skipped), Temporal 35, authz 11, mock 2, core 9, examples 3,
  Mongo 475 (serial), Postgres 1,061 with the same `-n 4` failure in
  `test_pg_isolation_conformance.py` as before, which passed twice alone.
- `just quality -s`: every gate green.

**Drift count: 4.**
