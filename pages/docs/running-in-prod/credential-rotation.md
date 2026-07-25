---
title: Credential rotation
icon: lucide/key-round
summary: Rotate access credentials without an outage — versions, change feeds, hot reload, the durable rotator, and leases
---

Every deployment holds credentials that eventually have to change: database
passwords, API keys, per-tenant DSNs. The dangerous part is not generating a new
password — it is the window between "the backend accepted the new credential" and
"every container observed it". Get the ordering wrong in exactly one way (publish
before verify) and the failure mode is a fleet-wide outage triggered by your own
rotation signal.

Forze splits the problem into layers you can adopt one at a time. Each layer is an
**accelerator** over the one guarantee that is always on:

> **Signals accelerate, the TTL floor guarantees.** Routed tenant pools re-resolve
> credentials and rebuild when their fingerprint changes; `fingerprint_ttl` makes
> that happen within a bounded window even if every signal below is lost. Wiring a
> change source does not remove the TTL — it lets you raise it.

## Versioned reads

Every secrets backend serves `resolve_versioned` / `current_version` — a
`SecretVersion` is an **opaque, equality-only token** (Vault yields integers, files
and env yield content hashes). Same token at one ref ⇒ same value; nothing more.
That contract is what makes change detection universal instead of Vault-only.

Backends declare what they honor through `SecretsCapabilities`, and every consumer
below fails closed (`secrets_feature_unsupported`) rather than degrading silently —
a watcher over a backend without versions, a rotator over a read-only store, a
lease manager over a store without a lease engine all refuse at wiring.

## Watching for changes

The change feed is its own small contract: `SecretChanged(ref, version)` delivered
by a `SecretsChangeSource`. Events never carry values — a consumer re-resolves
through its own authenticated store connection, which also makes a spoofed or
replayed event harmless (it can trigger a refetch, never inject a credential).

Two shipped sources cover most deployments:

```python
from forze_kits.integrations.secrets import (
    DirectorySecretsChangeSource,
    SecretsPollWatcher,
)

watcher = SecretsPollWatcher(secrets=versioned_backend, refs=[dsn_ref])
step = watcher.lifecycle_step()   # a supervised 30s tick; first tick primes silently
```

- **`SecretsPollWatcher`** polls `current_version` for a ref set and emits on
  deltas. Thirty seconds is the deliberate default: kubelet's own secret sync is
  minute-granular, and against Vault a tick is one metadata read per ref.
- **`DirectorySecretsChangeSource`** watches mounted secret files with Kubernetes
  semantics: it re-stats the *path* each tick (kubelet updates a Secret by
  atomically swapping the `..data` symlink — an inode watch sees the old file
  forever), and a stat gate keeps unchanged files at one `stat` per tick.
  **`subPath` mounts never update** — that is a Kubernetes fact no watcher can fix.

With the `watchfiles` package installed (an app-level dependency, deliberately not
a Forze extra), the directory source can also react to OS-native events instead of
waiting out the poll interval:

```python
steps = [
    file_source.lifecycle_step(interval=timedelta(minutes=5)),  # the floor, raised
    file_source.native_events_lifecycle_step(),                 # the accelerator
]
```

Event paths are deliberately never trusted — every native event just triggers the
same stat-gated diff, so a spurious burst costs one `stat` per ref. Keep the poll
step wired: native events let you *raise* its interval, never remove it.

Delivery is at-least-once, unordered, and advisory. That costs nothing: eviction on
an unchanged secret re-resolves, recomputes an equal fingerprint, and rebuilds
nothing — over-notification is free by design.

## Hot reload

The binder turns changes into evictions:

```python
from forze_kits.integrations.secrets import SecretsHotReloadBinder

binder = SecretsHotReloadBinder(
    sources=[watcher],
    routed_clients=[routed_postgres, routed_mongo],
)
steps = [watcher.lifecycle_step(), binder.lifecycle_step()]
```

For each event it recomputes the cached tenants' refs against the changed ref and
calls `evict_tenant` on the matches; the next access re-resolves and rebuilds.

**Non-routed singleton pools follow a different doctrine: re-resolve at
connection-establishment time.** A Postgres/MySQL password is checked only at
connect and established connections survive rotation, so a connect-time
`resolve_str` makes the rotation window race-free with or without a signal. The
binder's `on_change` callbacks (e.g. a soft pool recycle) only accelerate draining
of old connections — they are never the correctness mechanism.

## Rotation notifications across containers

The rotator publishes `SecretRotated {ref, version, at}` — never a value — through
the **outbox**, relayed onto a broadcast pub/sub channel; every app container wires
a `PubSubSecretsChangeSource` into its binder exactly like a poll watcher:

```python
from forze_kits.integrations.secrets import (
    PubSubSecretsChangeSource,
    secret_rotated_pubsub_spec,
)

spec = secret_rotated_pubsub_spec()
query = ctx.deps.resolve_configurable(ctx, PubSubQueryDepKey, spec, route=spec.name)
binder = SecretsHotReloadBinder(sources=[PubSubSecretsChangeSource(query=query)], ...)
```

Pub/sub is at-most-once and live-only; a missed message is covered by the TTL
floor. The channel needs no dedup, ordering, or persistence beyond the outbox's.

## The rotator

`SecretRotator` runs one rotation as a **durable four-step run** per
`(ref, tenant)` — create → set → test → finish, the AWS Secrets Manager ordering,
because it is the known-safe one:

1. **create** — mint from CSPRNG (`SecretEntropy`; a seeded simulation source
   *cannot* be passed here), compose the pending value through the backend target,
   stage it at `<path>.pending`;
2. **set** — `RotationTargetPort.apply`: make it valid at the backend (idempotent);
3. **test** — `RotationTargetPort.verify`: a **real connection**, not a syntactic
   check. Failure halts the run before promote — promoting an unverified
   credential and evicting the fleet onto it is a self-inflicted outage;
4. **finish** — promote, confirm, then publish `SecretRotated` via the outbox.
   Promotion is fenced in depth, because the distributed lock is advisory: the
   staged version must still be the one this run verified (no unverified text can
   be promoted); the write to the primary ref is **compare-and-set** against the
   version observed at create (a competing rotation that already promoted wins;
   the stale run fails loudly instead of clobbering it); and because a backend
   write like `ALTER ROLE` is not fenceable at all, the winner **re-verifies the
   promoted credential after the promote and converges the backend** if a stale
   apply landed late — a credential that still fails then fails the run loudly
   rather than publishing. Finally, a **delayed reconfirmation run**
   (`reconfirm_after`, default 60s) re-asserts the canonical credential past the
   only physical bound a stale in-flight statement has — the stale worker's own
   statement timeout. The shipped Postgres target enforces that bound itself
   (`apply_statement_timeout`, default 30s — the `ALTER ROLE` runs under a
   server-side `SET LOCAL statement_timeout`); keep `reconfirm_after` above it,
   and apply the same pairing to any custom target.

The pending ref is what makes this crash-safe: after **set**, the only copy of a
password already live at the backend exists durably in the secret store. A rotator
container that dies mid-rotation is reclaimed after its durable lease and resumes
from the last completed step; step results carry `{ref, version}` only, so secret
text never touches a journal. A distributed lock single-flights concurrent
rotations of one ref across replicas.

```python
from forze_kits.integrations.secrets import SecretRotator
from forze_postgres import PostgresRotationTarget

target = PostgresRotationTarget(secrets=secrets, client=admin_pg, role_pair=("app_a", "app_b"))
rotator = SecretRotator(target=target)
rotator.register(durable_registry)

await rotator.rotate_now(ctx, dsn_ref)                     # admin trigger
await rotator.ensure_cron(ctx, dsn_ref, cron="0 4 * * 0")  # weekly policy
```

Multi-tenant fleets enqueue one run per tenant (`rotator.enqueue_tenants`) — one
failing verify never blocks the rest, and a partial pass resumes where it stopped.

### The Postgres target: dual-user alternation

`PostgresRotationTarget` defaults to **two roles** (`app_a`/`app_b`): the rotation
sets the minted password on the *idle* role, verifies it with a live connection,
and promotes a DSN naming that role. The previously-active role stays valid through
the whole propagation window and becomes the idle target of the next rotation — no
moment exists where a credential in flight is invalid.

Single-role `ALTER ROLE ... PASSWORD` is available behind an explicit
`single_role_degraded=True`: between promote and full propagation, *new*
connections with the old password fail (established ones survive), so with
connect-time re-resolution the blast radius is retry noise — but it is documented
degraded for a reason.

Run the rotator as a utility container in the outbox-relay shape: a headless
`build_runtime(...)` composition wiring `{secrets + secrets_admin, durable, dlock,
outbox/pubsub, rotation targets, tenant directory}` with the durable recovery and
scheduler steps — no HTTP surface. See the runnable walkthrough in
`examples/recipes/secrets_rotation/`.

## Leases (dynamic credentials)

Where a backend adopts a lease engine (Vault database engines), short TTLs *are*
the rotation and the rotator becomes unnecessary for that backend. Credentials are
per-issuance (each container becomes a distinct DB principal) and revocation is
hard-edged (it kills established connections), so `SecretsLeaseManager`
renews at ~⅔ TTL with jitter, **reissues-then-drains** before `max_ttl`, and on
renewal failure retries forever while escalating log severity — abandoning means
certain credential death at TTL.

```python
from forze_kits.integrations.secrets import SecretsLeaseManager

manager = SecretsLeaseManager(
    dynamic=vault_dynamic,          # forze_vault.VaultDynamicSecrets
    roles=[SecretRef("app-readwrite")],
    on_credential=rebuild_pool,     # the same hot-reload path as any rotation
)
step = manager.lifecycle_step()
```

## Security invariants

- **Values never leave the resolve path.** Not in change events, not in rotation
  notifications, not in durable journals, not in logs, not in watcher snapshots.
  Everything except `resolve_*` / `issue` traffics in `{ref, version}`.
- **No caching inside secrets adapters.** Freshness is the entire point of this
  plane; a resolve-path cache reintroduces exactly the staleness the watcher exists
  to kill.
- New secrets are minted from `SecretEntropy` only — the type split makes a
  replayable entropy source unpassable, so a simulated rotator can never produce a
  predictable production credential.
