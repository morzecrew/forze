# RFC 0068 — Cross-process grants cache invalidation

- **Status:** 📝 Draft — design proposed; builds on the opt-in grants cache shipped in #506
- **Scope:** Makes a grants-cache invalidation in one process reach every other process that holds
  a `GrantsCache`, so a revoked role stops granting everywhere within broadcast latency instead of
  within the TTL. Adds an optional broadcast setting on the authz kernel, a lifecycle step that
  subscribes each process to it, a publish after each committed invalidation, and an app-facing
  call for invalidations made outside role assignment. Reuses the cache contract's existing
  push-invalidation capability (`SupportsInvalidationPush`) as the transport; no new port, no new
  contract. The in-process cache, its key, its epoch and its TTL are unchanged; the TTL stays as
  the backstop. Does not make invalidation transactional, does not hook plain document commands,
  and changes nothing when the setting is absent.
- **Related:** [`src/forze_identity/authz/services/grants_cache.py`](../src/forze_identity/authz/services/grants_cache.py)
  (`GrantsCache` `:25`, `begin` `:90`, `put` `:123`, `forget` `:149`, `clear` `:160`, from #506);
  [`src/forze_identity/authz/services/grants.py:277`](../src/forze_identity/authz/services/grants.py)
  (the resolver's after-commit `forget`);
  [`src/forze_identity/authz/adapters/role_assignment.py:106,141`](../src/forze_identity/authz/adapters/role_assignment.py)
  (the `forget` calls on assign and revoke);
  [`src/forze_identity/authz/execution/deps/configs.py:59`](../src/forze_identity/authz/execution/deps/configs.py)
  (`AuthzKernelConfig.grants_cache`);
  [`src/forze/application/contracts/cache/invalidation.py:20-51`](../src/forze/application/contracts/cache/invalidation.py)
  (`CacheInvalidation`, `SupportsInvalidationPush`);
  [`src/forze/application/integrations/document/cache.py:168-193`](../src/forze/application/integrations/document/cache.py)
  (the document L1, the one existing subscriber);
  [`src/forze_redis/adapters/cache.py:283`](../src/forze_redis/adapters/cache.py) and
  [`src/forze_redis/kernel/client/client.py:612-660`](../src/forze_redis/kernel/client/client.py)
  (`CLIENT TRACKING` BCAST, reset on reconnect);
  [`src/forze/application/contracts/pubsub/ports.py:14-60`](../src/forze/application/contracts/pubsub/ports.py)
  (the at-most-once pub/sub ports, the transport not chosen).
- **Origin:** The erp-backend issue list (P1) asked for a grants memo with explicit invalidation;
  #506 ships it per process. Its review asked what a mature framework does about the other
  processes; the answer was "broadcast, with the TTL as the backstop".

---

## 1. Summary

When an app configures `GrantsInvalidation(cache=<a cache spec whose port can push
invalidations>)` next to its `GrantsCache`, every committed invalidation — `assign_role`,
`revoke_role`, and the app's own `invalidate_grants` calls — writes one marker key through that
cache port. The cache backend broadcasts the write to every subscribed process, and each process
drops the principal's entries (or everything, for a catalog change) from its own `GrantsCache`. A
process that may have missed broadcasts — on reconnect or a stream failure — flushes its whole
cache, which the push contract already signals. The TTL keeps bounding staleness for anything a
broadcast does not reach.

## 2. Motivation

#506 caches the catalog-derived grants per process. Within a process, assign and revoke forget the
principal when they commit. Every other process keeps serving the old grants until its entry
expires: with a five-minute TTL, a revoked administrator keeps administrator rights on every other
worker for up to five minutes. The identity docs say so, which makes the cache honest, but it
leaves users choosing between a short TTL, which gives up most of the benefit (erp-backend's own
memo took `/auth/me` from 8.7 ms to 2.2 ms), and a revocation delay a security review will flag.

A revocation is the case where the delay matters, and it is also the rare one: grants change far
less often than they are read, so broadcasting each change costs almost nothing.

## 3. Current state

Verified against main (with #506):

- **The grants cache is per process.** `GrantsCache` holds `(principal, tenant, scope) → grants`
  with an LRU bound, a TTL measured from when the read began (`begin()`), and an epoch that every
  `forget`/`clear` advances so a read overtaken by a forget is never stored (`put`). Decisions
  inside a transaction bypass it.
- **Local invalidation is after commit.** `AuthzGrantResolver.forget` defers `cache.forget` to the
  transaction's commit (`run_or_defer`), or runs it at once outside a transaction. Assign and
  revoke call it on every path, the idempotent no-op included.
- **The cache contract can already push invalidations.** `SupportsInvalidationPush` is an optional
  capability of a cache port: `subscribe_invalidations(callback)` delivers
  `CacheInvalidation(key=...)` for each key any client writes, expires or evicts, and
  `CacheInvalidation(key=None)` — *flush everything* — on every (re)connect and on degradation,
  "so a subscriber never trusts state that predates a gap in the stream". It returns `None` when
  push is unavailable.
- **Redis implements it** with `CLIENT TRACKING` in BCAST mode over one pinned RESP3 connection
  (redis-py 8+ and a static namespace; a dynamic per-tenant namespace or a tenant-routed client
  returns `None`). A client's own writes are broadcast to itself too. It tracks the **pointer**
  scope only (`adapters/cache.py:283`): `set_versioned` re-sets a pointer and `delete` unlinks
  one, so both broadcast, but a plain `set` writes the separate KV scope, which is not tracked and
  broadcasts nothing.
- **The document L1 is the only subscriber today.** It drops the touched entry, or flushes on
  `key=None`.
- **No in-memory implementation exists.** `forze_mock` cache ports do not implement
  `SupportsInvalidationPush`, so nothing in the simulation can exercise a broadcast yet.
- **The pub/sub ports are at-most-once and hide gaps.** The Redis client's subscribe generator
  reconnects internally (`pubsub_auto_reconnect`) and reports a reconnect only to an optional
  client-level hook (`on_pubsub_reconnect`); a consumer of `PubSubQueryPort.subscribe` cannot tell
  that it missed messages.

## 4. Goals / Non-goals

**Goals**

- A committed invalidation reaches every subscribed process within broadcast latency.
- A process that may have missed an invalidation flushes rather than serving stale grants.
- Misconfiguration fails at startup, never as a silent fall-back to TTL-only.
- Nothing changes for an app that does not configure it.

**Non-goals**

- **Not strong consistency.** Between a commit and the broadcast's arrival another process can
  still serve the revoked grant; the window is the propagation latency, documented, as it already
  is within one process.
- **Not automatic invalidation for plain document commands.** A binding written through a document
  command has no hook; the app calls `invalidate_grants`, as #506's docs already ask it to call
  `forget`/`clear`.
- **Not a shared cache.** Grants stay in process memory; only invalidations travel.
- **Not cross-region delivery.** One broadcast domain per cache backend.

## 5. Design

### 5.1 Configuration

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GrantsInvalidation:
    """Broadcast grants-cache invalidations to every process through a cache backend."""

    cache: CacheSpec
    """A cache spec with a static namespace whose port implements SupportsInvalidationPush."""

    marker_ttl: timedelta = timedelta(minutes=1)
    """How long a marker key lives. Its expiry broadcasts too: a second, idempotent forget for a
    principal marker, and a second full flush for the catalog marker (§9)."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzKernelConfig:
    ...
    grants_cache: GrantsCache | None = None
    grants_invalidation: GrantsInvalidation | None = None
```

`grants_invalidation` without `grants_cache` is refused as a configuration error: there is nothing
to invalidate.

### 5.2 Subscribing: a lifecycle step

A startup step resolves the cache port for `grants_invalidation.cache` and calls
`subscribe_invalidations(on_invalidation)`. If the port does not implement the capability, or the
call returns `None`, startup fails with a configuration error naming the spec and the reason (push
unavailable, dynamic namespace, tenant-routed client, redis-py older than 8). The shutdown step
calls the returned unsubscribe.

```python
ALL_MARKER = "all"
PRINCIPAL_PREFIX = "principal:"


def parse_principal_marker(key: str) -> UUID | None:
    if not key.startswith(PRINCIPAL_PREFIX):
        return None
    try:
        return UUID(key.removeprefix(PRINCIPAL_PREFIX))
    except ValueError:
        return None


def on_invalidation(inv: CacheInvalidation) -> None:
    if inv.key is None or inv.key == ALL_MARKER:
        grants_cache.clear()          # a gap in the stream, or a catalog-wide change
        return
    if (principal_id := parse_principal_marker(inv.key)) is not None:
        grants_cache.forget(principal_id)
    # any other key under the namespace is ignored
```

The callback is synchronous and cheap, as the contract requires. `forget`/`clear` advance the
epoch exactly as a local invalidation does, so a read in flight when the broadcast arrives is not
stored.

### 5.3 Publishing: after commit

The marker key is the transport: writing it is what the backend broadcasts.

| Invalidation | Marker written |
| --- | --- |
| one principal (`assign_role`, `revoke_role`, `invalidate_grants(principal_id)`) | `principal:<uuid>` |
| the catalog (`invalidate_grants()` with no principal) | `all` |

The resolver's `forget` already runs after commit. With a broadcast configured, at that point it
does the local `cache.forget` (unchanged) and then writes the marker through the **versioned**
path, `set_versioned(marker, version=<a fresh token>, value=<the token>, ttl=marker_ttl)`. That
re-sets the marker's pointer, which is what Redis tracks; a plain `set` would write the untracked
KV scope and reach no other process (§3). A fresh version each time makes every write a change.
The local
process also receives its own broadcast; the second forget is idempotent and costs one extra miss.

A failed publish cannot undo a committed write. It is logged at error level with the principal id
and counted on a metric; the TTL bounds the effect. The write's caller is not failed.

### 5.4 The app-facing call

Plain document commands that change bindings need a broadcast too. The resolver gains

```python
async def invalidate_grants(self, principal_id: UUID | None = None) -> None:
    """Forget one principal's cached grants — or all of them — here and, when a broadcast is
    configured, in every subscribed process, once the current transaction commits."""
```

reachable from app code the same way `forget` is today (row 7). `GrantsCache.forget` and `clear`
stay synchronous and local, for tests and single-process apps.

### 5.5 Alternatives considered

- **The pub/sub ports.** Generic across backends, but at-most-once with no gap signal: the Redis
  subscribe generator reconnects silently, so a process could miss a revocation and never know.
  Using it safely would mean adding a gap signal to `PubSubQueryPort` — a contract change for every
  pub/sub backend — or a per-publisher sequence that detects loss only when that publisher's next
  message arrives. The cache push capability already carries the gap signal; that decided it.
- **A shared cache instead of a per-process one.** Consistent, but every decision pays a network
  round trip, which removes most of what the cache saves after #501's batching.
- **A version stamp read on every decision.** Strongly consistent, but also a read per decision,
  and every binding write would have to bump it, plain document commands included, which have no
  hook.
- **Postgres `LISTEN`/`NOTIFY`.** Its notifications are delivered only on commit, which is exactly
  the ordering wanted. It ties the identity plane to Postgres while the plane also runs on MongoDB
  and Firestore; recorded as a possible second transport (§8).
- **TTL only.** What #506 ships; kept as the default and as the backstop.

## 6. Tests

- **Unit:** the callback maps `principal:<uuid>` to `forget`, `all` and `key=None` to `clear`, and
  ignores foreign keys; publishing happens after commit and not on rollback; a failed publish is
  logged, counted and does not fail the caller; the misconfiguration refusals (no grants cache, no
  push capability, `None` from subscribe).
- **In-memory push:** a `forze_mock` cache port implementing `SupportsInvalidationPush` — the
  missing piece in §3 — with a way to drop events and to trigger a reset.
- **Simulation:** two processes sharing the mock backend. A revoke on one is denied on the other
  once the broadcast is delivered; a dropped event followed by a reset flushes; a dropped event
  with no reset is bounded by the TTL. `no_permission_after_deactivation` runs with the broadcast
  on.
- **Redis integration:** two clients on one Redis; a revoke through one is seen by the other;
  killing the tracking connection produces a flush; the publisher writes through
  `set_versioned`, pinned by a test that a plain `set` marker reaches no other client.

## 7. Docs

The identity page's grants-cache section gains the broadcast: how to configure it, the startup
refusals, what the window between commit and arrival means, that a missed broadcast flushes, and
that `invalidate_grants` is the call after plain document commands. The TTL wording changes from
"how long other processes may be stale" to "how long a change nothing broadcasts may be stale".

## 8. Out of scope

- **Hooks on the identity specs' document commands**, so plain writes invalidate by themselves.
  Valuable, but it changes the document plane; named as the follow-up that would make
  `invalidate_grants` unnecessary.
- **A Postgres `NOTIFY` transport.** Transactional delivery for Postgres-only deployments; build it
  if a deployment cannot run a push-capable cache.
- **A gap signal on the pub/sub ports.** Would make any pub/sub backend usable here; a contract
  change in its own right.

## 9. Risks

- **The backend is a single point.** If Redis is down, broadcasts stop; the tracking hub reports a
  reset on reconnect and every process flushes. While it is down, staleness is bounded by the TTL.
  Accepted and documented.
- **A catalog change flushes twice.** The `all` marker's expiry broadcasts a second time, so
  every process clears its cache once on delivery and again `marker_ttl` later. Catalog changes
  are rare and a flush only costs misses; accepted, and named in the docs. A principal marker's
  expiry repeats an idempotent forget.
- **Flush storms.** Every reconnect flushes every process's cache. On a flapping connection the
  cache stops helping but stays correct. Accepted; a metric on flushes makes it visible.
- **Read as stronger than it is.** "Broadcast" may be read as "instant". The docs state the
  commit-to-arrival window and the publish-failure case plainly.
- **Marker keys in a shared namespace.** A spec reused for something else would deliver foreign
  keys; the callback ignores anything that is not a marker, and the docs ask for a dedicated spec.

## 10. Unresolved questions

- How `invalidate_grants` is exposed to app code: a method on the resolver reached through the
  kernel, or a small port of its own. Implementation decides (row 7).
- Whether the marker value should carry anything (an origin id to skip self-echo, a timestamp for
  diagnostics). Not needed for correctness; implementation decides (row 8).

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Grants stay in process memory; only invalidations travel, and the TTL remains the backstop for anything a broadcast does not reach. Changing this means a shared cache and a per-decision round trip. |
| 2 | `LOCKED` | An invalidation is broadcast only after the change commits, at the same point the local forget runs. Broadcasting before commit would let other processes re-cache state the commit is about to replace. |
| 3 | `LOCKED` | A process that may have missed broadcasts flushes its whole grants cache, on every `CacheInvalidation(key=None)`. Serving grants across an unacknowledged gap is the failure this RFC exists to prevent. |
| 4 | `ASSUMED` | The transport is the cache contract's `SupportsInvalidationPush` (Redis `CLIENT TRACKING` today), carried by marker keys under a dedicated static namespace and written through `set_versioned`, the path the push tracks — not the pub/sub ports, which cannot signal a gap. |
| 5 | `LOCKED` | Configuring a broadcast that cannot run — no grants cache, a port without push, a `None` subscription — fails at startup. A silent fall-back to TTL-only would leave the app believing revocation is prompt. |
| 6 | `ASSUMED` | A failed publish after commit is logged and counted, never raised to the caller: the write has committed and cannot be undone, and the TTL bounds the effect. |
| 7 | `OPEN` | How app code reaches `invalidate_grants` (a resolver method via the kernel, or a dedicated port). Decide by what the kernel already exposes for `forget`; log the choice. |
| 8 | `OPEN` | What the marker value carries (nothing, an origin id, a timestamp). Correctness does not depend on it; decide and log. |
| 9 | `ASSUMED` | A received invalidation advances the cache epoch exactly as a local one does, so a read in flight is not stored after it. |
| 10 | `ASSUMED` | Self-echo is not suppressed: the publishing process receives its own marker and forgets again, which is idempotent. |

## 12. Phasing

- **P1** — the in-memory push port in `forze_mock`, `GrantsInvalidation`, the
  lifecycle step, after-commit publishing, `invalidate_grants`, the simulation and Redis tests, the
  docs. One PR.
- **P2** — demand-gated: document-command hooks on the identity specs (§8), or a Postgres `NOTIFY`
  transport.
