# RFC 0069 — Session revocation fast path

- **Status:** 📝 Draft
- **Scope:** An opt-in fast path for verifying an access token's session that replaces the
  session-row read with one Redis read while Redis answers. Once two preconditions hold, it
  replaces the policy-principal eligibility read too. A verification admits the token from two
  Redis keys:
  - a per-session *admission* record, written when the session is issued;
  - a per-principal *generation*.

  Anything other than a clean match falls back to today's authoritative reads. Revocation
  invalidates through the transactional outbox, staged in the same transaction as the row
  change, so an invalidation is never lost, only delayed by its delivery. Touches:
  - the `forze_identity` token verifier, the session-issuing and session-revoking adapters,
    and the authn kernel configuration;
  - a small session-admission port, with Redis and mock adapters, written by an outbox relay.

  It needs the revocation paths to run inside a transaction, which today none do, and session
  issue to serialize with deactivation before the eligibility read can go. The
  session row stays the source of truth, `forbid_cache_and_history` stays, and no reader
  ever writes. API keys, OIDC, and any change to the token format are out of scope.
- **Related:** [`src/forze_identity/authn/verifiers/forze_jwt_token.py:80-133`](../src/forze_identity/authn/verifiers/forze_jwt_token.py)
  (`_require_active_session`);
  [`src/forze/application/integrations/authn/orchestrator.py:240`](../src/forze/application/integrations/authn/orchestrator.py)
  (the eligibility check after every token verification);
  [`src/forze_identity/authn/adapters/principal_eligibility.py:32-42`](../src/forze_identity/authn/adapters/principal_eligibility.py);
  [`src/forze_identity/authn/adapters/_utils.py:145-172`](../src/forze_identity/authn/adapters/_utils.py)
  (`revoke_sessions_matching`);
  [`src/forze_identity/authn/adapters/token_lifecycle.py:153-260`](../src/forze_identity/authn/adapters/token_lifecycle.py)
  (logout, family revocation, refresh rotation);
  [`src/forze_identity/authn/services/access_token.py:46-56`](../src/forze_identity/authn/services/access_token.py)
  (`AccessTokenClaims`);
  [`src/forze_identity/_secure_spec.py`](../src/forze_identity/_secure_spec.py)
  (`forbid_cache_and_history`);
  RFC 0068 (why a pub/sub broadcast cannot carry an invalidation that must not be missed);
  the outbox relay wake (`OutboxWake`, PR #546).
- **Origin:** hh-backend-v2's realtime load tests (request 4 of 7, 2026-10-13). The
  application re-validates a socket's session on every command; at connect storms
  authentication was about 18% of the realtime process's CPU, about 7% of it the session-row
  read.

---

## 1. Summary

A token verification with the fast path enabled replaces the session-row read with one Redis
`MGET`. It admits the token when three things hold:

- the session's admission record exists;
- the record carries the principal's current generation;
- the record names the token's subject and tenant.

Every other outcome takes today's path: a miss, a mismatch, a Redis error or a timeout. A
session-scoped invalidation (rotation, family revocation) deletes the session's admission
record. A principal-scoped one (logout, password change or reset, deactivation) drops the
principal's generation, which strands every admission record minted under it; the next
session issued mints a fresh one.
Both are staged in the outbox inside the transaction that changes the rows, and relayed to
Redis. Because Redis only ever holds *admissions*, a lost, evicted or flushed key costs a
Postgres read and never admits a revoked session. The one window the fast path has is an
invalidation still in flight. Normally its length is the outbox's delivery latency. A
freshness fence that only the relay refreshes, and only while nothing on the route has failed
or is pending too long, bounds it under every failure: a stuck, failed or undeliverable
invalidation turns the fast path off within the fence's TTL (§5.4).

The eligibility read stays until two things hold: session issue serializes with deactivation,
and every write that deactivates a principal drops its generation. Then a present generation
stands in for that read too (§5.2), and a verification reads no Postgres at all.

## 2. Motivation

Each access-token verification today costs two document reads:

- the session row in `_require_active_session` (`forze_jwt_token.py:96-108`);
- the policy principal's `is_active`, read by the orchestrator's eligibility check right
  after (`orchestrator.py:240`, `principal_eligibility.py:32-42`).

An application that re-validates on every command, as realtime apps must to notice a
logout, pays both reads per command per socket. That cost grows with traffic, and adding
realtime replicas does not reduce it: it lands on the database.

forze deliberately refuses the obvious fix. `forbid_cache_and_history` rejects a cached
session spec at construction, because a cached row is a revocation that takes effect a TTL
late. That stance is correct and this RFC keeps it. The question is whether a cheaper check
can keep the property the cache would lose: *a revoked session is refused on its next
check*. It can, except for invalidations still being delivered.

## 3. Current state

All verified against the tree at the time of writing.

- **Verifier.** `ForzeJwtTokenVerifier._require_active_session` (`forze_jwt_token.py:80-133`):
  - reads the `sid` claim and makes one `find` on the session spec by id;
  - refuses a missing row, `revoked_at` set, or `rotated_at` set (`session_revoked`);
  - then checks `session.principal_id` against `sub` (`session_subject_mismatch`), and `tid`
    against `session.tenant_id` when both are present (`session_tenant_mismatch`);
  - does not check the session's `expires_at`. The access token's own `exp` bounds the token.
- **Eligibility.** After the verifier, the orchestrator calls
  `eligibility.require_authentication_allowed(principal_id)`. The default adapter reads the
  policy principal and checks `is_active`.
- **Claims.** `AccessTokenClaims` holds `iss`, `aud`, `sub`, `iat`, `exp`, and optional `tid`
  and `sid` (`access_token.py:46-56`). There is no family id, `jti` or version claim. Access
  tokens live 15 minutes by default.
- **Session model.** `principal_id`, `tenant_id`, `family_id`, `refresh_digest` and
  `expires_at`, plus the mutable `revoked_at`, `rotated_at` and `replaced_by`. The spec is
  `authn_token_sessions`: sensitive, with no cache and no history.
- **What invalidates a session.** Nothing deletes a session row; invalidation stamps it.

  | Path | Scope | Where |
  | --- | --- | --- |
  | Logout (`revoke_tokens`) | every session of the principal, across tenants | `token_lifecycle.py:153` |
  | Family revocation, refresh-token reuse | one family | `token_lifecycle.py:168`, `:200-212` |
  | Refresh rotation | one session: `rotated_at` on the old row | `token_lifecycle.py:180-260` |
  | Password change (when configured) | every session of the principal | `password_lifecycle.py:140-146` |
  | Password reset (when configured) | every session of the principal | `password_reset.py:317-323` |
  | Principal deactivation | `is_active=False`, then every session | `principal_deactivation.py:38-41` |
  | Authz registry `deactivate_principal` | `is_active=False` only, no sessions | `principal_registry.py:102-112` |

- **No transaction anywhere in that chain.**
  - The authn kit binds no transaction route.
  - `revoke_sessions_matching` reads then updates outside any transaction (`_utils.py:145-172`).
  - Principal deactivation is three sequential writes.
  - Nothing uses `run_or_defer`.
- **No Redis in `forze_identity`.** The only cache there is `GrantsCache`: per process, with
  TTL and an epoch, invalidated after commit (`grants.py:277-293`).
- **Mock gap.** The mock's `deactivate` emits the event but revokes no sessions
  (`forze_mock/adapters/identity/authn.py:371-376`), unlike the real adapter.

## 4. Goals / Non-goals

**Goals**

- No session-row read on a token verification while Redis answers and nothing about the
  session or principal changed (phase 2). Once its preconditions hold, no eligibility read
  either (phase 2b).
- A revoked or rotated session, or an ineligible principal, is refused by the fast path once
  its invalidation has been delivered. Delivery is bounded by the outbox relay; with a wake,
  that is milliseconds.
- Redis being down, slow, flushed, restarted or evicting keys falls back to today's reads.
  None of those can admit a session the authoritative path would refuse.
- Opt-in, and absent by default with no behaviour change.

**Non-goals**

- **Not a cache of the session row.** Readers never write; the row stays the only truth.
- **Not zero-latency revocation under every failure.** An invalidation not yet applied leaves
  the fast path admitting, but only until the freshness fence lapses. §9 states the bound.
- **Not a token format change.** `sub`, `sid` and `tid` are enough.
- **Not a general authz cache.** Grants stay in RFC 0068's territory.

## 5. Design

### 5.1 What Redis holds

Two key kinds and one fence, under a dedicated prefix:

```text
authn:admit:g:{principal_id}  ->  <generation>                       (no TTL)
authn:admit:s:{session_id}    ->  <generation>|<principal_id>|<tenant_id or ->
                                   TTL = session.expires_at - now
authn:admit:fresh             ->  1                                  TTL = fence TTL (§5.4)
```

- **A generation is random (128 bits), not a counter.** After a flush, a re-created
  generation is a value no admission record holds, never a reused one, so a flush can only
  cause misses.
- **An admission record says "this session was admitted under this generation".** It is
  never a statement that a session is revoked. That is the asymmetry the design rests on:
  any key that is missing reads as "ask Postgres".

### 5.2 The fast path

```python
# inside _require_active_session, when the fast path is configured
record, generation, fresh = await admit.mget(f"s:{sid}", f"g:{sub}", "fresh")  # one round trip

if fresh is not None and record is not None and generation is not None:
    gen, principal, tenant = record.split("|")
    if gen == generation and principal == sub and tenant == (tid or "-"):
        return                                                   # admitted, no read

# otherwise, the authoritative path exactly as today
```

- **Failure semantics:**
  - A Redis error, or the bounded timeout expiring, is logged and counted, then falls through
    to the authoritative path. It is never raised.
  - The fast path never refuses: refusal stays the authoritative path's job, so its codes and
    messages do not change.
- **Eligibility is not covered yet.** It would be tempting to argue that an admission under the
  current generation implies an eligible principal, because a deactivation drops the
  generation. Login and deactivation can interleave so that it does not:

  1. Login checks eligibility: the principal is active.
  2. Deactivation commits `is_active=False` and revokes the principal's sessions. The one about
     to be issued does not exist yet.
  3. The outbox drops the generation.
  4. Login reads the generation for its snapshot, finds none, and mints a fresh one.
  5. Login commits the session and admits it under that generation.

  The session is active in Postgres, so the session check is right to admit it. Only today's
  per-request eligibility read refuses it. So in phase 2 the orchestrator keeps that read.

  It can go (phase 2b) once two things hold:
  - Every flow that issues a session, login (`issue_tokens`, `token_lifecycle.py:104`) and
    refresh (`refresh_tokens`, `:217`) alike, checks eligibility and inserts the session in one
    transaction that reads the policy principal with a lock (`for_update=True`, the document
    port's row lock). A concurrent deactivation then waits for the insert, or the issue waits
    for the deactivation.
  - Every write of `is_active=False` drops the generation. `deactivate_principal` in the authz
    registry, which revokes no sessions, must do so too.

  Then a present generation means "no deactivation since the last session issued under it",
  and an eligibility decorator treats it as eligible (decision 9).

### 5.3 Who writes

| Event | Redis effect | Delivered by |
| --- | --- | --- |
| A session is issued (login, refresh) | `SET s:{sid}` with the generation **read before the session row commits** | after commit, best effort |
| Refresh rotation | `DEL s:{old_sid}` | outbox |
| Family revocation, reuse detection | `DEL s:{sid}` for each session of the family | outbox |
| Logout, password change or reset, principal deactivation | `DEL g:{principal}` | outbox |
| Any other write of `is_active=False` on the policy principal | `DEL g:{principal}` | outbox |

**The snapshot is what makes an issue race safe.** Without it, a logout committing between
the new session's commit and its admission write would drop the generation, the admission
write would mint a fresh one, and the record would admit a session the logout revoked. Read
before the commit, the admission carries the old generation, which no longer exists, and
simply never matches. A principal with no generation gets one with `SET NX`, read back in
the same step (one script, so a concurrent drop cannot fall between them).

An admission write that fails costs that session its fast path (every check misses), so it
does not need the outbox. An invalidation that fails would admit a revoked session, so it
does.

### 5.4 Delivery through the outbox

- **Staging.** Each revoking adapter stages its invalidation in an outbox route dedicated to
  session admission, in the transaction that stamps the rows. The payload is the session ids
  to delete, or the principal whose generation to drop. The adapters get the route's outbox
  command port from their deps factory, which already builds them per request.
- **Enforced.** The route declares `require_transaction=True`, so a revocation run outside a
  transaction fails as a configuration error rather than staging an invalidation that is not
  atomic with its rows.
- **Relaying.** A relay applies the invalidations to Redis, with the route's `OutboxWake`, so
  delivery follows the commit by milliseconds. A duplicate is safe. A session `DEL` is
  idempotent. A generation drop delivered twice can also strand an admission minted between
  the two deliveries, which costs that session Postgres reads until its next refresh, never a
  wrong admission.
- **The freshness fence.** The relay's retry policy can park a row as `failed` after its last
  attempt, and a row it cannot deliver stays pending. Either would leave a revoked session
  admitted for as long as its admission record lives (up to the session's lifetime), so
  delivery alone is not enough. After each pass, the relay asks the route's outbox admin port
  for its depth and its oldest pending age. It refreshes `fresh` (`SET ... EX`, the fence TTL)
  only when the route has no `failed` row and nothing pending or processing older than the
  fence's tolerance. The fast path admits only while `fresh` exists. So:
  - an invalidation that is parked, pending past the tolerance, or held behind a relay that
    cannot reach Redis stops the fence from being refreshed;
  - within the fence TTL every verification falls back to Postgres, until the backlog drains
    and an operator requeues anything parked;
  - the worst-case window for any invalidation is the tolerance plus the TTL. With a wake and a
    healthy relay it is still milliseconds.

  The relay must pass more often than the fence TTL, so the route's poll interval is set below
  it. That is one claim query and two admin probes per interval on an idle route.
- **Precondition: the revocation paths must run in a transaction.** None does today (§3). The
  authn kit binds a transaction route on its operations, configurable as
  `build_authn_registry(tx_route=...)`. The adapters hold ports, not the execution context, so
  a scope inside them would mean threading a transaction manager through each one and nesting
  awkwardly in an app's own transaction. An app calling the adapters directly opens its own
  transaction, and the `require_transaction` check above makes forgetting it loud. This is
  phase 1, and it is worth doing on its own: principal deactivation's three sequential writes
  can today leave a principal inactive with live sessions, or the reverse.

### 5.5 Configuration

```python
class SessionAdmissionPort(Protocol):
    async def admitted(self, session_id: UUID, principal_id: UUID) -> AdmissionRecord | None: ...
    async def admit(self, session_id: UUID, record: AdmissionRecord, *, ttl: timedelta) -> None: ...
    async def generation(self, principal_id: UUID) -> str: ...          # SET NX + read, one script
    async def invalidate_sessions(self, session_ids: Sequence[UUID]) -> None: ...
    async def drop_generation(self, principal_id: UUID) -> None: ...

AuthnKernelConfig(
    session_admission=SessionAdmission(
        route="authn_session_admission",   # the port's deps route: Redis, or the mock
        outbox=OutboxSpec(
            name="authn_session_admission", ...,
            require_transaction=True,
            wake=OutboxWake(route="outbox-wake"),
        ),
        timeout=timedelta(milliseconds=50),
        fence_ttl=timedelta(seconds=10),          # the relay polls well inside it
        fence_tolerance=timedelta(seconds=5),     # oldest undelivered invalidation allowed
    ),
)
```

- **A dedicated port, not the cache contract.** `CacheSpec` is allowed to serve stale or
  absent values on purpose: an in-process L1 layer, probabilistic early expiry, soft deletes.
  An admission store must be read exactly from Redis, and it needs `SET NX` with its read-back
  as one step. A conformance test runs the Redis and mock adapters against the same battery.
- **Startup refuses** a configuration whose outbox route has no `OutboxWake`. Without one, the
  window in §9 silently becomes the relay's poll interval.
- **A circuit breaker** around the port, from forze's resilience executor, stops a Redis
  outage costing every verification its full timeout. A few timeouts open it, and
  verifications go straight to the authoritative path until a probe succeeds.

### 5.6 Alternatives considered

- **A TTL cache of the session row.** Simple, and wrong in exactly the way
  `forbid_cache_and_history` exists to prevent: a revocation takes effect up to a TTL late
  with no bound on how many checks it admits. Rejected.
- **A deny-list of revoked sessions in Redis (tombstones).** Fewer writes, but every way a key
  disappears (eviction, a flush, a restart without persistence, a write that failed) admits
  a revoked session. The admission list fails the other way. Rejected.
- **A per-principal epoch claim in the token** (the request's sketch). Refresh rotation
  invalidates one session and happens on every refresh. Bumping a principal-wide epoch for
  it would log out the principal's other devices, and family revocation needs per-session
  granularity too. Kept for principal-wide events only, as the generation.
- **Carrying the keys on the cache contract.** It is already wired everywhere, but its
  semantics allow a stale or missing read by design (an L1 layer, early expiry, soft
  deletes), and it has no `SET NX`. The admission store's safety argument needs exact reads.
  Rejected in favour of a five-method port.
- **Broadcasting revocations to in-process caches over pub/sub.** At-most-once, with no
  signal that a message was missed (RFC 0068 §5 makes the same argument). Rejected.
- **Writing Redis synchronously in the revocation and failing it when Redis is down.**
  Strict, but it makes logout and password reset fail whenever Redis is unavailable, which is
  the worst moment to refuse a password reset. Rejected in favour of the outbox's delayed but
  guaranteed delivery.

## 6. Tests

- **Property: the fast path never admits what the authoritative path refuses, outside the
  set of invalidations still in the outbox.** It runs over every event in §5.3, interleaved
  with verifications, against the mock and against Redis plus Postgres.
- **The issue race:** a logout committing between a session's commit and its admission write
  leaves the session refused.
- **The eligibility race (phase 2b):** a deactivation committing between a login's or a
  refresh's eligibility check and its session insert either waits for the insert, then revokes
  the new session, or makes the issue fail its check. Never a live session for an inactive
  principal.
- **Every fall-through:** a missing record, a missing generation, a generation mismatch, a
  subject or tenant mismatch, a Redis error, a timeout and a flush. Each takes the
  authoritative path and returns its result and code.
- **Eviction:** with an eviction policy forced on the admission keys, nothing is admitted
  that should not be.
- **Delivery:** with a wake, a logout's next verification is refused within the relay
  latency; without a wake the configuration is refused at startup.
- **The fence:** with the applier failing every attempt until the row parks, with the relay
  stopped, and with Redis reachable by verifiers but not by the relay, a revoked session is
  refused within the tolerance plus the fence TTL. The fast path resumes only after the
  route drains.
- **The breaker:** with Redis unreachable, verifications stop waiting the timeout after the
  breaker opens, and resume the fast path after a probe succeeds.
- **Phase 1:** each revocation path commits atomically. A failure mid-deactivation leaves
  neither the principal inactive with live sessions nor the reverse. The mock's `deactivate`
  revokes sessions as the real adapter does.

## 7. Docs

The authn page gets a "Session check fast path" section:

- what it costs and saves;
- the deployment requirements: a pub/sub wake route, and a Redis whose admission keys are
  never shared with data that may be flushed independently;
- the one window, stated plainly. A revocation is enforced on the fast path once its outbox
  row is relayed, which with a wake is milliseconds after commit. If delivery stalls, the
  fence lapses and the authoritative path refuses within the fence's tolerance plus its TTL,
  until the backlog drains and anything parked is requeued;
- what to alert on: the route's oldest pending age, its `failed` count, and the fence being
  absent while Redis is up.

## 8. Out of scope

- **API-key verification.** Its own verifier and its own revocation path; it can adopt the
  same shape later.
- **OIDC and external IdP tokens.** Their revocation is the IdP's.
- **Cross-region Redis.** The design assumes one Redis per deployment of the identity plane.
- **Caching grants.** Covered by RFC 0068.

## 9. Risks

- **The window.** An invalidation in the outbox is not yet enforced by the fast path. With
  a wake that is the relay latency; hh-backend-v2 measured about 10 ms p50 outbox lag with an
  app-side wake under load. Whatever the failure (a parked row, a stopped relay, Redis
  reachable by verifiers but not by the relay), the fence bounds it to the tolerance plus the
  fence TTL. Accepted, stated in the docs, and observable through the route's oldest pending
  age and `failed` count; depth alone cannot show a single row ageing.
- **The fence's polling.** It costs the idle polling the wake removes: one claim query and two
  admin probes per interval on an otherwise quiet route. Accepted for the bound it buys.
- **Operators reading "fast path" as "cache".** The docs say what it is not.
- **Half the saving until phase 2b.** Phase 2 removes the session-row read and keeps the
  eligibility read. Accepted: the eligibility skip is unsound until its preconditions hold.
- **Login serializes on the policy principal's row.** Phase 2b's locking read makes concurrent
  logins of one principal wait for each other. That is a narrow contention (one principal),
  and the price of eligibility never trailing a deactivation.
- **Phase 1 changes the revocation adapters' transactional shape.** They gain a transaction
  they did not have. Tests pin atomicity, and the change is independently a fix.
- **Generation churn.** A principal-wide event strands every admission record of that
  principal, which then costs one Postgres read per session until each session's next
  refresh re-admits it. That is bounded by the access-token lifetime.

## 10. Unresolved questions

- Whether every writer of `is_active=False` on a policy principal is covered by §5.3. A
  census is part of phase 2b's entry condition.
- How the fence sees a row stuck in `processing`. `OutboxAdminPort.oldest_pending_age`
  measures pending rows, and `depth` counts processing ones without their age. Either the
  admin port gains an oldest-undrained age, or the route's `reclaim_stale_after` is kept
  inside the tolerance so a stranded claim returns to pending first. Phase 2 decides.
- Where the fence refresh hooks into the relay: a post-pass callback on the background relay
  step, or a small lifecycle step of its own that probes the route and writes the fence.
  Phase 2 decides; either way only a process that relays the route writes it.
- Whether backends other than Postgres give the locking read the exclusion phase 2b needs.
  The document port degrades `for_update` modes elsewhere; phase 2b names the backends it
  supports.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Redis holds admissions, never revocations: any missing or unreadable key means "ask Postgres". A deny-list design makes every lost key an admitted revoked session. |
| 2 | `LOCKED` | Readers never write. A verification that misses takes the authoritative path and stores nothing, so no read can race an invalidation into a stale admission. |
| 3 | `LOCKED` | An invalidation is staged in the outbox inside the transaction that changes the rows, and is never best-effort; the route declares `require_transaction=True` so a non-transactional revocation fails loudly. Losing one admits a revoked session until the session expires. |
| 4 | `LOCKED` | An admission write uses the principal's generation read before the session row commits, minted with `SET NX` and read back in one step. Reading it after lets a concurrent principal-wide revocation be undone. |
| 5 | `ASSUMED` | A generation is a random 128-bit value, not a counter, so a generation minted after a flush or a drop matches no surviving record. A principal-scoped invalidation drops it; the next session issued mints a fresh one. |
| 6 | `ASSUMED` | The revocation paths get their transaction from a transaction route the authn kit binds on its operations (`build_authn_registry(tx_route=...)`), and the adapters stage through an outbox port their deps factory builds per request. An adapter-level scope would thread a transaction manager through every adapter and nest awkwardly in an app's own transaction. |
| 7 | `ASSUMED` | The admission keys are carried by a dedicated `SessionAdmissionPort` (Redis and mock adapters, one conformance battery), not the cache contract, whose L1 layer, early expiry and soft deletes allow stale reads and which has no `SET NX`. |
| 8 | `ASSUMED` | Startup refuses the fast path when its outbox route has no `OutboxWake`, since the window would silently become the poll interval. |
| 9 | `LOCKED` | The eligibility read stays until every session-issuing flow, login and refresh, serializes with deactivation (a locking read of the policy principal in the issuing transaction) and every write of `is_active=False` drops the generation. Then an eligibility decorator treats a present generation as eligible; the orchestrator and `VerifiedAssertion` do not change. Skipping it earlier admits a principal deactivated while it logged in. |
| 10 | `ASSUMED` | The fast path is bounded by its own timeout (default 50 ms) behind a circuit breaker from the resilience executor, so a Redis outage costs a few timeouts rather than one per verification. Its errors and timeouts are counted, never raised. |
| 11 | `ASSUMED` | The fast path never refuses; refusal and its error codes stay with the authoritative path. |
| 12 | `LOCKED` | The fast path admits only while a freshness fence exists, and only the relay refreshes it, after a pass that finds the admission route with no `failed` row and nothing pending or processing older than the tolerance. Delivery alone cannot bound the window, because the relay parks a row after its last attempt and nothing drains a parked row on its own. |
| 13 | `ASSUMED` | The fence TTL defaults to 10 s and the tolerance to 5 s, and the admission route's relay polls inside the TTL. The defaults trade idle polling against the worst-case window. |

## 12. Phasing

1. **Transactions on the revocation paths.** Logout, family revocation, refresh rotation,
   password change and reset, and principal deactivation each commit atomically, and the
   mock's `deactivate` revokes sessions. Login's and refresh's eligibility checks and session
   inserts become one transaction with a locking read of the policy principal. Independently a
   fix; ships first.
2. **Admission keys and the session fast path.** The port and its adapters, the outbox route
   and relay, the admission writes on issue, and the verifier fast path behind its breaker.
   Opt-in. The eligibility read stays.
   - **2b. The eligibility skip.** After the census of `is_active=False` writers, each of
     them drops the generation, and the eligibility decorator takes over the read.
3. **Docs and metrics.** The authn page section; fast-path hit, miss and error counters; a
   fence-absent counter; and the admission route's oldest pending age and `failed` count as
   the revocation window.
