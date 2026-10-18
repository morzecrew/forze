# RFC 0069 — Session revocation fast path

- **Status:** ✅ Complete — phase 1 shipped in 0.13.0 (2026-10-14) and phase 2 in 0.14.0;
  phases 2b, 2c and 5 followed, with the epoch and session states signed under `key_mac`
  (row 29). Phase 2 was redesigned on 2026-10-14 from hh-backend-v2's specification: §13
  replaces §5.1–5.5, and holds wherever §1–§12 conflict with it for phases 2 onward. Rows 14–29
  replace the decisions they name.
- **Scope:** *(As first proposed; §13 redesigns phase 2 onward.)* An opt-in fast path for verifying an access token's session that replaces the
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

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

A token verification with the fast path enabled replaces the session-row read with one Redis
`MGET`. It admits the token when four things hold:

- the session's admission record exists;
- the record carries the principal's current generation;
- the record, found under the token's subject and session, names the token's tenant;
- the freshness fence is present.

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
invalidation turns the fast path off within the fence's tolerance plus its TTL (§5.4).

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

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

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

*§5.1–5.5 are the design as first proposed. Phase 2 follows §13 instead.*

### 5.1 What Redis holds

Two key kinds and one fence, under a dedicated prefix:

```text
authn:admit:{<principal_id>}:g         ->  <generation>                    (no TTL)
authn:admit:{<principal_id>}:s:<sid>   ->  <generation>|<tenant_id or ->   TTL = session.expires_at - now
authn:admit:fresh                      ->  1                               TTL = fence TTL (§5.4)
```

The record is keyed by the token's subject as well as its session. A token whose `sub` is
not the session's principal therefore finds no record, and the authoritative path refuses it
as a subject mismatch.

- **A generation is random (128 bits), not a counter.** After a flush, a re-created
  generation is a value no admission record holds, never a reused one, so a flush can only
  cause misses.
- **An admission record says "this session was admitted under this generation".** It is
  never a statement that a session is revoked. That is the asymmetry the design rests on:
  any key that is missing reads as "ask Postgres".
- **Redis Cluster.** `forze_redis`'s client is not cluster-aware today, so the target is a
  single Redis (with or without Sentinel), where the three keys are one `MGET`. The braces are
  a hash tag: a principal's record and generation share a slot. A cluster-aware adapter can
  therefore read them with one `MGET` and the fence alongside it in the same pipeline, rather
  than meeting `CROSSSLOT`.

### 5.2 The fast path

```python
# inside _require_active_session, when the fast path is configured
record, generation, fresh = await admit.read(sub, sid)   # one MGET on a single node
parsed = parse_record(record)                            # None unless exactly <generation>|<tenant>

if fresh is not None and parsed is not None and generation is not None:
    gen, tenant = parsed
    if gen == generation and tenant == (tid or "-"):
        return                                           # admitted, no read

# otherwise, the authoritative path exactly as today
```

- **Failure semantics:**
  - A Redis error, or the bounded timeout expiring, is logged and counted, then falls through
    to the authoritative path. It is never raised.
  - A value that does not parse as a record is a miss, never an error.
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
| A session is issued (login, refresh) | the admission script, **before the session row commits** | deadline-bound, best effort |
| Refresh rotation | `DEL {principal}:s:{old_sid}` | outbox |
| Family revocation, reuse detection | `DEL {principal}:s:{sid}` for each session of the family | outbox |
| Logout, password change or reset, principal deactivation | `DEL {principal}:g` | outbox |
| Any other write of `is_active=False` on the policy principal | `DEL {principal}:g` | outbox |

**The admission is written before the session row commits.** A write after the commit
loses two races:
- a logout committing between the session's commit and the write drops the generation, the
  write mints a fresh one, and the record admits a session the logout revoked;
- a rotation or family revocation committing in that gap has its `DEL` applied first, and the
  late write recreates the record under a generation that is still current.

Nothing can revoke a session before it commits, so any invalidation of it lands after an
admission written earlier. That holds as long as no write executes late, which a write the
issuer gave up waiting for could do. The admission is therefore two script calls:
1. Mint the generation with `SET NX` if absent, and return it with Redis's own `TIME`.
2. Write the record only while Redis's clock is before that time plus the issuer's timeout.

The issuer commits once step 2 answered or its timeout has elapsed, counted from step 1's
answer. By then Redis's clock has passed the deadline too, so a late execution of step 2 finds
it passed and writes nothing. A transaction that rolls back leaves a record
for a session id no token carries, which expires with its TTL.

An admission write that fails costs that session its fast path (every check misses), so it
does not need the outbox. An invalidation that fails would admit a revoked session, so it
does.

### 5.4 Delivery through the outbox

- **Staging.** Each revoking adapter stages its invalidation in an outbox route dedicated to
  session admission, in the transaction that stamps the rows. The payload always names the
  principal, because every admission key sits under the principal's hash tag. It is either
  that principal and the session ids to delete, or the principal whose generation to drop.
  Every revoking path already holds the principal:
  - rotation from the old session row;
  - family revocation and reuse detection from their `principal_id` argument;
  - `revoke_sessions_matching` from the rows it stamps.

  The adapters get the route's outbox command port from their deps factory, which already
  builds them per request.
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
  for its `failed` count and its oldest undrained age: pending or processing, a processing row
  measured from its claim. The port gains that probe in phase 2, because `oldest_pending_age`
  does not see a claim stranded in `processing`. It refreshes `fresh` (`SET ... EX`, the fence TTL)
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
    async def read(self, principal_id: UUID, session_id: UUID) -> AdmissionRead: ...  # record, generation, fence
    async def generation(self, principal_id: UUID) -> tuple[str, datetime]: ...        # SET NX + TIME, one script
    async def admit(
        self, principal_id: UUID, session_id: UUID, record: AdmissionRecord,
        *, ttl: timedelta, not_after: datetime,                                         # Redis time, from generation()
    ) -> None: ...
    async def invalidate_sessions(self, principal_id: UUID, session_ids: Sequence[UUID]) -> None: ...
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

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

- **Property: the fast path never admits what the authoritative path refuses, outside the
  set of invalidations still in the outbox.** It runs over every event in §5.3, interleaved
  with verifications, against the mock and against Redis plus Postgres.
- **The issue races:** a logout committing while a session is issued leaves the session
  refused. An admission write held past its deadline and executed after the session's own
  invalidation writes nothing.
- **Malformed records:** a value of the wrong shape under an admission key is a miss.
- **Every invalidation reaches its key:** for each revoking path, the staged payload names
  the principal, and once applied the session's record or the principal's generation is gone.
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

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

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

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

- Whether every writer of `is_active=False` on a policy principal is covered by §5.3. A
  census is part of phase 2b's entry condition.
- Where the fence refresh hooks into the relay: a post-pass callback on the background relay
  step, or a small lifecycle step of its own that probes the route and writes the fence.
  Phase 2 decides; either way only a process that relays the route writes it.
- Whether backends other than Postgres give the locking read the exclusion phase 2b needs.
  The document port degrades `for_update` modes elsewhere; phase 2b names the backends it
  supports.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | ~~Redis holds admissions, never revocations: any missing or unreadable key means "ask Postgres". A deny-list design makes every lost key an admitted revoked session.~~ — **superseded by row 14**: every state Redis holds now expires, and Postgres answers every miss |
| 2 | `LOCKED` | ~~Readers never write. A verification that misses takes the authoritative path and stores nothing, so no read can race an invalidation into a stale admission.~~ — **superseded by row 15**: a lookup writes, but never overwrites and never outlives its read |
| 3 | `LOCKED` | ~~An invalidation is staged in the outbox inside the transaction that changes the rows, and is never best-effort; the route declares `require_transaction=True` so a non-transactional revocation fails loudly. Losing one admits a revoked session until the session expires.~~ — **superseded by row 16**: revocations write through after commit, and `ttl` bounds a lost write |
| 4 | `LOCKED` | ~~An admission is written before the session row commits. Step 1 mints or reads the generation with Redis's time; step 2 writes the record only before a deadline set from that time. The issuer commits once step 2 answered or the deadline passed. Written after the commit, a delayed write could land after the session's own invalidation, or a principal-wide one, and admit a revoked session.~~ — **superseded by row 17**: nothing is written when a session is issued |
| 5 | `ASSUMED` | ~~A generation is a random 128-bit value, not a counter, so a generation minted after a flush or a drop matches no surviving record. A principal-scoped invalidation drops it; the next session issued mints a fresh one.~~ — **superseded by row 18**: the epoch is a counter held in Postgres |
| 6 | `ASSUMED` | ~~The revocation paths get their transaction from a transaction route the authn kit binds on its operations (`build_authn_registry(tx_route=...)`), and the adapters stage through an outbox port their deps factory builds per request. An adapter-level scope would thread a transaction manager through every adapter and nest awkwardly in an app's own transaction.~~ — **superseded in part by row 16**: the transaction route shipped in phase 1; the outbox staging half is gone |
| 7 | `ASSUMED` | The admission keys are carried by a dedicated `SessionAdmissionPort` (Redis and mock adapters, one conformance battery), not the cache contract, whose L1 layer, early expiry and soft deletes allow stale reads and which has no `SET NX`. |
| 8 | `ASSUMED` | ~~Startup refuses the fast path when its outbox route has no `OutboxWake`, since the window would silently become the poll interval.~~ — **superseded by row 16**: there is no outbox route |
| 9 | `LOCKED` | ~~The eligibility read stays until every session-issuing flow, login and refresh, serializes with deactivation (a locking read of the policy principal in the issuing transaction) and every write of `is_active=False` drops the generation. Then an eligibility decorator treats a present generation as eligible; the orchestrator and `VerifiedAssertion` do not change. Skipping it earlier admits a principal deactivated while it logged in.~~ — **superseded by row 25**: the same preconditions, with the epoch in place of the generation |
| 10 | `ASSUMED` | The fast path is bounded by its own timeout (default 50 ms) behind a circuit breaker from the resilience executor, so a Redis outage costs a few timeouts rather than one per verification. Its errors and timeouts are counted, never raised. |
| 11 | `ASSUMED` | ~~The fast path never refuses; refusal and its error codes stay with the authoritative path.~~ — **superseded by row 22**: the fast path refuses from states that only move one way |
| 12 | `LOCKED` | ~~The fast path admits only while a freshness fence exists, and only the relay refreshes it, after a pass that finds the admission route with no `failed` row and nothing pending or processing older than the tolerance. Processing age comes from an admin-port probe phase 2 adds, since `oldest_pending_age` misses a stranded claim. Delivery alone cannot bound the window, because the relay parks a row after its last attempt and nothing drains a parked row on its own.~~ — **superseded by row 16**: with an expiry on every state, `ttl` bounds the window without a fence |
| 13 | `ASSUMED` | ~~The fence TTL defaults to 10 s and the tolerance to 5 s, and the admission route's relay polls inside the TTL. The defaults trade idle polling against the worst-case window.~~ — **superseded by row 16**: there is no fence |
| 14 | `LOCKED` | Postgres is the only truth. Redis caches states a lookup read there, each with an expiry, and any missing or unreadable key means "read Postgres". |
| 15 | `LOCKED` | A lookup writes what it read with `SET NX PXAT`, at Redis's time when its read script ran plus `ttl`, and writes nothing inside a transaction. A revocation's write overwrites it: `SET` for `revoked`, and for the epoch a max-set of the value it reads from Postgres at write time, expiring at Redis's time before that read plus `ttl`. It never writes the value it raised to, which a newer raise may already have made stale. Together these expire every stale state within `ttl` of the revocation's commit, in Redis's clock (§13.3). |
| 16 | `LOCKED` | A revocation writes its states through after commit (`run_or_defer`), retried `write_attempts` times. If every attempt fails it logs at error level, and the operation still succeeds. A revoked credential can be admitted for at most `ttl` after its commit. That happens only when the write never lands (failed, the process died before the drain, or the process lacked the configuration), or when the revocation's key is lost while an earlier lookup is still writing (§13.5). No outbox route, wake or fence. |
| 17 | `ASSUMED` | Nothing is written to Redis when a session is issued; lookups populate. |
| 18 | `ASSUMED` | The principal epoch is an integer in Postgres, raised in the transaction of every revocation that ends all of a principal's sessions, and carried as the `pep` claim. A token whose `pep` is behind it is refused, and one ahead of a cached epoch is a miss. The epoch's row is never deleted while a token might carry it. |
| 19 | `OPEN` | Where the epoch lives and how each backend raises it atomically. The lean is a forze-owned epochs relation, which leaves the principal model alone. |
| 20 | `ASSUMED` | Access tokens gain `fam` and `pep` only when the fast path is configured. A token without them takes the authoritative path until it lapses. |
| 21 | `ASSUMED` | `session_binding` is `"session"` by default (a rotated session's tokens are refused, as today) or `"family"` (admitted while the family is live). On a Redis error, the verifier makes the same checks from Postgres through the same lookup routine. An outage therefore never admits what the fast path would refuse, and within the window it can refuse sooner. |
| 22 | `ASSUMED` | The fast path refuses with today's codes, because `revoked` and the epoch only move one way. States are keyed under the token's subject and carry the tenant, compared only when both sides name one. |
| 23 | `ASSUMED` | The verifier and every revoking adapter read one configuration value, so one process cannot run readers without writers. The fast path is refused when either registry's `tx_route` is `None`: the authn write-through would run inline and fail the operation, and a tenant switch would read the epoch without its lock. |
| 24 | `ASSUMED` | `ttl` is required and has no default: it is the window, and the operator picks it. |
| 25 | `LOCKED` | The eligibility read stays until every write of `is_active=False` raises the epoch, enforced at the policy principal's write path rather than by a census. Then a decorator skips a read only for a non-locking call about the admitted token's own subject, in the request that admitted it; locking calls (issue) and actor checks always read. Postgres only. |
| 26 | `ASSUMED` | API-key states (phase 2c) are keyed by digest and carry a MAC under a server key, over the prefix, the digest, the principal, the actor, the key's expiry, the state and an absolute not-after. The not-after is the earlier of Redis's read time plus `ttl` and the key's expiry. The verifier refuses a state past its not-after by Redis's `TIME` from the same read script, so one clock sets and checks it. It also checks the key's expiry as today, and treats a state whose MAC fails as a miss. Keys keep the eligibility read. |
| 27 | `LOCKED` | Every issue reads the epoch with `FOR SHARE` after upserting its row, and every raise is an atomic upsert (`INSERT … ON CONFLICT DO UPDATE`) as its first statement, which locks the row or claims its key. So a principal-wide revocation and an issue serialize on Postgres, even when no row existed, and a login, refresh or tenant switch cannot commit a session a concurrent logout misses. |
| 28 | `ASSUMED` | A revocation of chosen sessions or a family writes the state of each session it stamped and of its family, whatever the binding, so a family revocation refuses session-bound tokens and a change of binding is safe. A revocation that ends every session of a principal raises the epoch instead. |
| 29 | `LOCKED` | With `key_mac`, a principal's epoch and every live family or session state carry a not-after and a MAC under the API-key states' keys, over a label of their own, the states route, the principal, the state key, the value and the not-after; `revoked` carries none. The not-after is the store's read time plus `ttl`, checked as in row 26, and a state that fails either check is a miss. The eligibility skip requires it, and the authz kernel refuses `revocation_states` without `key_mac`: unsigned, an older epoch restored in Redis admits a deactivated principal's tokens until their `exp`. |

Rows 14–28 were added on 2026-10-14, when hh-backend-v2's specification for request 4 was
adopted for phase 2 (§13), and revised the same day after an adversarial review of §13. Row 29
was added on 2026-10-18, after phases 2b and 2c shipped.

## 12. Phasing

*As first proposed. For phase 2 onward, §13 holds where the two conflict.*

1. **Transactions on the revocation paths.** Logout, family revocation, refresh rotation,
   password change and reset, and principal deactivation each commit atomically, and the
   mock's `deactivate` revokes sessions. Login's and refresh's eligibility checks and session
   inserts become one transaction with a locking read of the policy principal. Independently a
   fix; ships first.
2. **Admission keys and the session fast path.** The port and its adapters, the outbox route
   and relay, the admin port's oldest-undrained probe, the fence, the admission writes on
   issue, and the verifier fast path behind its breaker.
   Opt-in. The eligibility read stays.
   - **2b. The eligibility skip.** After the census of `is_active=False` writers, each of
     them drops the generation, and the eligibility decorator takes over the read.
3. **Docs and metrics.** The authn page section; fast-path hit, miss and error counters; a
   fence-absent counter; and the admission route's oldest pending age and `failed` count as
   the revocation window.

## 13. Phase 2 redesign (2026-10-14)

hh-backend-v2 moved to 0.13.0 and sent a specification for request 4 that departs from §5. Its
design was adopted for phase 2. This section is that design, with the corrections a review of
the specification against the code produced, and those an adversarial review of this section
added. Where §1–§12 conflict with this section for phases 2 onward, this section holds;
rows 14–28 record which decisions changed and why.

### 13.1 Why §5 was revisited

§5 bought a short window (the fence's tolerance plus its TTL, 15 s by default) with a lot of
machinery:
- a dedicated outbox route, its relay and its wake;
- a freshness fence, refreshed only by that relay, and an admin probe to drive it;
- a two-step admission script that has to run before the session commits.

It also left out two things the application needs:
- API keys, which are verified from their digest row on every request;
- a token whose session a refresh in another tab rotated. The application catches the
  verifier's refusal and reads again to ask whether the family is still live.

The specification gets both with less machinery. Every state Redis holds expires, Postgres
answers every miss, and a revocation writes its states through after commit. Its window is
`ttl`, longer than §5's.

This is, in effect, the TTL cache §5.6 rejected first, with two additions that bound it: the
write-through, and the write rules in 13.3. Without them a revocation takes effect a TTL late
on every check. With them it takes effect at once unless its write fails to land, and even
then within `ttl`.

§5.6 rejected three other ideas this design now uses, each for a reason that no longer
applies:
- **Tombstones**, because a lost key admitted a revoked session. Here a lost key is a Postgres
  read, and a stale one expires within `ttl` (13.3).
- **An epoch claim**, because every refresh would have raised it. Here only revocations that
  end every session raise it, and a rotation is the family's or the session's state.
- **A synchronous write**, because it failed a logout while Redis was down. Here a failed
  write is logged, and `ttl` bounds what it costs.

§5's timeout and circuit breaker (row 10) stay.

### 13.2 What Redis holds

Postgres stays the only truth. Redis caches what a lookup read there, under a prefix and the
principal's hash tag:

```text
{<principal_id>}:e            ->  <epoch>[|<not-after>|<mac>]
{<principal_id>}:f:<family>   ->  live|<tenant or ->[|<not-after>|<mac>] | revoked
{<principal_id>}:s:<sid>      ->  live|<tenant or ->[|<not-after>|<mac>] | revoked
k:<digest>                    ->  live|<principal>|<actor or ->|<expires or ->|<not-after>|<mac> | revoked   (phase 2c)
```

- **The principal epoch** is an integer in Postgres. Where it lives is still open (row 19).
  - It is raised in the transaction of every revocation that ends all of a principal's
    sessions: logout, a password change or reset, and deactivation. Every access token issued
    after carries it as the `pep` claim.
  - Unlike §5's generation, it can be a counter, because a flushed Redis re-reads it from
    Postgres rather than minting a new one.
  - Its row is never deleted while a token might carry it, since a missing row reads as 0.
    Principal erasure keeps the row, which holds nothing but the id.
- **A family or session state** says whether the family has a session, or the session itself
  is, neither revoked nor rotated.
  - A live state carries the tenant, so the tenant check stays. As today, it compares only
    when both the token and the state name a tenant.
  - The key sits under the token's subject, so a token whose `sub` is not the session's
    principal never finds a state. Its lookup refuses it as `session_subject_mismatch` and
    writes nothing.
- **The states move one way.** A revoked family or session never revives, and an epoch never
  falls. `revoke_sessions_matching` stamps only live sessions, so a family is live exactly
  while its newest session is. A cached `revoked`, or a cached epoch, is never wrong. Only a
  cached `live`, or an epoch older than the current one, can be stale, and 13.5 bounds how
  long.

### 13.3 The write rules

- **A lookup never overwrites, and never outlives its read.**
  - A verification's `MGET` runs in a script that also returns Redis's `TIME`.
  - On a miss, the lookup reads Postgres, then writes what it read with `SET NX PXAT`, at
    that time plus `ttl`. The expiry is fixed in Redis's clock before the read began, so a
    write that lands late lands already expired or close to it, never longer.
  - A lookup that runs inside a transaction (`tx_ctx.depth() > 0`) writes nothing. A
    snapshot transaction older than a revocation, or the revoking transaction itself, can
    read a state that is already false.
  - So anything a lookup writes expires within `ttl` of a moment before its read. A lookup
    that read before a revocation committed therefore writes something that expires within
    `ttl` of that commit. `SET NX` means it can never replace a revocation's value that is
    still present.
- **A revocation always wins.**
  - It writes `revoked` with a plain `SET` and `PX ttl`. `revoked` is never stale, so its
    expiry needs no precision. Once the key expires, the next lookup reads `revoked` from
    Postgres again.
  - **It writes the epoch the way a lookup does, but overwriting.**
    1. It takes Redis's `TIME`.
    2. It reads the current epoch from Postgres.
    3. It sets the larger of the stored and the read value, expiring at that time plus `ttl`.

    It never writes the value its own transaction raised to. An epoch is stale once a newer
    raise commits, so a write-through delayed past that raise, with its key already expired
    or lost, would otherwise bring the older epoch back with a fresh expiry. Reading at write
    time means a delayed write sets the newer epoch, or a value that expires within `ttl` of
    the newer commit. The max means two racing writes never move the epoch backwards.

### 13.4 Verification

With the fast path configured, `_require_active_session`:

1. Takes the authoritative path for a token without `fam` and `pep`, one issued before the
   fast path, until it lapses.
2. Runs the read script: the principal's epoch, plus the family's state under family binding
   or the session's state under session binding. The call keeps §5's timeout and circuit
   breaker (row 10).
3. Reads a missing state from Postgres and writes it under 13.3.
4. Refuses with today's codes:
   - `pep` behind the epoch, or a `revoked` state, is `session_revoked`;
   - a tenant mismatch is `session_tenant_mismatch`.
5. Treats a `pep` ahead of the cached epoch as a miss: a lookup that raced the raise wrote
   the older value, so it reads Postgres again.
6. Admits otherwise.

**On a Redis error or timeout** the verifier makes the same checks from Postgres, through the
same lookup routine the miss path uses, without writing. That is the epoch plus the family's
or the session's state. A Redis outage therefore never admits what the fast path would
refuse. Within the window it can refuse sooner, because it reads the truth rather than a
stale state. It costs one read more than today's session-row read.

**Session binding** (`session_binding`) chooses what a rotation does to the old session's
access tokens:
- `"session"`, the default: refused, as today.
- `"family"`: admitted while the family is live, until the token's own `exp`. Reuse
  detection, a logout or a family revocation still refuses the whole family.

### 13.5 Who writes, and the window

A revocation of chosen sessions or a family writes both the family's and the session's
state for each session it stamped, whatever the binding. A family revocation must refuse a
session-bound token whose session it stamped, and writing both also keeps a change of binding
during a rolling deploy safe. A revocation that ends every session of a principal raises the
epoch instead. That refuses every token issued before it, so writing a state per stamped
session would add nothing but writes.

| Path | In the transaction | After commit |
| --- | --- | --- |
| Logout, password change or reset, deactivation | raise the epoch first, then the rows as today | the epoch, by the max script |
| Family revocation, reuse detection, any other `revoked_at` write | rows as today | `revoked` for each stamped session and its family |
| Refresh rotation | rows as today | `revoked` for the old session |
| Authz `deactivate_principal` and every other `is_active=False` write (phase 2b) | raise the epoch | the epoch |
| API-key revocation and credential deactivation (phase 2c) | rows as today | `revoked` for each key |

**Issue serializes with the epoch.**
- Every issue reads the epoch with `FOR SHARE`, upserting its row if absent, in its
  transaction. That covers login, refresh and the tenant switch.
- Every raise is an atomic upsert of the epoch row as its first statement (`INSERT … ON
  CONFLICT DO UPDATE SET epoch = epoch + 1`). That locks the row, or claims its unique key
  when the row is absent, which a plain `UPDATE` of an absent row would not. An issue's
  upsert-then-`FOR SHARE` then waits for it, whichever creates the row.
- So a principal-wide revocation and an issue cannot interleave on Postgres. If the issue
  commits first, the revocation's scan finds its session and revokes it. If the revocation
  commits first, the issue reads the raised epoch.
- This also closes the login-against-logout race that phase 1 left open. On backends whose
  `for_update` takes no lock (Mongo), it does not hold, so the fast path names Postgres as
  its supported backend.

**The write-through.**
- It runs after commit, through `run_or_defer`, as the grants cache's invalidation does. The
  deps factory hands each revoking adapter a deferred writer, as phase 1 handed eligibility
  its transaction probe.
- It retries up to `write_attempts` times, each bounded by the port's timeout.
- When every attempt fails, the operation still succeeds, since the revocation is in
  Postgres. The failure is logged at error level with the principal, family, session or key,
  and counted.
- The fast path requires a transaction route on both registries, and the build refuses it
  otherwise.
  - With `build_authn_registry(tx_route=None)`, `run_or_defer` runs the write inline and
    would propagate its failure.
  - With `build_tenancy_registry(tx_route=None)`, a tenant switch would issue without a
    transaction, read the epoch without its lock, and could commit a session after a
    logout's scan.

**The window.** A revoked credential can be admitted for at most `ttl` after its revocation
commits, measured in Redis's clock. Any stale state was written by a lookup that read
Postgres before the commit, so under 13.3 it expires within `ttl` of the commit. Two kinds of
cause leave one in place:
- **A write-through that never lands:**
  - every attempt failed, which is logged;
  - the process died between the commit and the after-commit drain, which nothing logs;
  - the revoking process lacked the configuration, for example during a rolling deploy or
    from another service, which nothing logs either.
- **A revocation's key lost while a lookup that read before the commit is still writing:**
  eviction, a flush, a restart, or a failover that drops an acknowledged write. Nothing logs
  these.

An access token is also bounded by its own `exp`, whichever comes first. An API key has no
`exp`, so its window is `ttl` alone. `ttl` is the knob, and the docs give every cause.

### 13.6 Eligibility (phase 2b)

The orchestrator's eligibility read stays in phase 2, so phase 2 removes the session-row read
and keeps one policy-principal read per verification. It can go once every write of
`is_active=False` raises the epoch in its transaction.

That is enforced where the write happens, not by a census. The adapter that writes the
policy principal raises the epoch whenever `is_active` turns false. Applications write it
through the generic document port, and a census cannot see that. The authz plane then needs
the revocation configuration and port, and phase 2b wires them.

Deactivation and issue already serialize on the policy principal's row through phase 1's
locking read. So a fast-path admission with `pep` equal to the current epoch implies the
principal was eligible when the token was issued, and nothing has deactivated it since,
within the window.

An eligibility decorator may then skip a read only when all of these hold:
- the call is non-locking (`for_update=False`);
- it asks about the admitted token's own subject;
- the admission was made in this request;
- the epoch it was judged against was signed under `key_mac` (row 29), or read from Postgres.

A locking call is an issue, and the tenant switch issues inside a token-authenticated
request, so it always reads. An actor check never inherits the subject's admission. The
supported backends are Postgres's, as in 13.5.

### 13.7 API keys (phase 2c)

A key's state is keyed by its digest. The verifier already computes the digest before its
read, so a hit needs no key-row read.

Unlike a session's state, a key's state is the only proof the key exists: the verifier takes
the principal, the actor and the expiry from it. Anyone able to write to Redis could
otherwise mint a key, or copy a victim's state onto their own key's digest, or restore a
`live` state over `revoked` with no expiry. So:
- **The state carries a MAC under a server key.** The MAC covers the prefix, the digest, the
  principal, the actor, the key's expiry, the state and an absolute not-after.
- **The not-after** is the earlier of two times: Redis's `TIME` when the lookup's read script
  ran plus `ttl`, and the key's own expiry. A replayed state therefore expires no later than
  any other state would.
- **The verifier checks two things, whatever Redis's expiry says:**
  - it refuses a state past its not-after, judged against Redis's `TIME` from the same read
    script that fetched the state;
  - it checks the key's own expiry, as it does today.

  The not-after is set and checked in Redis's clock, so the bound has no skew term. Someone
  who can write Redis's keys cannot move its clock.
- **A state whose MAC fails is a miss.**
- **The MAC key and its rotation** belong to the configuration.

Keys keep the eligibility read: they carry no `pep`, and the authz `deactivate_principal`
touches no key. The key verifier emits no tenant, and the state carries none.

### 13.8 Configuration

```python
AuthnKernelConfig(
    revocation_states=RevocationStates(
        route="authn-revocations",            # the state port's deps route: Redis, or the mock
        ttl=timedelta(minutes=5),             # required: the window in 13.5
        write_attempts=3,
        timeout=timedelta(milliseconds=50),
        session_binding="family",             # default "session"
        key_mac=...,                          # phase 2c: the MAC keyring
    ),
)
```

The verifier and every revoking adapter read this one value.
- The specification passed it twice, to the registry and to the verifier. That allows readers
  without writers, and then every revocation becomes one `ttl` late with nothing logged.
- A process can still revoke without it, which is a cause in 13.5. The docs say to configure
  every process that revokes.
- The build refuses it alongside `tx_route=None` on either registry (13.5).

The state port stays a dedicated port with Redis and mock adapters and one conformance
battery (row 7), because it needs `SET NX PXAT`, the max script, `TIME` and exact reads.

### 13.9 Tests

hh-backend-v2's acceptance list, plus what the corrections need:
- **Refused with no Postgres read** (counted on the port): a revoked family, a raised epoch
  and a revoked key. A token issued after a password change is admitted.
- **Bindings.**
  - Under family binding, a token from a refresh elsewhere is admitted with no read, and
    refused after logout.
  - Under session binding it is refused, and a family revocation refuses the token of every
    session it stamped.
- **Flush.** The next verification reads once and repopulates, and nothing revoked is
  admitted.
- **Redis down.** No verification admits what the fast path with Redis up would refuse, under
  both bindings, and outside any window both agree. Under session binding that is also what
  0.13 returns.
- **Races.**
  - A lookup and a revocation interleaved in both orders: the revocation's state stands.
  - A lookup write delayed past `ttl` lands expired.
  - A lookup inside a transaction writes nothing.
  - A lookup racing an evicted revocation key leaves a state that expires within `ttl` of the
    commit.
- **Issue against a logout, in both commit orders,** for a login, a refresh and a tenant
  switch, on both paths:
  - an issue that commits first is revoked by the logout;
  - an issue that commits after the logout carries the raised epoch and stays admitted.

  Both orders also hold when the epoch row did not exist before.
- **A delayed epoch write-through, in both orders** against a newer raise:
  - held before its Postgres read: it reads and writes the newer epoch;
  - held after its read, with the read before the newer commit: it writes the older epoch,
    whose expiry was fixed before that read. The older epoch expires within `ttl` of the
    newer commit, or has already expired when it lands.

  In both orders, a token carrying the older `pep` is refused from `ttl` after the newer
  commit at the latest.
- **Write-through failure.** A write-through that fails every attempt: the operation succeeds
  and logs, and the credential is refused within `ttl`.
- **2b.** A locking eligibility call, and an actor check, always read.
- **2c.**
  - A key state with a bad MAC, one copied from another digest, and one past its not-after
    are all misses.
  - A state whose not-after outlives the key's own expiry is refused once the key expires.
  - A deactivated principal's key is refused through the eligibility read.
- **Configuration.** Readers cannot be enabled without writers, and `tx_route=None` on either
  registry is refused.
- **Load.** 1,000 verifications a second for a minute, with no session-store reads after
  warm-up.

### 13.10 Phasing, revised

1. Shipped in 0.13.0.
2. **Revocation states and the session fast path:**
   - the epoch, its store and its locking with issue;
   - the `fam` and `pep` claims;
   - the state port with Redis and mock adapters;
   - the verifier's fast path and its Postgres twin under both bindings;
   - the write-through on every session writer.

   Opt-in, and the eligibility read stays.
3. **2b, the eligibility skip:**
   - the epoch raised at the policy principal's write path;
   - the authz wiring;
   - the per-call decorator.
4. **2c, API keys:** digest-keyed states carrying a MAC.
5. **Docs and metrics:** the window and every cause, plus hit, miss, write-through failure and
   fallback counters.
6. **Signed principal states** (row 29): the epoch and the live family and session states
   carry a MAC and a not-after under `key_mac`, which the eligibility skip requires.
