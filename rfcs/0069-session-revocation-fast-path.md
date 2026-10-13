# RFC 0069 — Session revocation fast path

- **Status:** 📝 Draft
- **Scope:** An opt-in fast path for verifying an access token's session that needs no Postgres
  read while Redis answers. A verification admits the token from two Redis keys:
  - a per-session *admission* record, written when the session is issued;
  - a per-principal *generation*.

  Anything other than a clean match falls back to today's authoritative reads. Revocation
  invalidates through the transactional outbox, staged in the same transaction as the row
  change, so an invalidation is never lost, only delayed by its delivery. Touches:
  - the `forze_identity` token verifier, the session-issuing and session-revoking adapters,
    and the authn kernel configuration;
  - a small Redis applier fed by the outbox.

  It needs the revocation paths to run inside a transaction, which today none do. The
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

A token verification with the fast path enabled makes one Redis `MGET` and no Postgres read.
It admits the token when three things hold:

- the session's admission record exists;
- the record carries the principal's current generation;
- the record names the token's subject and tenant.

Every other outcome takes today's path: a miss, a mismatch, a Redis error or a timeout. A
session-scoped invalidation (rotation, family revocation) deletes the session's admission
record. A principal-scoped one (logout, password change or reset, deactivation) replaces the
principal's generation, which strands every admission record minted under the old one.
Both are staged in the outbox inside the transaction that changes the rows, and relayed to
Redis. Because Redis only ever holds *admissions*, a lost, evicted or flushed key costs a
Postgres read and never admits a revoked session. The one window the fast path has is an
invalidation still in flight, and its length is the outbox's delivery latency.

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

- No Postgres read on a token verification while Redis answers and nothing about the
  session or principal changed.
- A revoked or rotated session, or an ineligible principal, is refused by the fast path once
  its invalidation has been delivered. Delivery is bounded by the outbox relay; with a wake,
  that is milliseconds.
- Redis being down, slow, flushed, restarted or evicting keys falls back to today's reads.
  None of those can admit a session the authoritative path would refuse.
- Opt-in, and absent by default with no behaviour change.

**Non-goals**

- **Not a cache of the session row.** Readers never write; the row stays the only truth.
- **Not zero-latency revocation under a Redis partition.** An invalidation the relay cannot
  deliver yet leaves the fast path admitting until it lands. §9 states the bound.
- **Not a token format change.** `sub`, `sid` and `tid` are enough.
- **Not a general authz cache.** Grants stay in RFC 0068's territory.

## 5. Design

### 5.1 What Redis holds

Two key kinds, under a dedicated prefix:

```text
authn:admit:g:{principal_id}  ->  <generation>                       (no TTL)
authn:admit:s:{session_id}    ->  <generation>|<principal_id>|<tenant_id or ->
                                   TTL = session.expires_at - now
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
record, generation = await admit.mget(f"s:{sid}", f"g:{sub}")   # one round trip, bounded

if record is not None and generation is not None:
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
- **Eligibility:** when the fast path admits, the orchestrator skips the eligibility read.
  Every write that changes eligibility replaces the generation (§5.3), so an admission under
  the current generation implies the principal was eligible when the record was minted and
  has not changed since. How the orchestrator learns that the verifier admitted fast is
  decision 9.

### 5.3 Who writes

| Event | Redis effect | Delivered by |
| --- | --- | --- |
| A session is issued (login, refresh) | `SET s:{sid}` with the generation **read before the session row commits** | after commit, best effort |
| Refresh rotation | `DEL s:{old_sid}` | outbox |
| Family revocation, reuse detection | `DEL s:{sid}` for each session of the family | outbox |
| Logout, password change or reset, principal deactivation | `SET g:{principal}` to a fresh generation | outbox |
| Any other write of the policy principal's `is_active` | `SET g:{principal}` to a fresh generation | outbox |

**The snapshot is what makes an issue race safe.** Without it, a logout committing between
the new session's commit and its admission write would replace the generation, and the
admission would be written with the new value, admitting a session the logout revoked.
Read before the commit, the admission carries the old generation and simply never matches.
A principal with no generation yet gets one with `SET NX`, then reads it.

An admission write that fails costs that session its fast path (every check misses), so it
does not need the outbox. An invalidation that fails would admit a revoked session, so it
does.

### 5.4 Delivery through the outbox

- **Staging.** Each revoking adapter stages its invalidation in an outbox route dedicated to
  session admission, in the transaction that stamps the rows. The payload is the session ids
  to delete, or the principal whose generation to replace.
- **Relaying.** A relay applies the invalidations to Redis, with the route's `OutboxWake`, so
  delivery follows the commit by milliseconds. Applying one twice is harmless: a `DEL` is
  idempotent, and a second fresh generation strands nothing the first didn't.
- **Precondition: the revocation paths must run in a transaction.** None does today (§3).
  Making them do so is phase 1 and is worth doing on its own: principal deactivation's three
  sequential writes can today leave a principal inactive with live sessions, or the reverse.

### 5.5 Configuration

```python
AuthnKernelConfig(
    session_admission=SessionAdmission(
        cache=CacheSpec(name="authn_session_admission", ...),   # carrier: decision 7
        outbox=OutboxSpec(name="authn_session_admission", ..., wake=OutboxWake(route="outbox-wake")),
        timeout=timedelta(milliseconds=50),
    ),
)
```

Startup refuses a configuration whose outbox route has no `OutboxWake`. Without one, the
window in §9 silently becomes the relay's poll interval.

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
- **Every fall-through:** a missing record, a missing generation, a generation mismatch, a
  subject or tenant mismatch, a Redis error, a timeout and a flush. Each takes the
  authoritative path and returns its result and code.
- **Eviction:** with an eviction policy forced on the admission keys, nothing is admitted
  that should not be.
- **Delivery:** with a wake, a logout's next verification is refused within the relay
  latency; without a wake the configuration is refused at startup.
- **Phase 1:** each revocation path commits atomically. A failure mid-deactivation leaves
  neither the principal inactive with live sessions nor the reverse. The mock's `deactivate`
  revokes sessions as the real adapter does.

## 7. Docs

The authn page gets a "Session check fast path" section:

- what it costs and saves;
- the deployment requirements: a pub/sub wake route, and a Redis whose admission keys are
  never shared with data that may be flushed independently;
- the one window, stated plainly. A revocation is enforced on the fast path once its outbox
  row is relayed, which with a wake is milliseconds after commit and with Redis or the relay
  down is when they recover. Until then the authoritative path, not the fast path, is what
  refuses.

## 8. Out of scope

- **API-key verification.** Its own verifier and its own revocation path; it can adopt the
  same shape later.
- **OIDC and external IdP tokens.** Their revocation is the IdP's.
- **Cross-region Redis.** The design assumes one Redis per deployment of the identity plane.
- **Caching grants.** Covered by RFC 0068.

## 9. Risks

- **The window.** An invalidation in the outbox is not yet enforced by the fast path. With
  a wake that is the relay latency; hh-backend-v2 measured about 10 ms p50 outbox lag with an
  app-side wake under load. If
  the relay is down, or Redis is unreachable by the relay but reachable by verifiers, it
  lasts until they recover. Accepted, stated in the docs, and observable: outbox depth on
  the admission route is the window's size.
- **Operators reading "fast path" as "cache".** The docs say what it is not.
- **Phase 1 changes the revocation adapters' transactional shape.** They gain a transaction
  they did not have. Tests pin atomicity, and the change is independently a fix.
- **Generation churn.** A principal-wide event strands every admission record of that
  principal, which then costs one Postgres read per session until each session's next
  refresh re-admits it. That is bounded by the access-token lifetime.

## 10. Unresolved questions

- How the revocation paths get their transaction: a transaction route bound by the authn
  kit's operations, or a transaction scope inside the adapters. Decision 6.
- Which contract carries the admission keys (§5.5). Decision 7.
- Whether every writer of a policy principal's `is_active` is covered by §5.3. A census is
  part of phase 2.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Redis holds admissions, never revocations: any missing or unreadable key means "ask Postgres". A deny-list design makes every lost key an admitted revoked session. |
| 2 | `LOCKED` | Readers never write. A verification that misses takes the authoritative path and stores nothing, so no read can race an invalidation into a stale admission. |
| 3 | `LOCKED` | An invalidation is staged in the outbox inside the transaction that changes the rows, and is never best-effort. Losing one admits a revoked session until the session expires. |
| 4 | `LOCKED` | An admission write uses the principal's generation read before the session row commits. Reading it after lets a concurrent principal-wide revocation be overwritten. |
| 5 | `ASSUMED` | A generation is a random 128-bit value, not a counter, so a re-created generation after a flush matches no surviving record. |
| 6 | `OPEN` | How the revocation paths run in a transaction (a kit-bound transaction route or an adapter-level scope). Decide in phase 1 by which keeps the adapters usable outside the kit; log the choice. |
| 7 | `OPEN` | The carrier of the admission keys: the cache contract (`get_many`, `set` with TTL, `delete`) or a small dedicated port. Decide by whether the cache contract can express `SET NX` and a bounded `MGET` without bending; log the choice. |
| 8 | `ASSUMED` | Startup refuses the fast path when its outbox route has no `OutboxWake`, since the window would silently become the poll interval. |
| 9 | `OPEN` | How the orchestrator learns the verifier admitted fast, so it can skip the eligibility read (a flag on the verification result or a fast-path eligibility adapter). Decide by what keeps the orchestrator's contract smallest; log the choice. |
| 10 | `ASSUMED` | The fast path is bounded by its own timeout (default 50 ms), and its errors and timeouts are counted, never raised. |
| 11 | `ASSUMED` | The fast path never refuses; refusal and its error codes stay with the authoritative path. |

## 12. Phasing

1. **Transactions on the revocation paths.** Logout, family revocation, refresh rotation,
   password change and reset, and principal deactivation each commit atomically, and the
   mock's `deactivate` revokes sessions. Independently a fix; ships first.
2. **Admission keys and the fast path.** The applier route and relay, the admission writes on
   issue, the verifier fast path, and the eligibility skip. Opt-in.
3. **Docs and metrics.** The authn page section, fast-path hit, miss and error counters, and
   outbox depth on the admission route as the revocation window.
