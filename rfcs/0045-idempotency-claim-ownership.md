# RFC 0045 — Idempotency claim ownership

- **Status:** 📝 Draft
- **Scope:** One missing fence in the idempotency plane: `commit` and `fail` cannot
  tell the caller's own claim from a duplicate's reclaim of the same key, so a late
  operation can complete or release an operation that is not its own. Adds an
  **owner** to the claim, carried by the same wiring mechanism `tenant_provider`
  already uses, and fences both methods on it. Touches
  `contracts/idempotency` (documented guarantee), a new provider field on the four
  stores, and the shared conformance battery. **No port signature change** under the
  locked option; the Postgres table gains one nullable column. Does not change
  what happens when a claim lapses and nobody reclaims it.
- **Related:** `src/forze/application/contracts/idempotency/ports.py`,
  `src/forze/application/hooks/idempotency/plans.py`,
  `src/forze/application/contracts/tenancy/mixins.py` (the provider pattern this
  copies), the four stores under `forze_postgres`, `forze_redis`, `forze_mongo`,
  `forze_mock`, and `tests/support/idempotency_conformance.py`, whose docstring
  currently records this limit as unmakeable.
- **Origin:** Found while adding the Mongo store (PR #401): three reviewers
  independently flagged it, and probing the plane showed all four backends share it.

---

## 1. Summary

A claim gains an **owner** — the `execution_id` of the invocation that took it —
supplied to each store the way the tenant already is, through a provider callable
injected at wiring. `commit` and `fail` then match on it, so an operation completes
or releases only the claim it holds. Nothing about the port's signature changes, and
a store with no owner available behaves exactly as it does today.

## 2. Motivation

`commit(op, key, payload_hash, record)` and `fail(op, key, payload_hash)` identify a
claim by three values that **two duplicates of one request necessarily share**. When
operation A overruns its dedup window and duplicate B reclaims the key, A's late
`commit` matches B's live claim on every predicate the signature permits, and
overwrites it.

This was verified rather than reasoned, across the whole plane:

- **Mongo** (probe, real server): after B reclaimed A's lapsed key with the same
  payload hash, A's `commit` overwrote B's claim and a later duplicate replayed
  A's result while B was still executing.
- **Postgres**: the same shape — its `commit` filter is
  `op AND idem_key AND payload_hash AND status = 'pending'`.
- **Redis**: its compare-and-set fences on the exact serialized claim metadata
  `{st:"P", ph:…}`, which the reclaimer writes **byte-identically**, so it matches too.
- **Mock**: the same, confirmed by the battery check that was written for this and
  failed on all four engines.

An `expires_at`-based fence was tried on the Mongo store and reverted: it fires only
when the claim lapsed and *nobody* reclaimed it — the harmless case, where refusing
costs a co-located store the rollback of work that already succeeded — and does
nothing in the case above, because a reclaimed claim is live.

The failure is silent, and it is the one the plane exists to prevent: two
executions, one cached result, and the record describing whichever operation
committed first rather than the one whose effects survived.

## 3. Current state

Verified against the tree (2026-09-04):

- `IdempotencyPort` has `begin` / `commit` / `fail` plus the
  `commits_in_transaction` property. No method carries a claim identity.
- The hook (`hooks/idempotency/plans.py`) resolves the port **twice** per operation —
  once in `__call__` for the middleware wrap and again in `commit_on_success`'s
  factory — so a store instance cannot carry per-claim state between `begin` and
  `commit`. Any handle has to come from outside the store.
- `InvocationMetadata.execution_id: UUID` already exists on the invocation context
  (`execution/context/invocation.py`) and is unique per invocation. Two duplicates
  of one request are two invocations, so it separates exactly the callers the
  payload hash cannot.
- Every store already receives a wiring-injected callable of this shape:
  `TenancyMixin.tenant_provider`, set by each `Configurable…` factory from
  `ctx.inv_ctx.get_tenant`. The factories have the whole `ExecutionContext` in hand.
- The Mongo store already mints a per-claim `claim_token` and stores it, used to tell
  a fresh insert from a live claim; it is unreachable from `commit`/`fail` today.
- The Postgres table is **application-provided** (its schema is documented in the
  store's docstring), so any new column is a migration for every deployment.
- `tests/support/idempotency_conformance.py` records this limit in prose as
  deliberately unasserted. That paragraph is what this RFC retires.

## 4. Goals / Non-goals

**Goals**

- A late `commit` or `fail` from an operation whose claim was reclaimed is refused,
  on every store.
- A late `commit` whose claim lapsed but was **not** reclaimed still succeeds — the
  work is preserved, which is the behaviour `expires_at` fencing got wrong.
- The guarantee is stated once in the port and proven by one conformance check that
  every engine runs.
- Deployments that do not migrate keep working, with today's semantics.

**Non-goals**

- Changing the dedup-window semantics, or what a lapsed-and-unreclaimed claim does.
  That divergence follows from each store's expiry mechanism and both outcomes are
  safe (see the battery's docstring).
- Making idempotency exactly-once. The plane is at-least-once with a dedup window;
  this closes a hole in the window, not the crash gap `commits_in_transaction`
  addresses.
- A general "claim handle" abstraction for other planes. The dlock plane has its own
  fencing story; nothing here generalizes to it without evidence.

## 5. Design

### 5.1 The owner, and where it comes from

The claim records the `execution_id` of the invocation that took it:

```python
class ClaimOwnerMixin:
    owner_provider: Callable[[], UUID | None] | None = attrs.field(default=None)
    """Identity of the invocation taking a claim — wired like ``tenant_provider``."""

    def claim_owner(self) -> UUID | None:
        """The current invocation's id, or ``None`` outside one (fencing then degrades)."""
```

Each `Configurable…Idempotency` factory sets it beside the tenant provider:

```python
return MongoIdempotencyStore(
    ...,
    tenant_provider=ctx.inv_ctx.get_tenant,
    owner_provider=lambda: (m := ctx.inv_ctx.get_metadata()) and m.execution_id,
)
```

`execution_id` and not `correlation_id`: the FastAPI middleware mints the execution id
itself (`uuid7()` per request, `forze_fastapi/middlewares/invocation.py`), while the
correlation id is read from an advisory client header. An owner a caller can set is an
owner a caller can forge, which would turn the fence into a way to steal a claim rather
than a way to hold one.

`begin` writes the owner into the claim; `commit` and `fail` add it to the predicate
they already use. The lapsed-but-unreclaimed case keeps working precisely because
the owner, not the expiry, is what is checked: the claim is still the caller's.

### 5.2 Per-store shape

| Store | Claim carries | Fence |
|---|---|---|
| Mongo | the existing random `claim_token`, **plus a separate `owner` field** | add `owner` to the `commit`/`fail` filters |
| Redis | `own` in the claim metadata JSON | free — the CAS already compares the metadata byte-for-byte |
| Postgres | a new nullable `owner uuid` column | two query shapes, chosen at wiring — see §5.3 |
| Mock | a field on the stored entry | direct comparison |

**The Mongo token and the owner are different fields on purpose.** `claim_token` is how
`begin` tells its own fresh insert from a live claim it merely read back, so it must be
unique *per call*; an owner is not, and cannot be. Setting the token to the owner breaks
that test twice over: with no provider both sides are `None`, so a live claim reads as a
fresh insert and two callers believe they hold it, and even with a provider an invocation
that calls `begin` twice for one key matches its own earlier claim. One field answers "did
I just create this", the other "is this still mine".

### 5.3 Degrading instead of breaking

Three situations have no owner to fence on, and each must keep working:

1. **The store is wired without a provider.** No owner exists to write or match.
2. **The Postgres table predates the migration**, so there is no `owner` column.
3. **A row was written without an owner** while the column existed.

(1) and (3) are predicate questions; (2) is not — a query naming a column the table
does not have fails to execute, so no amount of conditional SQL text rescues it and a
warning log certainly does not. Postgres therefore needs **capability detection**, not
just a nullable column: the store asks the catalog for the column once at wiring (the
introspection `forze_postgres.kernel.catalog.introspect` already performs for schema
validation) and selects between two statement shapes for the life of the store —

- **legacy** — today's predicate exactly, with no mention of `owner`, used when the
  column is absent;
- **owner-aware** — the same predicate plus `AND (owner IS NULL OR owner = %s)`, used
  when the column exists *and* the provider yielded an owner. The `IS NULL` arm is what
  lets (3) still complete.

When the column exists but the provider yields nothing, the owner predicate is
**omitted**, not bound to `NULL`: `owner = NULL` is `UNKNOWN` in SQL, so binding it
would silently stop matching every owned row — the opposite of degrading to today's
behaviour. That is the trap this paragraph exists to name.

The design's weakness is what remains: a deployment that never migrates never gets the
guarantee. Detection makes that state *knowable* rather than silent — §9 carries the
mitigation.

### Alternatives considered

- **A claim handle in the port signature** — `begin` returns a token, `commit`/`fail`
  take it. Structurally honest: a store cannot silently omit the fence, and there is
  no ambient-context dependency. Rejected as the first step because it is a breaking
  change to a shipped contract, reaching the port, the hook, the encryption wrapper
  and all four stores at once, for a guarantee the provider approach delivers with
  the wiring mechanism the plane already uses. If §10 Q2 finds the conditional fence
  too weak to state as a guarantee, this is what it becomes.
- **Fence on `expires_at`** — tried and reverted on PR #401; the evidence is in §2.
- **Per-store state between `begin` and `commit`** — impossible: the hook resolves
  the port twice, and adapters are frozen.

## 6. Tests

One conformance check, run by all four engines: an operation whose claim was
reclaimed by a duplicate cannot complete or release it, with the reclaim's own
subsequent `begin` as the control that proves the claim is still held. The harness
gains a seam for minting a store with a different owner, alongside the TTL seam it
already has. The battery paragraph declaring this unassertable is deleted in the
same change — an entry that outlives its truth is worse than none.

Per store, Postgres carries three legs, because each is a shipped configuration and
they fail differently: the column absent (legacy statement, no fencing), the column
present with an owner (fenced), and the column present with **no** provider (predicate
omitted — the leg that catches a `NULL` binding, which would match no owned row while
looking like it worked). Mongo adds one: a claim whose `claim_token` and `owner` differ
must still be read as someone else's live claim, which is the misclassification §5.2
describes.

## 7. Docs

The port docstring states the guarantee and its condition. The Postgres store's
documented schema gains the column with a note that it is optional and what is lost
without it. The idempotency docs page gets the operator-facing sentence: an
un-migrated table cannot refuse a reclaimed commit.

## 8. Out of scope

- Reporting *which* invocation owns a contested claim in the error. Useful for
  debugging, but it means reading the row back on the failure path; named, not built.
- Making the owner visible to application code.
- Backfilling `owner` for existing Postgres rows: they expire within the dedup
  window, so the column self-populates. Named because it looks like an omission.

## 9. Risks

- **A guarantee that silently does not hold.** The conditional fence means an
  un-migrated Postgres deployment believes it has ownership fencing and does not.
  Mitigation: the wiring-time capability detection this needs anyway is exactly what
  makes the state observable — the store logs once when the column is absent, and the
  docs state it. The alternative (refusing to start) breaks every existing deployment
  on upgrade.
- **`execution_id` is not stable across a retry that re-enters the same claim.** A
  retried invocation is a new execution and so a new owner, which is correct for
  this purpose but must not be read as a general "request identity".
- **Ambient-context coupling.** A store built outside an invocation has no owner.
  Acceptable because every store is resolved per-invocation by its factory, but it
  is the reason the alternative in §5 stays named rather than dismissed.

## 10. Unresolved questions

- **Q1:** does the mock adapter fence by default, given DST builds it directly rather
  than through a factory? If it degrades silently there, the oracle is weaker than
  the real stores and the battery check would pass for the wrong reason.
- **Q2:** is the conditional fence strong enough to state as a *port guarantee*, or
  should the port say "fenced when an owner is wired"? The honest wording depends on
  whether every shipped factory sets the provider — settled by writing them.
- **Q3:** does the encryption wrapper (`integrations/idempotency/encryption.py`) need
  to forward anything, or does it pass through unchanged? It delegates
  `commits_in_transaction`; the owner may need the same treatment.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The owner is the invocation's `execution_id`, not a store-minted token and not the correlation id. A store-minted value cannot reach `commit`/`fail` (the hook resolves the port twice); the invocation id already separates exactly the two callers the payload hash cannot; and it is server-minted, where the correlation id arrives in a client header and could be forged to steal a claim. |
| 2 | `LOCKED` | The owner arrives through a wiring-injected provider callable, mirroring `tenant_provider` — not through a port signature change. This keeps a shipped contract intact; the signature change stays specified in §5 as the escape hatch if Q2 rules the conditional guarantee too weak to state. |
| 3 | `LOCKED` | Fencing is conditional on an owner being present on both sides, and on Postgres also on the column existing — detected at wiring, selecting between a legacy and an owner-aware statement. An absent owner omits the predicate rather than binding `NULL`, which would match nothing. That is what makes the column additive and keeps un-migrated deployments running. Consequence: the guarantee is deployment-dependent, which §7 and §9 must state plainly rather than paper over. |
| 4 | `ASSUMED` | The lapsed-but-unreclaimed `commit` keeps succeeding. Ownership is the right axis; expiry is not, and PR #401's probe is the evidence. Depart only with a new probe showing harm. |
| 5 | `ASSUMED` | One conformance check is enough to state the guarantee, because the failure mode is single-shaped: a reclaim by a same-payload duplicate. |
| 6 | `OPEN` | Q1 — the mock's default. Whether the oracle fences without a factory decides if the battery check is meaningful there; the executor settles it and logs the choice. |
| 7 | `OPEN` | Q2 — the port's wording, and with it whether decision 2 survives contact. |
| 8 | `OPEN` | Q3 — whether the encryption wrapper forwards the owner. |

## 12. Phasing

- **P1:** contracts (the mixin + the documented guarantee), the mock, and the
  conformance check. The check must fail on the three unmigrated stores at this
  point — that is what proves it tests something.
- **P2:** Mongo and Redis, which need no application migration.
- **P3:** Postgres, with the nullable column, the un-migrated leg, and the wiring log.
