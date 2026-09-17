# RFC 0063 — Per-owner write serialization and the overlap invariant

- **Status:** 📝 Draft — second in the batch's priority order, **gated on [RFC 0052](0052-versioned-facts-correction-lineage.md) P2** for the storage-guarantee primitive.
- **Scope:** `serialize_by=` on `DocumentSpec`: every write to that aggregate for one owner is serialized by a transaction-scoped advisory lock the adapter takes, the mock simulates it, and DST gains an overlap assertion over the intervals a workload wrote. Touches `DocumentSpec`, the Postgres write path, the mock adapter and `forze_dst`; the hard-guarantee half rides [RFC 0052](0052-versioned-facts-correction-lineage.md)'s `guarantees` vocabulary. No port signature change.
- **Related:** [`src/forze_postgres/adapters/tenant_provisioner.py:328-345`](../src/forze_postgres/adapters/tenant_provisioner.py) (`pg_advisory_xact_lock` already used, once, privately) and `:511-525` (`_advisory_key` — a blake2b digest rather than `hash`, with the salted-hash failure written down), [`src/forze_kits/scopes/dlock.py:29`](../src/forze_kits/scopes/dlock.py) (`DistributedLockScope`, the cross-process lock this is **not**), [`src/forze_dst/oracle/invariants.py:306`](../src/forze_dst/oracle/invariants.py) (`mutual_exclusion(kind, resource=, start=, end=)` — "no two holds overlap in `[start, end)` for the same resource", the checker this reuses), [`src/forze/application/contracts/invariants.py:42-57`](../src/forze/application/contracts/invariants.py) (why `no_overlap` cannot be a `SystemInvariant`: the reducer set is `SumOf | CountAll`), [RFC 0053](0053-temporal-validity.md) (the same overlap question, on validity periods).
- **Origin:** A working-time ledger where one of four writers to a table takes `pg_advisory_xact_lock(hash(employee_id))` before writing and the other three do not — finding 3 in its audit — while overlap is checked in application code against the whole history, with a docstring calling the race accepted.

---

## 1. Summary

"One owner, many facts, none overlapping" is calendars, bookings, reservations, shifts and leases.
It needs two things the framework does not have: a way to say *every* write for an owner is
serialized (so the rule cannot be enforced by three writers out of four), and a way to prove the
rule held under concurrency. `serialize_by` is the declaration; the DST assertion is the proof;
the declared guarantee is the hard stop where the backend can give one.

## 2. Motivation

The origin application's bug is the interesting part. Somebody knew the race existed — they took
an advisory lock in the writer they were working on, and wrote the reason down. Three other
writers to the same table never got one, because nothing about taking a lock in one function
propagates to the next function somebody adds.

That is the framework-shaped failure: a rule enforced at call sites is a rule that holds until the
next call site. A declaration on the spec holds for every writer, including the one written next
year.

## 3. Current state

**The primitive exists, once, privately, and correctly.** The tenant provisioner takes
`pg_advisory_xact_lock(%(key)s)` before binding a role, and `_advisory_key` derives the key with
blake2b — with the reason documented: `hash()` is salted per interpreter, so "two workers
onboarding at the same time would take *different* keys and serialize against nobody, which is the
one deployment this lock exists for". It also rejects the server's `hashtext` as undocumented and
upgrade-unstable.

So the framework already knows how to do this, and knows the two ways of getting the key wrong —
including the exact mistake the origin application made (`hash(employee_id)`).

**`DistributedLockScope` is a different tool.** It is a cross-process lock with a heartbeat, an
extend loop and fencing tokens, for a critical section that spans I/O. A write-serializing lock
wants none of that: it is taken inside one transaction and released by the commit.

**`DocumentSpec` has no serialization concept.** Verified against its fifteen fields: history,
conformity, materialization, caching, sorting, query policy, query params, encryption. Nothing
about write ordering.

**DST can already check overlap — of lock holds.** `mutual_exclusion(kind, resource, start, end)`
asserts no two holds of one resource overlap in `[start, end)`. Pointing it at *stored intervals*
rather than lock holds is the new part, and it is shared with
[RFC 0053](0053-temporal-validity.md).

**`no_overlap` cannot be a `SystemInvariant`.** The reducer set is closed at `SumOf | CountAll`
because each member must both push down to the query port and fold from a recorded trace; overlap
is pairwise. So the data-level rule is a history invariant or a database constraint, never a
declared law.

## 4. Goals / Non-goals

**Goals**

- One declaration that serializes every write for an owner, including writers added later.
- The correct key derivation, shipped, so no app repeats `hash(employee_id)`.
- A DST assertion that the intervals a workload wrote do not overlap per owner.
- An optional hard guarantee where the backend has one (0052's `NonOverlapping`).

**Non-goals**

- **Not a distributed lock.** `DistributedLockScope` covers a critical section across processes with
  a heartbeat. This is a transaction-scoped lock and nothing more.
- **Not a queue.** Serialization means "one at a time", not "in submission order". A caller that
  needs ordering needs the stream plane.
- **Not overlap *validation*.** The kit does not check intervals; it serializes writes so the app's
  own check can be correct, and declares the guarantee where the backend can enforce it.
- **Not cross-aggregate serialization.** One spec, one owner field.

## 5. Design

### 5.1 The declaration

```python
DocumentSpec(..., serialize_by="employee_id")
```

Before any write to that spec inside a transaction, the adapter takes a transaction-scoped
advisory lock keyed by `(spec_name, tenant_id, owner_value)` — spec and tenant included, so two
aggregates that happen to key on the same UUID do not serialize against each other, and two
tenants never do.

The key derivation is `_advisory_key`'s, promoted from the tenant provisioner to a shared helper,
with its docstring — the blake2b argument is the reason the helper exists and has to travel with
it.

### 5.2 Per backend

- **Postgres:** `pg_advisory_xact_lock`, taken on the transaction's connection. Released by commit
  or rollback, so no lock can outlive its transaction, and no unlock path can be forgotten.
- **Mongo:** no advisory-lock primitive. The declaration is a **wiring refusal** rather than a
  silent no-op — a spec that says its writes are serialized and is not is worse than one that
  says nothing. (A transaction-scoped alternative — a lock document with an upsert — is named in
  §8 and not built: it needs a lease, which turns a five-line feature into a lifecycle.)
- **Mock:** an in-process lock keyed the same way, so DST observes the same serialization the
  deployment gets. Without it the simulation would prove a property the mock enforces for free.

### 5.3 What is asserted, and where

| Layer | Property | Mechanism |
| --- | --- | --- |
| Serialization | two writes for one owner never interleave | DST `mutual_exclusion` over the write spans |
| Overlap | two stored intervals for one owner never overlap | DST history invariant over the rows written |
| Overlap (hard) | the database refuses an overlapping row | the `NonOverlapping` guarantee from [RFC 0052](0052-versioned-facts-correction-lineage.md) §5.4, mapped by the adapter |

The middle row is the one the origin application needed and did not have; the third is the one it
deferred in a comment. Both ship, and the invariant is the part that is valuable without Postgres.

### 5.4 The cost, stated

A serialized aggregate is a throughput ceiling per owner. That is the feature — but it is also a
lock held for the length of a transaction that may do I/O, so a slow write blocks the next write
for that owner. Named in the docs, and the reason `serialize_by` is a declaration rather than a
default.

### Alternatives considered

- **`SELECT … FOR UPDATE` on the owner's row.** Needs an owner row to exist (a booking's owner may
  live in another service) and locks a row somebody else is reading.
- **Serializable isolation for the transaction.** Correct and blunt: it serializes against
  everything, not per owner, and pushes the failure to a retry loop the app has to write.
- **A `DistributedLockScope` around the handler.** Works, costs a heartbeat and a lease for a
  section that ends at commit, and leaves the rule at the call site — the failure §2 is about.
- **Application-level overlap check with no lock** (the origin's accepted race). Two writers each
  see no conflict and both commit.

## 6. Tests

- Serialization: two concurrent writes for one owner, asserted to not interleave (by the lock's
  own observation, not by timing), and two for different owners asserted to proceed concurrently —
  the second leg matters, or a global lock would pass the first.
- Key derivation: the same owner value in two subprocesses derives the same key (the salted-hash
  regression, pinned the way the tenant provisioner pins it); different specs and different tenants
  derive different keys.
- Wiring refusal on Mongo, naming the declaration.
- Mock parity: the same interleaving test passes against the mock and against Postgres.
- DST: a workload of concurrent interval writes per owner, with the overlap history invariant; the
  ungoverned contrast (no `serialize_by`) must **fail** it, or the test proves nothing.
- With the `NonOverlapping` guarantee declared: an overlapping insert raises `conflict` even with
  the lock, since the two controls are independent.
- **Not tested:** advisory-lock semantics themselves. Postgres's property.

## 7. Docs

A section beside the aggregate declaration: what `serialize_by` guarantees, what it costs (§5.4),
the backend table, and the sentence that keeps the two layers apart — **the lock makes your check
correct; the guarantee makes the backend refuse the row.**

## 8. Out of scope

- **A Mongo lock-document implementation.** Needs a lease and an expiry policy; a five-line feature
  becomes a lifecycle. Named as the escape hatch if a consumer asks.
- **Ordering guarantees.** §4.
- **Multi-field owners** (`serialize_by=("employee_id", "site_id")`). Mostly free in the key
  derivation; out of scope until asked.
- **Read serialization.** Reads are not serialized, ever, by this declaration.

## 9. Risks

- **A lock held across I/O inside the handler's transaction.** A handler that calls an HTTP service
  mid-transaction blocks every other write for that owner for its duration. Mitigation: documented
  in §5.4; the operation's deadline already bounds it.
- **Lock-key collisions across specs or tenants.** Answered in the key: spec name and tenant are
  part of it, and the battery asserts distinctness.
- **A declaration that reads as an overlap guarantee.** `serialize_by` serializes; it does not
  validate. Mitigation: §7's sentence, and the fact that the guarantee is a separate declaration.
- **The mock's lock making DST optimistic.** If the mock serialized more than Postgres does, the
  simulation would pass where a deployment fails. Mitigation: the parity leg in §6.

## 10. Unresolved questions

- **Does `serialize_by` cover deletes and bulk writes?** It should, and bulk writes touch many
  owners — which means many locks in one transaction, in a deterministic order or it deadlocks.
  Sorting the keys is the obvious answer and needs asserting.
- **Is this a `StorageGuarantee` rather than its own field?** 0052's vocabulary already declares
  properties a backend must satisfy, and "every write for one owner is serialized" is one. The
  reason it is written as `serialize_by` here is that the guarantee vocabulary did not exist when
  this was drafted; decision 7 records that the two should be reconciled before either ships.
- **Is the lock taken by the adapter or by the transaction scope?** The adapter knows the spec; the
  transaction scope knows the connection. Implementation decides.
- **Does a lock timeout exist?** `pg_advisory_xact_lock` waits. A `_try` variant that refuses as
  `throttled` may be what a web request wants, and it changes the failure mode a caller sees.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Serialization is declared **on the spec**, not taken at call sites. A rule enforced per writer holds until the next writer, which is the origin application's audit finding 3. |
| 2 | `LOCKED` | The lock is **transaction-scoped**, so it is released by commit or rollback and no unlock path can be forgotten. `DistributedLockScope`'s heartbeat-and-lease shape is for sections that outlive a transaction. |
| 3 | `LOCKED` | The key is derived with the shipped blake2b helper, including spec name and tenant. `hash()` is salted per interpreter, so two workers would serialize against nobody — the exact mistake the origin made, already written down in `_advisory_key`. |
| 4 | `LOCKED` | A backend with no mechanism to serialize writes per key **refuses the declaration at wiring** (for Postgres that mechanism is an advisory lock; the rule names the property, not the tool). A spec claiming serialized writes that are not serialized is worse than a spec that claims nothing. |
| 5 | `ASSUMED` | The mock implements the same serialization, so DST observes what a deployment gets; the parity leg in the battery is what keeps it honest. |
| 6 | `ASSUMED` | Overlap is asserted as a **DST history invariant** (the `mutual_exclusion` shape over written rows), not as a `SystemInvariant` — the reducer set is closed at `SumOf \| CountAll` and overlap is pairwise. Shared with [RFC 0053](0053-temporal-validity.md). |
| 7 | `OPEN` | Whether this is a bespoke `serialize_by` field or a member of [RFC 0052](0052-versioned-facts-correction-lineage.md)'s `StorageGuarantee` vocabulary (`SerializedBy(key=…)`). Both declare a property the backend must satisfy and both refuse a backend that cannot; two mechanisms on one spec for one class of thing is the thing to avoid. Settled with 0052 P2, not independently. |
| 8 | `OPEN` | Whether bulk writes take many locks in a sorted order (and how that is asserted), where the lock is taken, and whether a non-waiting variant refusing as `throttled` ships. |

## 12. Phasing

- **P1** — `serialize_by`, the shared key helper, the Postgres and mock implementations, the wiring
  refusal, batteries including the subprocess key leg and the mock parity leg.
- **P2** — the DST overlap invariant and the ungoverned contrast.
- **P3** — the `NonOverlapping` guarantee declaration, once
  [RFC 0052](0052-versioned-facts-correction-lineage.md) P2 lands.
- **P4** *(demand-gated)* — bulk-write locking, multi-field owners, the non-waiting variant.
