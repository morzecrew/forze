# RFC 0053 — Temporal validity for effective-dated master data

- **Status:** 🚧 In progress — P1 and P1b shipped 2026-09-19 (the mixins, `TemporalPolicy`, `effective_on`, `timeline`, the declared guarantee, the mock's enforcement, the Postgres mapping and the batteries), together with [RFC 0066](0066-storage-guarantees.md) P3, which it was gated on. P3 (`grain="instant"`, multi-key exclusion) stays demand-gated. Composing with [RFC 0052](0052-versioned-facts-correction-lineage.md) is **refused**, not supported: a correction carries its predecessor's dates, so the overlap guarantee refuses it — §4's claim to the contrary is wrong and the composition waits on a filtered `NonOverlapping` (see `logs/T-0053.md`). Gated originally on 0066 P3 and [RFC 0067](0067-period-and-overlap.md) (what a period and an overlap are). Both were extracted from this RFC's first draft and 0052's.
- **Scope:** A `forze_kits.aggregates.temporal` kit for effective-dated records: `valid_from` / `valid_to` on the model, an `effective_on` filter and a batched `timeline` read, a declared non-overlap guarantee the adapter maps to its own mechanism, and one `bounds` choice per aggregate instead of a semantics paragraph per aggregate. Touches `forze_kits` only: the period type is [RFC 0067](0067-period-and-overlap.md)'s and the non-overlap guarantee is [RFC 0066](0066-storage-guarantees.md)'s, so **no contract change is left here**. **No contract change** beyond that member, and no new query operator — §3 shows the lookup is already expressible.
- **Related:** [`src/forze/application/contracts/querying/expressions.py:127-165`](../src/forze/application/contracts/querying/expressions.py) (`$and` / `$or` / `$not`, the constraint bundle), [`types.py:31-40`](../src/forze/application/contracts/querying/types.py) (`$null`, the ordering operators), [`src/forze_kits/aggregates/kit.py`](../src/forze_kits/aggregates/kit.py), [RFC 0066](0066-storage-guarantees.md) (the guarantee vocabulary, and why it is a property rather than DDL), [RFC 0067](0067-period-and-overlap.md) (`Period`, its bounds and the overlap oracle), [RFC 0054](0054-civil-time.md) (what a "date" means at a boundary).
- **Origin:** A working-time ledger where `employment_contract` and `work_schedule_version` carry `valid_from` / `valid_to` / `version` / `source`, the seed asserts non-overlap **in application code**, the models carry a comment saying the `EXCLUDE USING gist` constraint is deferred, and the effective-on lookup runs once per day inside a loop over a month.

---

## 1. Summary

An aggregate declares `temporal=TemporalPolicy(key=("employee_id",))`. Its model gains
`valid_from` and a nullable `valid_to`; the facade gains `effective_on(key, date)` and
`timeline(key, from, to)`; the kit declares one non-overlap guarantee per key, validated at
startup; and the half-open/closed question is answered once, in the framework, instead of per
application.

## 2. Motivation

Contracts, price lists, tax rates, org charts and consent records are all the same shape: a
key, a period, a payload, and the rule that two periods for one key may not overlap. Every
application writes the same three operations and the same off-by-one.

The origin application shows all three failure modes at once: the non-overlap rule lives in a
seed script rather than the database, so a concurrent insert can break it; the constraint that
would have enforced it is a deferred TODO in a comment; and the "which version applies on day
D" lookup is issued once per day in a loop, which is 30 round trips to answer one question
about a month.

## 3. Current state

**The lookup needs no new operator.** The filter language already carries `$and`, `$or`, `$not`,
the ordering operators and `$null`, so effective-on is expressible today:

```python
{"$and": [
    {"$values": {"valid_from": {"$lte": on}}},
    {"$or": [{"$values": {"valid_to": {"$null": True}}},
             {"$values": {"valid_to": {"$gte": on}}}]},
]}
```

That is the point of this RFC: the mechanism exists and the *declaration* does not, so each
application re-derives the predicate — and each one picks its own answer to whether `valid_to`
is inclusive.

**Nothing in `DocumentSpec` is period-aware.** Verified against
[`specs.py:58-200`](../src/forze/application/contracts/document/specs.py): the fifteen fields
cover history, conformity, caching, sorting, query policy and encryption. No validity, no key,
no overlap.

**There is no way to declare the rule, and no period type to state it over.**
[RFC 0066](0066-storage-guarantees.md) introduces the first and
[RFC 0067](0067-period-and-overlap.md) the second; both were extracted from this RFC's first
draft and 0052's, rather than each kit inventing one.

## 4. Goals / Non-goals

**Goals**

- One declaration: the fields, the two reads, and the guarantee.
- The non-overlap rule enforced **by the database**, not by application code.
- One stated answer to the boundary question, tested at the boundary.
- `timeline` answers a range in one call, so the origin's per-day loop is not the shape the
  framework encourages.

**Non-goals**

- **Not bitemporal.** Validity (when the fact applies) is here; lineage (when we asserted it,
  and who corrected it) is [RFC 0052](0052-versioned-facts-correction-lineage.md). Declaring
  both is supported and is the bitemporal case.
- **Not a scheduler.** "This contract starts on Monday" is data, not a timer. Nothing here
  fires at `valid_from`.
- **Not interval arithmetic.** No union, intersection or gap analysis over periods — an app that
  needs it reads the timeline and computes.

## 5. Design

### 5.1 The declaration and the mixin

```python
class TemporalMixin(CoreModel):
    valid_from: date = Field(frozen=True)
    valid_to: date | None = None      # None = open-ended, "still in force"

TemporalPolicy(key=("employee_id",), bounds="[]")
```

Two stored columns rather than one `Period` field: the filter language compares scalars and every
adapter maps them, while the *semantics* of the pair — what the end means, what an open end means,
when two of them overlap — are [RFC 0067](0067-period-and-overlap.md)'s `Period`, which the facade
returns and the guarantee names. `key` is what the period is scoped by (the non-overlap rule holds
*per key*), and `bounds` is the convention this aggregate declares.

### 5.2 Boundaries: one declaration, no per-kit rule

`bounds` is [RFC 0067](0067-period-and-overlap.md)'s convention, declared per aggregate rather than
decided per kit. A contract entered by a human is `"[]"` — "valid through 31 March" is inclusive —
and a day-aligned or instant-grained period is `"[)"`, which tiles. The kit does not redefine
either; it records which one this aggregate uses so `effective_on`, `timeline` and the guarantee all
read the same value. What a "day" is when the answer depends on a zone stays
[RFC 0054](0054-civil-time.md)'s.

### 5.3 The reads

- `effective_on(key_values, on)` — one row or `not_found`, using §3's predicate.
- `timeline(key_values, from, to)` — every row whose period intersects the window, sorted by
  `valid_from`, in **one** query. This is the operation the origin application did not have.

Both are facade methods over the existing query port; neither needs a new capability.

### 5.4 The declared guarantee

```python
guarantees = (
    NonOverlapping(key=("employee_id",), period=("valid_from", "valid_to"), bounds="[]"),
)
```

A property, not DDL, for the reason [RFC 0066](0066-storage-guarantees.md) decision 1 gives: the
declaration says *no two rows sharing this key have overlapping periods* — overlapping in
[RFC 0067](0067-period-and-overlap.md)'s sense — and the adapter decides how. Postgres satisfies it
with an `EXCLUDE USING gist` constraint over
`(key, daterange(valid_from, valid_to, '[]'))` and the `btree_gist` extension — words that live in
`forze_postgres` and nowhere near the contract — and startup validates that the constraint and the
extension exist, raising `internal` with the DDL that would satisfy it. Nothing is created:
declared, validated, never created.

On a backend with no mechanism for it — Mongo has no exclusion constraint, and this one is not
expressible as a partial unique index the way `UniqueTogether` is — the guarantee is
**unsatisfiable rather than skipped**, which is [RFC 0066](0066-storage-guarantees.md) decision 4:
the kit refuses at wiring and says so, because a temporal aggregate whose overlap rule is
unenforced is the origin application's bug with a framework's name on it.

### 5.5 What DST can and cannot assert

`no_overlapping_validity` is not a `SystemInvariant`: the reducer set is closed at
`SumOf | CountAll` ([RFC 0052](0052-versioned-facts-correction-lineage.md) §5.5) and overlap is
a pairwise comparison, not a scalar reduce. What DST *can* do is
[RFC 0067](0067-period-and-overlap.md)'s `no_overlapping_periods` — the `mutual_exclusion` shape
([`src/forze_dst/oracle/invariants.py:306`](../src/forze_dst/oracle/invariants.py), "no two holds
overlap in `[start, end)` for the same resource") generalized from lock holds to written rows.
That invariant was extracted precisely because this RFC and
[RFC 0063](0063-per-owner-write-serialization.md) were each about to write it.

### Alternatives considered

- **A `valid` range column instead of two dates.** Closest to Postgres, and it makes the filter
  language carry a range type for one backend's benefit. Two scalar columns filter with the
  operators that already exist.
- **Enforce non-overlap in the kit's write path.** Portable and racy — two transactions each
  read no conflict and both insert. The constraint is the only place the rule can be true.
- **Sentinel `valid_to = 9999-12-31` instead of `NULL`.** Simplifies the predicate and lies in
  every export, report and UI. `$null` is already in the operator set.

## 6. Tests

- Boundary battery: `effective_on` at `valid_from`, at `valid_to`, one day either side, against
  an open-ended row and a closed one.
- `timeline` over a window that clips both ends, one that covers nothing, one that hits an
  open-ended row.
- Overlap refusal on Postgres: an insert that overlaps raises `conflict`; two concurrent
  overlapping inserts leave exactly one committed.
- Wiring refusal: declaring `temporal` against the Mongo or mock document route fails at wiring,
  naming the unsatisfiable guarantee.
- DST: concurrent inserts for one key under the overlap history invariant.
- **Not tested:** that `EXCLUDE USING gist` works. That is Postgres's property; the battery
  asserts the refusal reaches the caller as `conflict`.

## 7. Docs

One section, and the sentence that has to be exact: **`valid_to` is inclusive at a `date` grain
and exclusive at an `instant` grain, and the declaration says which.** Plus the unsatisfiable
requirement, named as a deliberate refusal rather than a missing feature.

## 8. Out of scope

- **Gap detection** ("this employee has no contract for April"). It is a read over `timeline`
  and a policy call about what a gap means.
- **Retroactive rewrite.** Changing a past period is a correction, which is
  [RFC 0052](0052-versioned-facts-correction-lineage.md)'s business.
- **Multi-key exclusion** (non-overlap per `(employee_id, cost_centre)`). The vocabulary takes a
  tuple, so this is mostly free; it is out of scope only because no consumer has asked.

## 9. Risks

- **The inclusive/exclusive split by grain is a footgun.** Two semantics in one kit is one more
  than ideal. Mitigation: the grain is required in the declaration, printed in the docs table,
  and asserted at the boundary in the battery.
- **Refusing Mongo and the mock narrows the kit to one backend.** Accepted, and named in §5.4:
  the alternative is a kit that silently does not enforce its own rule. A named consumer on
  Mongo would reopen it with a different mechanism (a pre-write range query under a serialized
  write, which is 0063's shape).

## 10. Unresolved questions

- **Does the mock get an in-memory overlap check so DST can run the kit at all?** Without one,
  the kit is untestable under simulation, and with one, the mock is stricter than Mongo.
  Leaning: yes, and the wiring refusal stays for Mongo only.
- **Is `grain="instant"` in the first phase?** The origin case is `date`. Instant costs a
  second range type in the requirement vocabulary.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | Non-overlap is enforced by a **declared guarantee the backend satisfies**, not by the kit's write path. A read-then-insert check cannot be correct under concurrency, which is the origin application's accepted race written down. |
| 2 | `LOCKED` | A backend that cannot satisfy the guarantee **refuses at wiring**. A temporal aggregate with an unenforced overlap rule is worse than no kit, because the declaration reads as a guarantee. |
| 3 | `ASSUMED` | The aggregate **declares its `bounds`** rather than the kit deciding by grain. The convention and its edge behaviour are [RFC 0067](0067-period-and-overlap.md)'s; what this kit owns is that the declaration is required and is read by `effective_on`, `timeline` and the guarantee alike. |
| 4 | `ASSUMED` | `NULL` means open-ended. No sentinel date: `$null` is already an operator, and a sentinel lies in every export. |
| 5 | `ASSUMED` | The overlap property is asserted through [RFC 0067](0067-period-and-overlap.md)'s history invariant rather than a new `SystemInvariant` reducer — the reducer set is closed at `SumOf \| CountAll` and overlap is pairwise. |
| 6 | `OPEN` | Whether the mock implements an in-memory overlap check (making the kit simulable, and stricter than Mongo) or stays refused like Mongo. |
| 7 | `ASSUMED` | `timeline` **paginates** rather than answering a window "in one query". A timeline is unbounded by nature, and an unbounded read meets the store's implicit cap, which truncates with a warning the caller never receives — see `logs/T-0053.md`. |
| 8 | `ASSUMED` | The aggregate's convention is stated on the **domain model** as well as the policy, and the kit refuses a mismatch. An update patch carries one endpoint and the stored row the other, so only the model sees the period a write produces — see `logs/T-0053.md`. |
| 9 | `ASSUMED` | A period **in force on no day** is refused at the model, beside the inverted one. It answers no query and conflicts with nothing, and its absence is also what keeps `timeline`'s filter exact — see `logs/T-0053.md`. |
| 10 | `LOCKED` | Composing with [RFC 0052](0052-versioned-facts-correction-lineage.md) is **refused at build** until the guarantee vocabulary has a non-overlap restricted to current versions. §4's "declaring both is the bitemporal case" is superseded by this row — see `logs/T-0053.md` (self-audit, finding 2). |

## 12. Phasing

- **P1** — mixin, `TemporalPolicy`, `effective_on`, `timeline`, batteries. Gated on
  [RFC 0067](0067-period-and-overlap.md) for the period semantics.
- **P1b** — the `NonOverlapping` declaration, once
  [RFC 0066](0066-storage-guarantees.md) P3 ships it.
- **P2** — the mock's overlap check and the DST leg, if decision 6 lands that way.
- **P3** *(demand-gated)* — `grain="instant"`, multi-key exclusion.
