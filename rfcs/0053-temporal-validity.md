# RFC 0053 — Temporal validity for effective-dated master data

- **Status:** 📝 Draft — **gated on [RFC 0052](0052-versioned-facts-correction-lineage.md) P2**, which owns the declared-backend-requirement primitive this design needs for its exclusion constraint.
- **Scope:** A `forze_kits.aggregates.temporal` kit for effective-dated records: `valid_from` / `valid_to` on the model, an `effective_on` filter and a batched `timeline` read, a declared `EXCLUDE USING gist` non-overlap constraint, and the open-ended-interval semantics stated once instead of per aggregate. Touches `forze_kits` and 0052's `requires` vocabulary (one new member, `ExclusionConstraint`). **No contract change** beyond that member, and no new query operator — §3 shows the lookup is already expressible.
- **Related:** [`src/forze/application/contracts/querying/expressions.py:127-165`](../src/forze/application/contracts/querying/expressions.py) (`$and` / `$or` / `$not`, the constraint bundle), [`types.py:31-40`](../src/forze/application/contracts/querying/types.py) (`$null`, the ordering operators), [`src/forze_kits/aggregates/kit.py`](../src/forze_kits/aggregates/kit.py), [RFC 0052](0052-versioned-facts-correction-lineage.md) §5.4 (the requirement vocabulary), [RFC 0054](0054-civil-time.md) (what a "date" means at a boundary).
- **Origin:** A working-time ledger where `employment_contract` and `work_schedule_version` carry `valid_from` / `valid_to` / `version` / `source`, the seed asserts non-overlap **in application code**, the models carry a comment saying the `EXCLUDE USING gist` constraint is deferred, and the effective-on lookup runs once per day inside a loop over a month.

---

## 1. Summary

An aggregate declares `temporal=TemporalPolicy(key=("employee_id",))`. Its model gains
`valid_from` and a nullable `valid_to`; the facade gains `effective_on(key, date)` and
`timeline(key, from, to)`; the kit declares one exclusion constraint per key, validated at
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
[`specs.py:58-200`](../src/forze/application/contracts/document/specs.py): the twelve fields
cover history, conformity, caching, sorting, query policy and encryption. No validity, no key,
no overlap.

**There is no way to declare the constraint.** Postgres validates columns at startup and Mongo
lists indexes; neither takes a declared requirement. [RFC 0052](0052-versioned-facts-correction-lineage.md)
§5.4 introduces one for its partial unique index, which is why this RFC waits on it rather than
inventing a second vocabulary.

## 4. Goals / Non-goals

**Goals**

- One declaration: the fields, the two reads, and the constraint.
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

TemporalPolicy(key=("employee_id",), grain="date")
```

`key` is what the period is scoped by (the non-overlap rule holds *per key*), and `grain` is
`date` or `instant`. A `date` grain is the common case and the one the exclusion constraint
below is written for; `instant` exists because a shift or a lease is not day-aligned, and it
changes the declared range type rather than the semantics.

### 5.2 Boundaries: `valid_to` is inclusive at a `date` grain

`[valid_from, valid_to]` — both ends in force. Rejected alternative: half-open `[from, to)`,
which is correct for instants and reads wrong for humans, who write "valid through 31 March".
The cost is stated rather than hidden: at an `instant` grain the range is half-open
`[from, to)`, because a closed instant range cannot express "until midnight" without naming a
last representable moment. The declaration carries the grain, so the semantics are never
ambiguous at a call site, and [RFC 0054](0054-civil-time.md) owns what "a day" means when the
answer depends on a zone.

### 5.3 The reads

- `effective_on(key_values, on)` — one row or `not_found`, using §3's predicate.
- `timeline(key_values, from, to)` — every row whose period intersects the window, sorted by
  `valid_from`, in **one** query. This is the operation the origin application did not have.

Both are facade methods over the existing query port; neither needs a new capability.

### 5.4 The declared constraint

```python
requires = (
    ExclusionConstraint(
        using="gist",
        key=("employee_id",),
        range=("valid_from", "valid_to", "[]"),
        extension="btree_gist",
    ),
)
```

Startup validates that the constraint and the extension exist and raises `internal` naming the
DDL that would satisfy it — [RFC 0052](0052-versioned-facts-correction-lineage.md) decision 3:
declared, validated, never created. On a backend with no exclusion constraints (Mongo, the
mock), the requirement is **unsatisfiable rather than skipped**: the kit refuses at wiring and
says so, because a temporal aggregate whose overlap rule is unenforced is the origin
application's bug with a framework's name on it.

### 5.5 What DST can and cannot assert

`no_overlapping_validity` is not a `SystemInvariant`: the reducer set is closed at
`SumOf | CountAll` ([RFC 0052](0052-versioned-facts-correction-lineage.md) §5.5) and overlap is
a pairwise comparison, not a scalar reduce. What DST *can* do is what
[RFC 0063](0063-per-owner-write-serialization.md) needs too — a history invariant in the
`mutual_exclusion` shape ([`src/forze_dst/oracle/invariants.py:306`](../src/forze_dst/oracle/invariants.py),
"no two holds overlap in `[start, end)` for the same resource"), applied to the intervals a
workload wrote rather than to lock holds. That checker exists; pointing it at stored rows is the
new part, and it is shared with 0063.

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
  naming the unsatisfiable requirement.
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
| 1 | `LOCKED` | Non-overlap is enforced by a **declared database constraint**, not by the kit's write path. A read-then-insert check cannot be correct under concurrency, which is the origin application's accepted race written down. |
| 2 | `LOCKED` | A backend that cannot satisfy the constraint **refuses at wiring**. A temporal aggregate with an unenforced overlap rule is worse than no kit, because the declaration reads as a guarantee. |
| 3 | `ASSUMED` | `valid_to` is **inclusive** at a `date` grain and exclusive at an `instant` grain, with the grain required in the declaration. Human-written dates are inclusive; instants cannot be closed without naming a last moment. |
| 4 | `ASSUMED` | `NULL` means open-ended. No sentinel date: `$null` is already an operator, and a sentinel lies in every export. |
| 5 | `ASSUMED` | The overlap property is asserted in DST through the existing `mutual_exclusion`-shaped history invariant rather than a new `SystemInvariant` reducer, shared with [RFC 0063](0063-per-owner-write-serialization.md). |
| 6 | `OPEN` | Whether the mock implements an in-memory overlap check (making the kit simulable, and stricter than Mongo) or stays refused like Mongo. |

## 12. Phasing

- **P1** — mixin, `TemporalPolicy`, `effective_on`, `timeline`, the `ExclusionConstraint`
  requirement and its Postgres validator, batteries. Gated on
  [RFC 0052](0052-versioned-facts-correction-lineage.md) P2.
- **P2** — the mock's overlap check and the DST leg, if decision 6 lands that way.
- **P3** *(demand-gated)* — `grain="instant"`, multi-key exclusion.
