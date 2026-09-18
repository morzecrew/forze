# RFC 0067 — Period, its bounds, and the overlap oracle

- **Status:** ✅ Complete — shipped 2026-09-17 (#441): `Period`, its bounds, the predicates and `no_overlapping_periods`. Was **first in the batch's execution order**: [0066](0066-storage-guarantees.md) needs it to define what "overlap" means, and three drafts currently restate its rules ([0053](0053-temporal-validity.md), [0054](0054-civil-time.md), [0063](0063-per-owner-write-serialization.md)).
- **Scope:** One value object — a `Period` of two endpoints and a bounds convention — with the predicates that make it useful (`overlaps`, `contains`, `intersects_window`), and one `forze_dst` invariant that asserts no two periods sharing a key overlap in a recorded history. Pure domain code plus one oracle; **no ports, no spec field, no adapter work, no backend anywhere in it.**
- **Related:** [`src/forze_dst/oracle/invariants.py:306`](../src/forze_dst/oracle/invariants.py) (`mutual_exclusion(kind, resource=, start=, end=)` — "no two `kind` holds may overlap in `[start, end)` for the same `resource`", the checker this generalizes from lock holds to stored rows), [`src/forze/application/contracts/invariants.py:42-57`](../src/forze/application/contracts/invariants.py) (why overlap cannot be a `SystemInvariant`: `Reducer` is closed at `SumOf | CountAll` and overlap is pairwise), [`src/forze/application/contracts/querying/types.py:31-40`](../src/forze/application/contracts/querying/types.py) (`$null` and the ordering operators a period's filter compiles to), [`src/forze/application/contracts/querying/internal/time_bucket.py`](../src/forze/application/contracts/querying/internal/time_bucket.py) (calendar bucketing, the nearest shipped thing and a different job), [RFC 0054](0054-civil-time.md) (what a day *is*, when the answer depends on a zone).
- **Origin:** Extracted from the kits distilled from a working-time ledger: three of them declared periods, each with its own bounds rule, and two of them wrote the same overlap assertion. The ledger itself had the same duplication — a seed script, a policy engine and a travel engine each deciding separately whether a period's end was in force.

---

## 1. Summary

`Period(start, end, bounds)` with `end=None` meaning open-ended, `overlaps` and `contains` defined
once against the bounds convention, and `no_overlapping_periods(...)` as a DST invariant over a
recorded history. Everything that declares a period — an effective-dated contract, a booking, a
disclosure window, a local day — says it the same way and gets the same answers at the edges.

## 2. Motivation

A period is four decisions, and each one is silently re-made per feature: is the end in force, is
an open end expressible, what does overlap mean at a shared endpoint, and what does a
zero-length period mean. Get two of them different in two modules and the disagreement surfaces
as an off-by-one on the last day of a month.

The framework currently has none of it, so every consumer decides. Three drafts in this batch
already disagreed in review: one had an inclusive end, one half-open, one did not say. The ledger
they came from shipped all three at once.

The second half is the assertion. "No two periods for one key overlap" is the property behind
effective-dated master data, bookings, shifts and leases — and it cannot be a `SystemInvariant`,
because that declaration reduces a read-set to one number and overlap is pairwise. So each
consumer was about to write its own history invariant. There is already one for lock holds
(`mutual_exclusion`); generalizing it is smaller than repeating it.

## 3. Current state

**No period type exists.** Verified: no `Period`, `Interval`, `DateRange` or `TimeRange` anywhere
in `src/`. The phrase "half-open" appears only in resilience windows and probe timing — unrelated
uses of the idea, with no shared vocabulary.

**One overlap checker exists, for a different subject.**
`mutual_exclusion(kind, resource=, start=, end=)` asserts that no two *lock holds* of one resource
overlap in `[start, end)`. It reads a recorded history, it already answers the pairwise question,
and it is hard-coded to the hold-marker shape.

**The overlap property cannot be declared as a law.** `SystemInvariant` is a `ReadSet` reduced by
`SumOf | CountAll` with `holds(value) -> bool`, and the reducer set is closed on purpose: every
member has to push down to the query port *and* fold from a recorded trace. Overlap does neither.

**The filter language can express a period's lookups already.** `$and`, `$or`, `$null` and the four
ordering operators are enough for "effective on D" and "intersects window" — so this RFC adds a
type and its predicates, not query syntax.

## 4. Goals / Non-goals

**Goals**

- One period type, with the bounds convention **in the value**, not in each caller's head.
- `overlaps` decided once, including at a shared endpoint and for an open end.
- One DST invariant for "no two periods per key overlap", usable over any recorded rows.
- Nothing in it that knows a backend, a spec, a plane or a transport.

**Non-goals**

- **Not interval arithmetic.** No union, intersection, difference or gap analysis. A consumer that
  needs a coverage report reads the periods and computes; naming it here would grow an algebra
  nobody has asked for.
- **Not a calendar.** What a day, month or business day *is* stays [RFC 0054](0054-civil-time.md)'s;
  this type holds whatever endpoints it is given.
- **Not a storage concern.** How a backend enforces non-overlap is
  [RFC 0066](0066-storage-guarantees.md)'s `NonOverlapping` guarantee, which uses this type to say
  what it means.
- **Not a scheduler.** A period's start is data, not a timer.

## 5. Design

### 5.1 The type

```python
Bounds = Literal["[]", "[)", "(]", "()"]

@final
@attrs.define(frozen=True, slots=True)
class Period[T: (date, datetime)]:
    start: T
    end: T | None = None          # None = open-ended, still in force
    bounds: Bounds = "[)"

    @property
    def is_open_ended(self) -> bool: ...
    def contains(self, at: T) -> bool: ...
    def overlaps(self, other: "Period[T]") -> bool: ...
    def intersects(self, start: T, end: T | None) -> bool: ...
```

Generic over `date` and `datetime` rather than two types: the predicates are identical and the
difference is the grain a consumer chose. Mixing them in one comparison is a type error, which is
the property that matters.

**`bounds` defaults to `"[)"`** — half-open, the convention that composes: consecutive periods
`[Jan 1, Feb 1)` and `[Feb 1, Mar 1)` tile a timeline with no gap and no overlap, which a closed
convention cannot do without subtracting one grain somewhere. `"[]"` exists because humans write
"valid through 31 March", and a consumer that takes dates from people should say so in the value
instead of subtracting a day on the way in.

### 5.2 Overlap, decided once

Two periods overlap when each one's start precedes the other's end, with the *inclusive* variants
admitting equality. Concretely: a shared endpoint overlaps under `"[]"` and does not under
`"[)"`; an open end overlaps everything after its start; two periods with different `bounds`
compare under the stricter reading of each endpoint, so a mixed comparison never claims more than
both conventions agree on.

A zero-length period (`start == end`) is **empty under `"[)"`** and a single point under `"[]"`.
It is not refused: a booking cancelled in the same instant it was made is a real row, and a type
that rejected it would push the case back into every caller.

### 5.3 The invariant

```python
no_overlapping_periods(
    kind="work_interval",       # the recorded effect's label
    key="employee_id",
    start="valid_from",
    end="valid_to",
    bounds="[]",
) -> Invariant
```

An `Invariant` in the shipped sense — `Callable[[History], list[Violation]]` — so it composes with
everything `Simulation(invariants=[...])` already takes. It reads the rows a run wrote, groups by
`key`, and reports each overlapping pair with both row identities, because "these two overlap" is
the only violation message worth having.

It generalizes `mutual_exclusion` rather than replacing it: that one reads lock-hold markers, this
one reads written rows, and both answer the same pairwise question. Whether they end up sharing an
implementation is §10's question.

### 5.4 What a consumer gets

- [RFC 0053](0053-temporal-validity.md): `valid_from` / `valid_to` *are* a `Period`, and the kit's
  grain choice becomes "which `bounds` this aggregate declares" instead of a semantics paragraph.
- [RFC 0063](0063-per-owner-write-serialization.md): the overlap assertion is this invariant, not a
  private one.
- [RFC 0054](0054-civil-time.md): `local_day_bounds` and `month_bounds` return a `Period` instead of
  a tuple, so the half-open rule is carried by the value rather than the docstring.
- [RFC 0066](0066-storage-guarantees.md): `NonOverlapping` names a period's two fields and its
  bounds, and the adapter maps that to whatever range type it has.

### Alternatives considered

- **Two scalar fields and a convention in the docs.** What the drafts did. Free, and it is the
  duplication §2 measures: the convention lives in prose, so the next module picks again.
- **A closed default (`"[]"`).** Reads better for human-entered dates and breaks tiling: two
  adjacent closed periods overlap at their shared endpoint unless a caller subtracts a grain,
  which is the off-by-one this exists to prevent. Available as a declaration instead.
- **Refusing mixed-bounds comparison.** Stricter and pushes the case back to callers. Comparing
  under the stricter reading of each endpoint is defined, conservative and testable.
- **Making it a `SystemInvariant` reducer.** Rejected in §3 and by
  [RFC 0052](0052-versioned-facts-correction-lineage.md) decision 4: a join-shaped reducer has to
  fold from a trace too, and nothing else asks for one.

## 6. Tests

A table-driven battery, because the value of this type is entirely at its edges:

- `contains` at `start`, at `end`, one grain either side, for all four bounds, open-ended and
  closed.
- `overlaps` for every pair shape: disjoint, touching at a point, nested, identical, one open-ended,
  both open-ended, and the same pairs under each bounds combination — the shared-endpoint cases are
  the ones that matter, so they are enumerated rather than sampled.
- Zero-length periods under each bounds convention.
- Mixed bounds: the conservative reading is asserted in both directions (`a.overlaps(b) ==
  b.overlaps(a)`), which is the property a wrong implementation breaks first.
- `date` versus `datetime`: mixing them fails type-checking (asserted in the typing battery, not at
  runtime).
- The invariant: a history with one overlapping pair per key reports exactly that pair; a tiling
  history reports nothing; an open-ended row overlapping a later one is caught; grouping is per key
  (two keys with mirrored periods report nothing).
- **Not tested:** timezone behaviour. Endpoints arrive as given; [RFC 0054](0054-civil-time.md)
  owns where they come from.

## 7. Docs

A short domain-helpers section: the type, the bounds table with a tiling example, the overlap rule
at a shared endpoint, and one sentence on the invariant. The bounds table is the whole
documentation value — a reader should never have to re-derive whether an end is in force.

## 8. Out of scope

- **Interval algebra** (union, gaps, coverage). §4, and the named escape hatch is "read the periods
  and compute".
- **Recurring periods** ("every Monday 09:00–17:00"). A different type, and the durable cron already
  owns recurrence.
- **Period-valued query operators** (`$overlaps` on a range column). The filter language can express
  the lookups with what it has; a native range operator is [RFC 0066](0066-storage-guarantees.md)'s
  business if an adapter wants to push down.
- **Duration.** A period is two endpoints; how long it is depends on a calendar, which is 0054's.

## 9. Risks

- **A type that is almost `Period` from some library.** Someone will ask why not a dependency. The
  answer is in the shape: this one is generic over `date`/`datetime`, carries bounds in the value,
  and has to be comparable and hashable for a guarantee declaration — and adding a dependency for
  forty lines is the trade the framework does not make.
- **Bounds becoming a per-call argument.** If callers start passing `bounds` to predicates the
  convention leaves the value and the duplication returns. Mitigated by the API: predicates take no
  bounds, only periods.
- **Over-reach into an algebra.** Each new consumer will want one more method. The line is stated in
  §4 and §8, and the test of a proposed method is whether two consumers need it.

## 10. Unresolved questions

- **Does `mutual_exclusion` get reimplemented on top of this, or stay as it is?** Sharing the
  pairwise core is tempting and touches a shipped assertion with its own battery; leaving them
  separate duplicates twenty lines. Leaning: leave it, and note the relationship in both
  docstrings.
- **Does `Period` live in `forze.domain` or `forze_kits.domain`?** Same question
  [RFC 0054](0054-civil-time.md) has, and the two should answer it the same way — a value object
  with no dependencies argues for the core, precedent argues for kits.
- **Is `bounds` a four-member literal or just `"[)"` and `"[]"`?** The other two are expressible and
  nothing in the batch needs them; four is uniform, two is honest about demand.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | The bounds convention lives **in the value**, not in each consumer's documentation. Three drafts in this batch disagreed about the end being in force before this row existed; the ledger they came from shipped all three readings at once. |
| 2 | `ASSUMED` | The default is **half-open `"[)"`**, because consecutive periods must tile with no gap and no overlap; `"[]"` is declarable for human-entered dates rather than emulated by subtracting a grain. |
| 3 | `ASSUMED` | `end=None` is open-ended. No sentinel date — a sentinel lies in every export, report and comparison. |
| 4 | `ASSUMED` | A zero-length period is empty under `"[)"` and a point under `"[]"`, and is **not refused**: it is a real row, and refusing it pushes the case into every caller. |
| 5 | `ASSUMED` | Mixed-bounds comparison is defined under the stricter reading of each endpoint rather than refused, so the predicate is total and symmetric. |
| 6 | `LOCKED` | Overlap is asserted as an `Invariant` over a recorded history — `Callable[[History], list[Violation]]` — never as a `SystemInvariant`. The reducer set is closed at `SumOf \| CountAll` and overlap is pairwise. |
| 7 | `ASSUMED` | Generic over `date` and `datetime`, so mixing grains is a type error rather than a runtime coercion. |
| 8 | `OPEN` | Whether `mutual_exclusion` is reimplemented on this core, where the type lands (`forze.domain` vs `forze_kits.domain`, answered with 0054), and whether `bounds` carries two members or four. |

## 12. Phasing

One PR: the type, the predicates, the edge-case battery, the DST invariant with its own battery,
and the docs section with the bounds table. Nothing is gated on it;
[RFC 0066](0066-storage-guarantees.md), 0053, 0054 and 0063 all consume it.
