# RFC 0054 — Civil time: wall clock versus instant

- **Status:** 📝 Draft — execution-ready, one small PR, gated only on [RFC 0067](0067-period-and-overlap.md) for the `Period` its day and month helpers return.
- **Scope:** A `forze_kits.domain.civil_time` module: a `CivilZone` value object, wall-time → instant conversion that **refuses** ambiguous and non-existent local times, local-day and month bounds, the local days an instant range spans, and an `AwareDatetime` boundary type that refuses naive input. Pure functions plus one value object; **no ports, no contract change, no adapter work.** Deliberately does not unify the two timezone islands that already exist (§3) — it sits beside them and §9 says why that is a risk.
- **Related:** [`src/forze/application/contracts/querying/internal/time_bucket.py`](../src/forze/application/contracts/querying/internal/time_bucket.py) (`ResolvedTimeBucketTimezone`, the IANA/fixed-offset split already in the analytics path), [`src/forze/application/integrations/durable/cron.py:57`](../src/forze/application/integrations/durable/cron.py) (the one place that already refuses a naive datetime, and why), [`src/forze/base/primitives/datetime.py:10`](../src/forze/base/primitives/datetime.py) (`utcnow`, and the `TimeSource` every clock read goes through), [RFC 0053](0053-temporal-validity.md) (what a "day" means at a validity boundary), [RFC 0067](0067-period-and-overlap.md) (`Period` and its bounds, which the day and month helpers return).
- **Origin:** A working-time ledger that converts Berlin wall time to instants, refuses DST-ambiguous and non-existent times with stable error codes, and computes month bounds and spanned local days — then copies those helpers into three modules with three separate `BERLIN` constants.

---

## 1. Summary

Six functions and one value object, in one module, with a DST battery across three zones. A
`CivilZone` is injected (never a module constant); `to_instant` refuses the two local times that
are not a single instant; the day and month helpers return a `Period` that carries its own bounds;
and
`elapsed_minutes` only accepts instants, so the one arithmetic that is wrong in wall time cannot
be written.

## 2. Motivation

Every scheduling, attendance, billing or SLA product needs the same conversions, and each one
rediscovers the same two failures: a local time that happens twice (fall back) and one that
never happens (spring forward). A framework that offers `utcnow` and nothing else leaves both
to the application, which is where the origin's helpers came from — and having written them
once, it copied them twice, with the zone hardcoded in each copy.

The arithmetic failure is worse than the conversion one, because it is silent: subtracting two
wall-clock times across a DST boundary is off by an hour, and the result is a plausible number.

## 3. Current state

**Two timezone islands exist, and neither is reusable.**

- The analytics path has `ResolvedTimeBucketTimezone` with a real `iana` / `fixed` split, an
  offset grammar that rejects an ambiguous `+123`, and calendar bucketing. It is internal to
  querying (`contracts/querying/internal/`) and shaped for grouping, not for domain
  conversion.
- The durable cron validates a zone, refuses a naive `after` ("a naive value would be read in
  the host timezone") and computes the next fire in local time before converting back to UTC.
  It is 60 lines inside a scheduler.

So the framework has already met this problem twice and solved it twice, privately. What no
part of it has is a domain-level conversion: **a wall-clock time plus a zone to an instant, with
the two impossible cases refused.**

**Naive datetimes are accepted almost everywhere.** `utcnow()` returns aware UTC and reads
through the ambient `TimeSource`, so framework-generated timestamps are fine. Model fields are
plain `datetime`, so a naive value from a caller is accepted and stored — `AwareDatetime` appears
exactly once in `src/`, in the progress events module. The cron refusal is the only place naive
input is treated as an error.

## 4. Goals / Non-goals

**Goals**

- One injected zone per aggregate or route, never a module constant.
- The two impossible local times are **refused**, with a stable code a caller can branch on.
- Day and month bounds as periods, and the local days an instant range spans, with the bounds
  convention carried by the value rather than the prose.
- Duration arithmetic that cannot be performed on wall-clock values.
- A boundary type that turns "naive datetime accepted silently" into a validation error where an
  app wants that.

**Non-goals**

- **Not a calendar.** Holidays, business days and working-time rules are the app's domain.
- **Not a scheduler.** The durable cron owns firing; this module owns conversion.
- **Not a replacement for the analytics bucketing.** §9 records the duplication risk; unifying
  them is a separate change with its own compatibility surface.
- **Not a `datetime` subclass.** The stdlib types stay; this is functions over them.

## 5. Design

### 5.1 `CivilZone`

```python
@final
@attrs.define(frozen=True, slots=True)
class CivilZone:
    key: str                      # one IANA name, validated at construction
    def zone(self) -> ZoneInfo: ...
```

Constructed once at wiring and injected — a route's civil zone is configuration, like a prompt
template or a registered statement. An unknown key is a `configuration` refusal at construction,
not a `ZoneInfoNotFoundError` at the first request.

### 5.2 The conversions

```python
def to_instant(zone: CivilZone, local: datetime, *, fold: int | None = None) -> datetime
```

Refuses two cases, both `precondition`:

- **`dst_ambiguous`** — the local time occurs twice. Refused rather than resolved, unless the
  caller passes `fold` to say which occurrence it means. A default would silently pick an hour.
- **`dst_nonexistent`** — the local time does not occur. Refused always: there is no instant to
  return, and shifting forward by an hour is a guess about intent.

Detection is by round-trip (`local → aware → UTC → local`), which is the only method that does
not depend on a zone database's transition table being enumerable.

```python
def local_day_bounds(zone, day: date) -> Period[datetime]     # bounds="[)"
def month_bounds(zone, year: int, month: int) -> Period[datetime]
def spanned_local_days(zone, start: datetime, end: datetime) -> tuple[date, ...]
def elapsed_minutes(start: datetime, end: datetime) -> int           # instants only
```

Both return [RFC 0067](0067-period-and-overlap.md)'s `Period` with `bounds="[)"`, so the half-open
convention travels in the value instead of in this docstring. A day whose local start does not exist (a
spring-forward midnight, which some zones have) resolves to the first instant that does, because
a day always has a first moment even when 00:00 is not it — the one place this module resolves
rather than refuses, and the reason is that the alternative is a calendar with holes in it.

`elapsed_minutes` refuses a naive argument. That refusal is the whole point of the function: the
one arithmetic a wall-clock value silently gets wrong.

### 5.3 The boundary type

`AwareDatetime` re-exported with a `naive_datetime` refusal message that names the field, for
models at an app's edge. Not applied to framework models: making `Document.created_at` aware-only
would be a breaking change to every stored row's model for a benefit the framework already has
(`utcnow` is aware).

### Alternatives considered

- **Resolve ambiguity by picking the earlier instant** (what most libraries do). Convenient, and
  it means an hour of working time vanishes or doubles with no signal. A refusal makes the
  caller state which occurrence it meant.
- **Take the zone from a process setting.** What the origin application did, three times. A
  module constant is invisible at the call site and untestable across zones.
- **Extend `ResolvedTimeBucketTimezone` instead.** It is the closest existing thing, and it is
  internal to querying with a grouping-shaped API; widening it would couple domain conversion to
  the analytics path's compatibility surface.

## 6. Tests

The battery is the deliverable as much as the code: **every function across the spring-forward
and fall-back days of three zones** with different transition shapes — `Europe/Berlin`
(01:00→02:00 CET/CEST), `America/Santiago` (southern hemisphere, midnight transition, so
`local_day_bounds` hits the nonexistent-midnight case), `Australia/Lord_Howe` (a 30-minute
transition, so an hour-shaped assumption fails).

Per zone: an ambiguous local time refuses; with `fold=0` and `fold=1` it returns two different
instants an hour apart; a nonexistent local time refuses; `local_day_bounds` on the transition
day is 23 or 25 hours long; `spanned_local_days` over a range crossing the transition returns
consecutive days with no gap or repeat; `elapsed_minutes` between two instants across the
transition is the true elapsed time, not the wall-clock difference.

`elapsed_minutes` refuses naive input. `CivilZone("Not/AZone")` refuses at construction.

## 7. Docs

A short page section under the domain helpers: the value object, the six functions, the table of
what is refused with which code, and one worked example of the arithmetic trap (two wall-clock
times across a transition, and what each function returns).

## 8. Out of scope

- **Zone-database versioning.** A transition rule can change between `tzdata` releases; a stored
  instant is stable but a stored *local* time reinterprets. Named because a long-lived schedule
  is exposed to it, not solved here.
- **Recurrence** ("every Monday at 09:00 local"). That is the cron's shape, and it already
  handles a zone.
- **Unifying the analytics bucketing.** §9, and a separate change.

## 9. Risks

- **A third timezone island.** This module makes three: analytics bucketing, cron, and this.
  Mitigation is honesty rather than code — the docs name all three and say which to use — and
  the acceptance is that both existing ones are internal to their planes and shaped for them.
  What would change it: a second consumer wanting the analytics split at the domain layer.
- **Refusals surface as user-visible errors on two days a year.** An app that passes local input
  straight through will see `dst_ambiguous` in production at 02:30 on one autumn morning. That is
  the intended behaviour and it is also a support ticket; the docs say to bind `fold` at the
  boundary where the UI knows which occurrence the user meant.
- **`fold` is a stdlib concept few readers know.** Mitigation: the parameter is named in the
  docs with the two-instants example, and the refusal message says what to pass.

## 10. Unresolved questions

- **Does `CivilZone` belong in `forze_kits.domain` or `forze.domain`?** It has no dependencies
  beyond the stdlib, which argues for the core; kits is where non-core domain helpers live
  today, which argues for kits. Implementation decides; the row records it.
- **Is there a `civil_zone` dep key**, so a route resolves its zone like any other dependency,
  or does the app hold it? Leaning: the app holds it — a zone is not a resource.

## 11. Decisions

| # | Grade | Decision |
| --- | --- | --- |
| 1 | `LOCKED` | An ambiguous local time is **refused** unless the caller passes `fold`; a nonexistent one is **always refused**. Both are `precondition` with stable codes. Picking an instant silently is how an hour of working time disappears with no signal. |
| 2 | `LOCKED` | The zone is an injected value object, never a module constant or a process setting. The origin application's three `BERLIN` constants are the failure this prevents. |
| 3 | `ASSUMED` | The day and month helpers return a `Period` with `bounds="[)"` ([RFC 0067](0067-period-and-overlap.md)), so the convention travels in the value; a nonexistent local midnight resolves forward to the first instant of the day — the single place this module resolves instead of refusing, because a calendar with holes is worse. |
| 4 | `ASSUMED` | `elapsed_minutes` accepts instants only and refuses naive input, making the silently-wrong arithmetic unwritable rather than documented. |
| 5 | `ASSUMED` | The framework's own models are **not** migrated to `AwareDatetime`; the type is offered for an app's boundary. `utcnow` is already aware, so the framework gains nothing and every stored row's model would change. |
| 6 | `OPEN` | Whether the module lands in `forze_kits.domain` or `forze.domain` (no dependencies beyond the stdlib argues core; precedent argues kits). [RFC 0067](0067-period-and-overlap.md) has the same question and the two should answer it together. |

## 12. Phasing

One PR: the value object, the six functions, the three-zone battery, the docs section. Gated only
on [RFC 0067](0067-period-and-overlap.md), whose `Period` the day and month helpers return;
[RFC 0053](0053-temporal-validity.md) cites this RFC for what a day means, but does not import it.
