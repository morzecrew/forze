# T-quarantine — quarantine durable runs that recovery cannot decrypt

Building the durable-run quarantine on `feat/durable-run-quarantine`, based on `main`
(75dafd1f5). No RFC: the author's brief is the spec. It settles the status, when a run
enters it, the requeue verb and its tenant scoping, and the backends; it delegates the
exact predicate and the cancel of a quarantined run to the executor, and asks for both to
be logged. Every entry below is therefore `UNLISTED`.

Today a run the recovery scan cannot decrypt is held out of the scan, from two leases
doubling to a day (`src/forze/application/integrations/durable/recovery.py:56-58`). Past
roughly limit × cap / interval such runs (about 29k at defaults) they crowd the scan again,
which the `ponytail:` comment at `recovery.py:54` names. A status the scan never claims
lifts that ceiling.

## Plan

Commits, in this order:

1. The task log (this plan).
2. The feature, one commit: the status, the predicate, entering quarantine, the requeue verb
   on every backend, the cancel of a quarantined or requeued run, quiesce, tests, docs and
   the changelog. They cannot land apart: the status without the requeue strands runs, and
   the requeue without the cancel guard lets a requeued saga be cancelled unread.

Files touched:

- `src/forze/application/contracts/durable/function/run_store.py`: `DurableRunStatus.QUARANTINED`; docstrings of `claim_abandoned`, `payloads_unreadable`, the port's tenant-reach list.
- `src/forze/application/contracts/durable/function/run_admin.py`: `DurableRunAdminPort.requeue_quarantined() -> int`; module, port, `list_runs` and `request_cancel` docstrings.
- `src/forze/application/integrations/durable/recovery.py` + `__init__.py`: `quarantines_unopenable(error, *, lease_for, attempts)` beside `unopenable_hold`, sharing its doubled value; the `ponytail:` ceiling comment goes (entry 1).
- `src/forze_postgres/adapters/durable/run_store.py`: `_open_claimed` quarantines instead of holding (fenced on `attempts` and `status = 'running'`, lease cleared); `requeue_quarantined`; `request_cancel` (entries 2, 3).
- `src/forze_mongo/adapters/durable/run_store.py`: the same three.
- `src/forze_mock/adapters/durable/run_store.py`: `requeue_quarantined`, `request_cancel`; the mock never seals, so it never quarantines.
- `src/forze_kits/integrations/quiesce/quiesce.py`: a quarantined run holds the durable plane (entry 5).
- `src/forze_kits/integrations/durable/runner.py`: `recover` docstring only.
- `tests/support/durable_conformance.py`: `run_quarantine_scenario` (engines, through the real decrypt path) and `run_quarantine_admin_scenario` (compared mock vs engine, from a run put into quarantine directly).
- `tests/integration/test_forze_postgres/test_pg_durable_run_store_integration.py`, `tests/integration/test_forze_mongo/test_mongo_durable_run_store_integration.py`: legs for the first.
- `tests/integration/test_forze_postgres/test_pg_durable_conformance.py`, `tests/integration/test_forze_mongo/test_mongo_durable_conformance.py`: mock-vs-engine legs for the second.
- `tests/unit/test_forze/application/integrations/test_durable_recovery_hold.py`: the predicate.
- `tests/unit/test_forze_mock/test_mock_durable.py`: the mock leg; `tests/unit/test_forze_kits/test_quiesce.py`: the quiesce leg; `tests/unit/test_forze_kits/test_durable_cancel.py`: its list-only admin double gains the verb.
- `pages/docs/data-events/durable-execution.md`, `pages/docs/reference/contracts/durable.md`: the lifecycle, the status, the requeue call.
- `CHANGELOG.md`: an `Added` entry naming the break.

Checked and left alone: the claim predicates match only `pending` and `running`
(`src/forze_postgres/adapters/durable/run_store.py:418-421`, the Mongo `claimable` filter,
the mock's `_is_abandoned`), so the scan excludes the new status with no change, and the
`(status, created_at)` indexes the docs recommend carry no status predicate. `begin`,
`renew` and every terminal write are guarded on `pending` or `running`. Telemetry outcomes
describe executions, not statuses. Temporal maps its own `DurableWorkflowRunStatus`, and no
DST oracle, FastAPI or MCP surface models `DurableRunStatus`. `GuardedWritePort` and the
port proxies forward any name, and the admin port is a simple dependency that neither wraps.

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:00:00Z
attempt: 1
claim: the brief's predicate, doubled value `lease_for * 2**attempts >= cap` rather than the floored hold, cannot keep a long lease from quarantining on its first failure; after a claim `attempts >= 1`, so the doubled value is at least two leases, and the floored hold reaches the cap exactly when the doubled value does
evidence: src/forze/application/integrations/durable/recovery.py:56-58 — `doubled = lease_for * 2 ** min(max(attempts, 1), 20)` and `max(2 * lease_for, min(UNOPENABLE_HOLD_CAP, doubled))`; with a 12-hour lease the first claim's doubled value is already the cap
action: decided
proposal: ASSUMED — `quarantines_unopenable(error, *, lease_for, attempts)` is true when the fault is not passing, `attempts >= 2`, and the hold's own doubled value has reached `UNOPENABLE_HOLD_CAP`. At the stock 5-minute lease that is the ninth claim, after holds totalling about 42 hours. The `attempts >= 2` floor is what keeps a lease of half a day or more from quarantining on the run's first claim. `attempts` counts every claim, crash reclaims included, as the hold's doubling already does, so a long-lease run with a crash history can still quarantine on its first unopenable claim; a count of unopenable claims would need a column.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:01:00Z
attempt: 1
claim: the brief asks what `request_cancel` does to a quarantined run, consistent with a saga never being cancelled unread
evidence: src/forze_kits/integrations/durable/runner.py:382-386 — only a claim that opened the record decides an ask, and a saga decides it by its journal
action: decided
proposal: ASSUMED — `request_cancel` stamps `cancel_requested_at` on a quarantined run (first instant kept), leaves it quarantined, and returns `True`, as for a running run. The runner decides the ask once a requeued run is claimed and opened.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:02:00Z
attempt: 1
claim: requeue creates a state that did not exist before, a `pending` run that has already run, and `request_cancel` lands every `pending` run `cancelled` at once, so a requeued saga past its pivot would be cancelled unread
evidence: src/forze_postgres/adapters/durable/run_store.py:640-645 — `CASE WHEN status = 'pending' THEN 'cancelled'`; the Mongo arm at src/forze_mongo/adapters/durable/run_store.py:573 and the mock at src/forze_mock/adapters/durable/run_store.py:306 do the same; nothing else moves a run back to `pending`, so until now a pending run had `attempts = 0`
action: decided
proposal: ASSUMED — `request_cancel` lands `cancelled` at once only for a `pending` run never claimed (`attempts = 0`). A pending run that has been claimed before is stamped and left pending; the scan claims it and the runner decides, as it does for a dead holder's run. No run existing before this change is affected.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:03:00Z
attempt: 1
claim: the brief accepts that a requeued run whose key is still gone may be quarantined again quickly, and leaves the requeue's other fields to the executor
evidence: src/forze/application/integrations/durable/recovery.py:56 — the doubling restarts from the run's `attempts`, which the requeue keeps
action: decided
proposal: ASSUMED — a requeued run whose key is still gone is quarantined on its next claim, since its `attempts` already put the doubled value past the cap. The requeue sets `status = pending`, clears `leased_until` and `available_at` (due now, with no clock to skew between the application and the database), and keeps `attempts`, `error`, the payloads and both cancel stamps. Entering quarantine likewise leaves `error` alone, as the hold write does: the reason is logged, and `payloads_unreadable` on the listing shows it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:04:00Z
attempt: 1
claim: the quiesce sweep treats `pending` and `running` as the durable runs still owing work; a quarantined run owes work too, and leaving it out would report the plane settled with runs stranded in the store
evidence: src/forze_kits/integrations/quiesce/quiesce.py:31 — `_UNFINISHED_RUNS = (DurableRunStatus.PENDING, DurableRunStatus.RUNNING)`
action: decided
proposal: ASSUMED — `quarantined` joins the unfinished statuses. A quarantined run never settles by waiting, so the plane reports it as residual (`runs still quarantined`) until an operator requeues it or gives up on it.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T09:05:00Z
attempt: 1
claim: a new `DurableRunStatus` member breaks code that matches the statuses exhaustively, and `requeue_quarantined` widens a protocol applications may implement
evidence: src/forze/application/contracts/durable/function/run_admin.py:169 — `DurableRunAdminPort` is a `runtime_checkable` Protocol
action: decided
proposal: ASSUMED — the commit is `💥 feat(...)!` with a `BREAKING CHANGE:` footer, and the `Added` entry says **Breaking** for an exhaustive match on the statuses and for an admin port of the application's own, which must add the verb.
```

**Drift count: 0.**

Building it turned up three more things.

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-16T12:20:00Z
attempt: 1
claim: entry 4 is wrong about the requeue's fields; clearing `leased_until` and `available_at` changes nothing, since quarantine already cleared the lease and a quarantined run was claimed, so its `available_at` had passed
evidence: src/forze_postgres/adapters/durable/run_store.py:555 — the requeue now sets `status = 'pending'` and `updated_at` alone; the claim predicate admits a pending run only once `available_at IS NULL OR available_at <= now()`, which held when the run was last claimed
action: decided
proposal: ASSUMED — supersedes entry 4's field list. The requeue sets the status and `updated_at`, and keeps every other field: the run is due and has no lease already. The rest of entry 4 stands.
```

```divergence
decision: unlisted
grade: UNLISTED
class: discovery
at: 2026-10-16T12:21:00Z
attempt: 1
claim: the hold's doubling stops at 2**20 leases, which the predicate shares, so a lease under about 82 ms never reaches the cap; such a run is held for at most 2**20 leases and never quarantined
evidence: src/forze/application/integrations/durable/recovery.py:32 — `lease_for * 2 ** min(max(attempts, 1), 20)`; one day / 2**20 is about 82 ms
action: decided
proposal: ASSUMED — leave the clamp. A recovery lease is the time a worker has before its run is reclaimed, and nothing near 82 ms is a working setting; raising the exponent overflows `timedelta` for a long lease instead.
```

```divergence
decision: unlisted
grade: UNLISTED
class: spec-gap
at: 2026-10-16T12:22:00Z
attempt: 1
claim: the brief names the admin call but not whether the runner fronts it as it fronts `request_cancel`, behind a capability gate, or whether a quarantine is counted in telemetry
evidence: src/forze_kits/integrations/durable/runner.py:214 — the runner gates a cancel on `supports_cancel`, because an engine tier may not reach a running body
action: decided
proposal: ASSUMED — `requeue_quarantined` lives on the admin port only, with no runner method and no capability: it touches stored rows, which every backend implementing the port can reach. A quarantine is logged at exception level with the run id and claim count; no metric is added. Both can follow if an operator surface asks for them.
```

## Self-audit — 2026-10-16

**Scope.** The whole branch against `main` (75dafd1f5), audit fixes included: 24 files,
+1152 −85 (source +298 −76 in 10 files, tests +606 −4 in 9, docs, changelog, log and the
vulture baseline +248 −5 in 5). Re-read the brief before the diff.

**Checks run.**

- Every guard the change adds was sabotaged one at a time against the committed tree and
  restored from git: 19 sabotages, 19 caught. Recovery predicate: the first-claim floor, the
  passing-fault exclusion, `>=` turned `>`. Postgres and Mongo each: never quarantining, an
  unfenced quarantine write, a quarantine that keeps its lease, a requeued pending run
  cancelled at once, a quarantined run not askable, an unscoped requeue. Mock: the last
  three. Quiesce: a quarantined run settling the plane.
- Patch coverage over the new source lines, with the unit and both engines' durable
  integration tests: 52 of 52 lines covered.
- `just quality -s`: 19 of 19 gates green. `just test tests/unit`: 18113 passed, 16
  skipped. The Postgres and Mongo durable suites (run store, conformance, tenancy, step,
  schedule): 76 passed. The kits durable, quiesce and saga unit tests: 224 passed; the DST
  durable, crash and saga tests: 41 passed.
- After the audit refactored the Mongo test helpers, the Mongo sabotages and the Postgres
  unfenced one were run again: 7 of 7 caught.

**Findings, all fixed on the branch.**

1. MEDIUM — the quarantine write's fence on `attempts` had no test a sabotage could fail: no
   sequential scenario puts another claim between the scan's claim and its write. Added
   `test_a_scan_quarantines_only_a_run_it_still_holds` on both engines
   (tests/integration/test_forze_postgres/test_pg_durable_run_store_integration.py:298,
   tests/integration/test_forze_mongo/test_mongo_durable_run_store_integration.py:414), which
   reclaims the run with a healthy key just before the write; the unfenced sabotage now fails
   it on both.
2. MEDIUM — the predicate's boundary, `>=` against the cap, was unpinned: the stock lease
   never lands on the cap exactly, so `>` passed every test. A 45-minute lease, which doubles
   to exactly one day on the fifth claim, now pins it.
3. LOW — the requeue cleared two fields that were already settled; dropped (entry 7).
4. LOW — the predicate's docstring implied measuring by the doubled value differs from
   measuring by the hold. For every attempt count after a claim they agree (entry 1); the
   docstring now says what the first-claim floor is for instead.
5. LOW — two docstring reflows, and a duplicated Mongo `expire` helper in the new tests,
   extracted.

**Residue.** The `status = 'running'` half of the quarantine write's guard is unreachable
without a stale fence: only the claim holding `attempts` can write, and nothing moves a run
it holds out of `running`. It mirrors the hold write and has no test of its own. The
predicate counts crash reclaims as well as failed opens (entry 1), so a long-lease run with
a crash history can still quarantine on its first unopenable claim.

**Drift count: 0.**
