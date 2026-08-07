# RFC 0032 — Pipeline scheduling & run tracking kit

- **Status:** 📝 Draft (gated on RFC 0031 P1; the 0018→0019 pairing — 0031 is the port, this is what makes "Forze owns the schedule" real)
- **Scope:** A `forze_kits/integrations/pipeline/` kit that turns RFC 0031's premise into working machinery: a `schedules` document read model (cron, pipeline, params, enabled), a durable-cron tick that fires matured schedules per tenant, a `runs` read model fed by polling or inbox, deterministic idempotency keys derived from the scheduled instant, a reconciliation pass that finds orphaned triggers on backends without idempotency support, cancel-intent tracking, and cadence validation against per-backend platform floors. **Zero new ports** — kit and doctrine over the 0031 contract, in the RFC 0019 shape.
- **Related:** RFC 0031 §2.4 fixes this box's shape and §3.3 assigns it the cancel-intent job. RFC 0019 is the structural precedent (a scheduled kit over a plane whose ports shipped without automation) and §3 below **deliberately inverts its idempotency-key decision**, with the reason. The durable tier's `DurableScheduler` is the tick; the operation-progress kit projects run progress; durable run control supplies cancellation semantics; the inbox contract is the ingestion path RFC 0031 §5 pointed at. `DistributedLockScope` is the compensating control in §4.
- **Origin:** RFC 0031 deliberately ships a port with no scheduler attached, and the premise it rests on — Forze owns the schedule — is inert until something owns cron, tenancy fan-out and run bookkeeping. The failure modes here are not the port's: a tick that fires twice, a tick that fires zero times after an outage and quietly resumes, a run triggered but never recorded, a cadence that violates a platform's minimum interval. Each is invisible from inside the port and each produces wrong data rather than an error.

---

## 0. The kit splits along RFC 0031's mode line

RFC 0031 §4.1 makes the plane serve two deployments — `MANAGE` (forze owns the schedule) and `OBSERVE` (someone else does). This kit divides along the same line, and the division is clean because the two halves were always separable:

| Half | `MANAGE` | `OBSERVE` |
|---|---|---|
| Schedules document + tick (§1–§4, §6) | ✅ the whole point | ✗ nothing to schedule |
| Runs read model + ingestion (§5) | ✅ | ✅ **the freshness observable** |

The observation half is not a degraded mode. For an externally-owned pipeline it is what answers *"has the job that builds my Gold table run today, and did it succeed?"* — the causal half of RFC 0029 §2.1's staleness question, populated by polling `find_runs` on a tick that triggers nothing.

**The back door is closed here:** attaching a `PipelineSchedule` to an `OBSERVE` route is refused at wiring. Without that refusal, a deployment could take the observe mode to skip RFC 0031 §2.1's attestation and then schedule the pipeline from forze anyway — reintroducing the double run through the one path the gate does not watch.

## 1. Shape

```python
PipelineSchedule(            # a document, tenant_aware — this is the point
    pipeline="cdm_build",    # PipelineSpec.name, not a backend address
    cron="0 3 * * *",
    params={...},            # validated against the spec's params model at write time
    enabled=True,
)
```

Schedules are ordinary documents, so tenancy, audit, soft-delete and the query DSL come free. That was the original argument for putting the schedule in Forze rather than in DAG files, and it is worth restating as a property rather than a preference: **a tenant's schedule is data, subject to the same isolation as the rest of their data**, instead of a naming convention in someone else's repository.

Params validate **at write time** against the bound `PipelineSpec.params` model, not at fire time. A schedule that would fail every night at 03:00 should be rejected when it is saved, by the person saving it.

## 2. The tick

A durable-cron parent run enumerates tenants and fans out one child per (tenant, matured schedule) — the `CredentialSweeper` shape RFC 0019 also borrowed. Each child resolves the pipeline port for its tenant and calls `trigger`.

**Missed ticks do not catch up, and this is the important default.** If the worker is down for three hours under an hourly schedule, the next tick fires **once** and records the skipped instants; it does not fire three times.

The reasoning is specific to this plane rather than general scheduler taste: since RFC 0031 §2.2 moved the incrementality window into parameters, a catch-up run needs the *right* window, and the kit would have to invent it. Inventing windows silently is how a system produces gaps (each catch-up uses "now") or overlaps (each uses its own instant, against a pipeline that is not idempotent). The honest path for genuinely missed work is the one RFC 0031 §2.3 already named: an explicit loop of `trigger` calls with explicit windows, written by someone who knows which windows matter. A `catch_up` policy is recorded as demand-gated, and if it is ever built it must require an explicit window-derivation function rather than defaulting to one.

Skipped instants are **recorded, not silent** — the schedule document carries `last_fired_at` and a skipped count, so "why did last night's build not run" has an answer in the read model rather than in worker logs.

## 3. Idempotency: a deterministic key, inverting RFC 0019 on purpose

Each triggered run carries a key derived from the scheduled instant:

```
f"{tenant}:{pipeline}:{scheduled_at:%Y%m%dT%H%M}"
```

RFC 0019 decided the opposite for its maintenance sweeps — **children carry no idempotency key**, because a sweep pass that dedups itself into silence after a crash is worse than one that runs twice. Both decisions are right, and the difference is the operation, not the convention:

| | RFC 0019 sweep | This kit's trigger |
|---|---|---|
| Re-running is | harmless (idempotent maintenance) | **harmful** (a second pipeline run over the same window) |
| Therefore | no key — always re-run | deterministic key — never re-fire the same instant |

Stating both in one table is the point: a reader who has internalised 0019 should not read this as an inconsistency, and a future RFC should ask which column its operation is in rather than copying whichever precedent it saw first.

On Airflow the key is real — `dag_run_id` is caller-assigned and the backend rejects duplicates. On Airbyte and Dagster it is not (RFC 0033), and §4 is what the kit does about that instead of pretending.

## 4. Crash ordering, and a compensating control that does not lie

**Persist the intent, then trigger, then record the handle.** A run intent is written to the `runs` document *before* the port is called.

This is deliberately the **opposite order** to RFC 0030's watermark rule, and the pair is worth holding together because it shows the rule is derived rather than arbitrary:

- **RFC 0030**: advance the watermark *after* the swap — an early advance silently **skips** data, and silence is the worse failure.
- **Here**: persist the intent *before* the trigger — a late persist means a crash leaves a run nobody knows about, and the next tick fires it **again**. Duplication is the worse failure.

Different danger, opposite order, same method for choosing.

A crash between persist and trigger leaves an intent with no run, which is the benign direction — and the **reconciliation pass** closes it: on each tick, intents without a handle are matched against `find_runs(since=…)`. Where the backend honors idempotency keys, matching is exact. Where it does not, matching is heuristic (window plus time proximity) and is **reported, never auto-resolved** — an unmatched intent surfaces in the read model for a human, because a heuristic that silently decides "this run is that intent" would fabricate lineage.

For backends without native idempotency, the kit additionally takes a `DistributedLockScope` on the key across the persist-and-trigger window. Its limits are stated rather than implied: a lock lease can expire under a slow trigger call, so this narrows the duplicate window without closing it. **It does not change `supports_idempotency_key`** — the port keeps reporting what the backend can actually promise, and a compensating control in a kit is never allowed to rewrite a capability flag. Anything else would make the capability model unreliable everywhere it is consulted.

## 5. The `runs` read model

Per run: handle, state, `triggered_at` / `started_at` / `finished_at`, attempt, error, the originating schedule and window, `cancel_requested_at`.

This half runs in **both** RFC 0031 modes (§0). Under `OBSERVE` there is no `triggered_at`, no originating schedule and no window — the record is populated purely from what the orchestrator reports, which is exactly the shape a freshness consumer needs.

Two ingestion modes, both feeding the same projector:

- **Polling** (default, works everywhere): the tick refreshes non-terminal runs via `status`. Requires no public endpoint, which is why it is the default rather than the fallback.
- **Inbox** (opt-in): orchestrator callbacks/webhooks arrive through the existing inbox contract — RFC 0031 §5 declined to model the reverse direction precisely because inbox already owns it. Lower latency, at the cost of an exposed endpoint and its authentication.

`cancel_requested_at` is what makes RFC 0031 §3.3 work: on Airflow a cancelled run is reported honestly as `FAILED` by the adapter, and the read model — which knows a cancel was requested — presents `CANCELLED`. **The port never lies; the kit adds the context the port lacks.** That division is the whole reason the field lives here.

Operation-progress job records project run progress for UI; terminal transitions ride durable `stage`, ticks ride ephemeral `publish`, per the established split.

## 6. Cadence validation

Platform floors are real and are not the adapter's business — Airbyte Cloud enforces a minimum sync interval measured in hours, and a cron that violates it produces rejected or coalesced runs the caller never asked about. Validation belongs **at schedule write time**, where the person setting `*/5 * * * *` is present to be told no.

The kit therefore carries a per-backend `min_interval` in its route config, checked when a schedule is saved and again at wiring. Backends without a floor declare none. The check is cheap and its absence is the kind of gap discovered through a support ticket.

## 7. Out of scope

- **Dependency edges between pipelines.** RFC 0030 §5's fence applies verbatim and for the same reasons; this kit fires independent schedules and does not order them. A pipeline that must follow another says so *inside* its own definition, on the remote side, where the orchestrator already has that vocabulary.
- **Catch-up / missed-interval backfill** (§2), demand-gated with a hard requirement attached.
- **A schedule UI, calendars, holiday policies, timezone-aware business calendars.** Cron plus a timezone field; anything richer is a product feature.
- **Re-deriving run outputs.** RFC 0031 removed `outputs` from the contract for reasons that do not change here.

## 8. Acceptance battery

1. Tick fires a matured schedule exactly once per instant; running the tick twice in the same minute produces one run (deterministic key), and on a backend without key support the lock plus reconciliation still yields one — **or a reported unmatched intent, never a silent second run**. *(mock + real Airflow)*
2. Missed ticks: worker down across three instants → one run on resume, skipped instants recorded on the schedule document. *(mock)*
3. Crash between persist and trigger → intent without handle; reconciliation matches it exactly where keys are supported and **reports** it where they are not. *(DST)*
4. Crash after trigger before recording the handle → no second run on the next tick (the ordering claim, pinned; the inverted implementation fails this test). *(DST)*
5. Params validate at schedule-write time against the spec model; an invalid schedule cannot be saved. *(unit)*
6. Tenant fan-out: N tenants → N children; one tenant's failure does not stop siblings; a crashed child recovers on the next tick. *(mock)*
7. Cancel intent: `cancel` recorded, Airflow reports `FAILED`, the read model presents `CANCELLED`, and the port's own return value is **unchanged** — the kit adds context without rewriting the port. *(real Airflow)*
8. Cadence: a cron below a declared `min_interval` is refused at write time and at wiring. *(unit)*
9. Both ingestion modes converge to the same terminal state for the same run. *(mock ≡ real)*
10. RFC 0031 §2.1 re-attestation on the tick: a remote definition that gains a schedule after startup is detected and the forze schedule is disabled rather than left double-firing. *(real Airflow)*
11. **Mode split**: attaching a schedule to an `OBSERVE` route is refused at wiring; the runs read model populates under `OBSERVE` from polling alone, with no `triggered_at` and no window. *(unit + real Airflow)*

## 9. Phases

- **P1** — schedules document + tick + deterministic keys + persist-before-trigger + polling ingestion + runs read model + battery 1–6, 9.
- **P2** — reconciliation + lock compensating control + cancel intent + cadence validation + battery 3, 7–8.
- **P3** — inbox ingestion + job-record projection + tick re-attestation + battery 10.

## 10. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Kit and doctrine only, zero new ports — automation over RFC 0031's contract (the 0019 posture) | locked |
| 2 | Schedules are tenant-aware documents; params validate at **write** time, not fire time | locked |
| 3 | **No catch-up on missed ticks** — the window is a parameter, so a catch-up run needs a window the kit would have to invent, and inventing windows silently produces gaps or overlaps. Skips are recorded; explicit backfill is a loop of triggers | locked |
| 4 | Deterministic idempotency key from the scheduled instant — **the deliberate inverse of RFC 0019 decision 4**, because re-firing a pipeline is harmful where re-running a maintenance sweep is not | locked |
| 5 | Persist intent → trigger → record handle; the **opposite** of RFC 0030's watermark ordering, because here duplication is the worse failure and there silent skipping is | locked |
| 6 | The lock is a compensating control with stated limits and **never changes `supports_idempotency_key`** — a kit may not rewrite a capability flag | locked |
| 7 | Unmatched intents on keyless backends are **reported, never auto-resolved** — a matching heuristic that decided silently would fabricate lineage | locked |
| 8 | Polling is the default ingestion mode (no endpoint required); inbox is opt-in and reuses the existing contract | locked |
| 9 | `cancel_requested_at` lives in the kit so the adapter never fakes a state the backend cannot express | locked |
| 10 | Cadence floors validated at schedule-write time, not in the adapter | locked |
| 11 | Dependency ordering between pipelines stays out — RFC 0030 §5's fence, restated | locked |
| 12 | The kit splits along RFC 0031's mode line: schedules and tick are `MANAGE`-only, the runs read model serves **both** and is the freshness observable for externally-owned pipelines (RFC 0029 §2.1's causal half) | locked |
| 13 | Attaching a schedule to an `OBSERVE` route is refused at wiring — otherwise observe mode becomes a path around the §2.1 attestation that reintroduces the double run | locked |
