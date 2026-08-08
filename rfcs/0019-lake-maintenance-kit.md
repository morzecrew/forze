# RFC 0019 — Lake maintenance kit: scheduled table health

- **Status:** 📝 Draft (gated on RFC 0018 P2; the rotating-store→proactive-refresh pairing — 0018 is the store, this is the liveness half)
- **Scope:** A `forze_kits/integrations/lake/` maintenance kit that keeps Iceberg tables healthy **on a schedule**, because RFC 0018 deliberately ships mechanisms without automation: durable-cron runners driving the `LakeMaintenancePort` in the correct order (expire → orphans → compact), per-tenant fan-out in the `CredentialSweeper` shape, a table-health read model, and operation-progress job records for visibility. No new ports — this RFC is entirely kit + doctrine.
- **Related:** RFC 0018 (`LakeMaintenancePort`, the compaction honesty §6). The proactive credential-refresh sweeper (the exact precedent: a plane whose correctness half shipped first and whose *liveness* half is a scheduled sweeper, because the failure mode is slow accumulation no on-demand path ever sees). The operation-progress kit (its job records project the runs). The durable-cron tier (`DurableScheduler` over the self-hosted durable execution tier).
- **Origin:** Iceberg's transactional model has a maintenance tax: snapshots, metadata, small files and orphaned data files accumulate monotonically; pyiceberg is copy-on-write with no auto-compaction, so an upsert- or micro-batch-heavy table degrades *silently* — queries slow down and storage grows, with no error ever raised. Exactly like credential idleness (the proactive-refresh precedent), the pathology is invisible to the request path by construction; only a scheduled control-plane pass can see it coming.

---

## 1. The runbook, encoded

The maintenance literature converges on one ordering, and the kit encodes it rather than documenting it: **(1) expire snapshots → (2) remove orphan files → (3) compact**. Expiry dereferences files so orphan cleanup can reclaim them; compacting before expiring rewrites files that expiry would have dropped anyway. One durable function per table per pass, steps journaled individually, so a crash resumes mid-runbook instead of restarting it.

```python
LakeMaintenancePolicy(
    expire_older_than=timedelta(days=7),
    retain_last=5,                        # floor — never below, regardless of age
    orphan_older_than=timedelta(hours=72),# safety buffer vs in-flight writes
    orphan_dry_run_first=True,            # first pass per table reports, never deletes
    compact_small_file_threshold=0.5,     # fraction of files under target size that triggers
    compact_target_file_size=512 * 2**20,
    cron="0 3 * * *",
)
```

Safety doctrine, written into defaults (the "mechanism without the default" audit theme, again priced in):

- **`retain_last` is a floor, not a suggestion** — expiry never reduces below it, because time-travel readers and the 0018 `commit_ref` lookback both depend on recent history existing. The known race is named in the docstring: expiring a snapshot a long-running engine query is reading breaks that query; the retention floor plus the age threshold are the mitigation, and "expire aggressively" is documented as an operator decision with that failure spelled out.
- **Orphan removal is dry-run-first per table**: the first pass reports candidate bytes; deletion requires the policy to have seen a prior report (recorded in the health read model). A 72-hour minimum age is enforced, not defaulted — orphan-looking files can be in-flight commits.
- **Compaction skips hot partitions**: a partition written within `compact_cooldown` (default 1h) is skipped this pass — `compact_partition` is rewrite-by-overwrite (0018 §6) and racing a live writer buys a guaranteed conflict for nothing.

## 2. Fan-out and visibility

- **Per-tenant sweep in the sweeper shape**: a parent durable-cron run enumerates tenants (namespace resolver / `list_tenants`), enqueues one child run per (tenant, table spec); children carry no idempotency key (the proactive-refresh lesson — a sweep pass must re-run after a crash, not dedup itself into silence).
- **Table-health read model** (document spec, projector-owned): per table — snapshot count, small-file ratio, last-maintenance timestamps, orphan bytes last reported/reclaimed, last compaction outcome. This is what dashboards and the upsert-footgun warning (0018 §6) point at: "your upsert route's table has 40k files" is a number on a document, not a surprise in a query plan.
- **Operation-progress job records** project run progress (per-table fraction across the fan-out); terminal states ride durable `stage`, ticks ride ephemeral `publish` — the established split.

## 3. Out of scope

Manifest-rewrite/whole-table optimization (engine/operator territory, per 0018 §6); auto-tuning of policies (thresholds are config, not ML); reacting to catalog events (poll-per-cron is the v1 cadence; a catalog change feed is recorded if a REST-spec eventing story matures); cross-table global scheduling optimization (each table's runbook is independent).

## 4. Acceptance battery

1. Runbook ordering pinned: a pass on a degraded mock table performs expire → orphan → compact in order; crash after step 1 resumes at step 2 (journal proof).
2. `retain_last` floor holds under an aggressive `expire_older_than`; an `as_of` read of a retained snapshot still works after the pass.
3. Orphan dry-run-first: first pass reports and deletes nothing; second pass deletes only what a report covered; sub-72h files never touched. *(mock ≡ real Lakekeeper+MinIO — orphan semantics are exactly where the mock could lie)*
4. Hot-partition skip: a partition with a recent write is skipped; the health record says so.
5. Fan-out: N tenants → N child runs; a crashed child re-runs on the next pass (no idempotency-key silence); one tenant's failure doesn't stop siblings.
6. Health read model converges with real table state after each pass (real leg).
7. Compaction trigger: small-file ratio above threshold compacts, content-identical checksum; below threshold, no-op with a recorded reason.

## 5. Phases

- **P1** — runbook function (expire + orphans with dry-run discipline) + policy + per-tenant fan-out + health read model + battery 1–3, 5–6.
- **P2** — compaction pass (threshold/cooldown) + job-record projection + battery 4, 7.

## 6. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Kit + doctrine only; zero new ports — automation over 0018's `LakeMaintenancePort` | locked |
| 2 | Runbook order (expire → orphans → compact) is code, not documentation; steps individually journaled | locked |
| 3 | Safety defaults ship on: retain floor, 72h orphan age enforced, dry-run-first, hot-partition cooldown | locked |
| 4 | Sweep children carry no idempotency key (the proactive-refresh precedent) — crashed passes re-run, never self-dedup | locked |
| 5 | Health read model is the observable; the upsert growth warning points at it | locked |
| 6 | Poll-per-cron cadence v1; catalog eventing recorded, unpromised | locked |
