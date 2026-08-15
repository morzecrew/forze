# RFC 0030 — Materialization publish: build → assert → swap

- **Status:** 📝 Draft (RFC 0028 family tail; needs 0028 for the transform and 0029 for the staging relation)
- **Scope:** The kit that makes a warehouse rebuild **safe to re-run and impossible to half-publish**: a durable runbook — stage → assert → atomically swap → record — over the ports 0028 and 0029 provide, plus a small fixed assertion vocabulary, a per-relation **atomicity capability matrix** that fails closed, and a build read model carrying freshness, row counts, assertion outcomes and the incremental watermark. Zero new contract *planes*; one capability and one swap primitive land on the 0029 admin port. Lake tables ride the same runbook through their existing 0018 ports, which is the payoff for having written that contract format-neutral.
- **Related:** RFC 0019 is the structural precedent — a runbook whose ordering is *encoded, not documented*, with steps individually journaled so a crash resumes mid-runbook. RFC 0018 decision 5 (`overwrite_partition` = the blessed idempotent pipeline primitive) is the same idea at the lake plane, and this RFC generalizes it rather than competing with it. RFC 0018's out-of-scope list — no multi-table transactions, no Nessie-style branching — is what this RFC answers *without* branching. The operation-progress kit projects run progress; durable run control supplies cancellation; the self-hosted durable tier's steps supply the journal. RFC 0028 §5 explicitly defers idempotency here.
- **Origin:** A warehouse rebuild is the operation most likely to be interrupted and most damaging when it is. Today forze offers no answer at all: a transform is one at-least-once statement (0028), the relation it writes has no staging twin (0029 creates relations but does not sequence them), nothing checks the result before it becomes visible, and nothing records that a build happened. Every consumer therefore reinvents the same four steps, usually without the atomicity — and a partially-published Gold table is the failure mode that produces confidently-wrong dashboards, which is worse than an outage because nobody pages for it.

---

## 1. The runbook, and why it is code

```
stage    → build into a staging relation (0028 procedure route / 0018 lake write)
assert   → evaluate declared checks against the STAGED relation
swap     → atomically replace the live relation; on assertion failure, never runs
record   → build outcome, row counts, assertion results, watermark advance
```

The ordering is not advice. Assertions run against staging *because* that is what makes a failed check a no-op rather than an incident — the live relation is untouched and yesterday's data keeps serving. Swap is last *because* it is the only irreversible step. The watermark advances after the swap commits, never before, so a crash re-processes rather than skips. Each step is an individually journaled durable step, so a crash resumes at the failed step instead of rebuilding from scratch.

This is the write-audit-publish pattern, and it is worth naming that RFC 0018 recorded WAP as unavailable — Nessie-style branching is out of scope there, so a lake table had no staging story. **Staging relation plus atomic swap delivers WAP without branching**, on every engine whose capability reaches, which is the substantive thing this RFC contributes beyond sequencing.

## 2. Atomicity is a capability, and the engines genuinely differ

This is where an abstraction would most tempt one to lie, so the matrix is explicit and fail-closed. **Claims to verify at pickup** — each row changes what a consumer may declare.

| Engine | Atomic scope | Mechanism |
|---|---|---|
| Postgres | **multi-relation** | DDL is transactional — rename several relations in one transaction (brief `ACCESS EXCLUSIVE` lock, named as the cost) |
| ClickHouse | **multi-relation** (pairwise) | `EXCHANGE TABLES … AND …`; `REPLACE PARTITION` for the partition scope |
| DuckDB | **multi-relation** | transactional DDL |
| BigQuery | **relation** | `CREATE OR REPLACE TABLE … AS SELECT`, or a truncating copy job — atomic per table, no cross-table story |
| Iceberg (0018) | **relation** / **partition** | catalog CAS commit; `overwrite_partition` for the partition scope |

```python
class PublishAtomicity(StrEnum):
    NONE = "none"                    # cannot promise; publish refuses to wire
    PARTITION = "partition"
    RELATION = "relation"
    MULTI_RELATION = "multi_relation"
```

A materialization **declares the scope it needs** and wiring refuses an adapter that cannot meet it — the 0021/0015 gate shape. A fact-plus-dimensions publish that needs consistent cross-table visibility declares `MULTI_RELATION`, works on Postgres and ClickHouse, and is **refused at freeze time on BigQuery** with a message saying so. That refusal is the feature: today the same design silently ships a visible skew window on BigQuery and nobody finds out until a dashboard sums a fact against a dimension that has not landed yet.

`NONE` has no spelling that lets a publish proceed. An engine that cannot swap atomically cannot host this runbook, and the honest answer is to say so rather than to offer a best-effort rename with a comment about the race.

## 3. Assertions: a fixed vocabulary, not a rules engine

The gate must be expressive enough to catch the failures that actually happen and small enough that it cannot grow into a data-quality product. The vocabulary is closed:

```python
assertions=[
    RowCount(min=1),                                   # empty rebuild = the classic silent disaster
    RowCountDelta(max_decrease=0.10),                  # source dropped 90% of rows upstream
    NotNull("tenant_id", "order_id"),
    Unique("order_id"),
    AcceptedRange("amount", min=0),
    Freshness("updated_at", max_age=timedelta(hours=26)),
]
```

Each compiles to a query **the adapter builds** from structured input — the same "structured, never strings" doctrine that governs the querying DSL, 0018's `LakeFilter`, and 0015's refusal to parse. There is no predicate DSL and no author-supplied assertion SQL inline.

The escape valve, for the check the vocabulary cannot express, reuses machinery rather than adding a hatch: **a registered analytics query or 0028 procedure route returning a single scalar, referenced by name, compared against a declared bound.** Registered, reviewed, greppable, capability-limited — it is a named route like any other. `expression="…"` never appears in a config.

`RowCountDelta` deserves its own note: comparing against the previous build is the check that catches upstream breakage, and it is only possible because §4 keeps build history. It is the reason the read model is in this RFC rather than deferred to a follow-up.

## 4. Build state: one read model, three jobs

A projector-owned document read model per materialization, generalizing RFC 0019's table-health record (which stays the Iceberg-physical view; this is the logical-build view).

**Scope caveat, and it is load-bearing: this is the *builder's* view.** The record exists because *this service* published the relation, and it is written by this runbook. For a relation someone else owns — a dbt mart, a DLT gold table — no publish ever runs here and this read model stays empty forever. Freshness for those relations comes from **RFC 0029 §2.1's `observe_relation`**, which reads the engine's own metadata instead of forze's records. The two are complementary and must not be conflated: a service reading a mix of owned and external relations gets freshness for both, from two different sources, and the source is on the value object precisely so a dashboard can tell which is which. This RFC does not attempt to synthesize a build record for a build it did not perform.

With that boundary drawn, the record's three jobs:

- **Freshness** — when the relation was last successfully published, which is the number every downstream consumer and dashboard actually wants and which nothing in forze can answer today.
- **Assertion history** — outcomes per build, so a failing check has a trend rather than a single alarm, and so `RowCountDelta` has a baseline.
- **Watermark** — the high-water mark for incremental builds. `grep watermark` over `src/` today returns nothing relevant; every incremental consumer invents this.

**The watermark rule is the only subtle part and it inverts the credentials-plane rule for a reason.** The rotating credential store persists a credential *before* use because losing it is fatal. Here the danger is the opposite: advancing a watermark before the swap commits means a crash *skips* data permanently, which is silent and unrecoverable without a manual backfill. So the watermark advances **only after the swap commits**, and the consequence is at-least-once reprocessing on crash — which the replace-shaped publish absorbs harmlessly. Recorded as a decision, and battery-pinned, because the wrong ordering here is invisible in testing and catastrophic in production.

**Backfill needs no engine.** A partition-scoped publish is idempotent by construction (`REPLACE PARTITION` / `overwrite_partition` / a partition-predicated swap), so backfilling is "run this materialization for partition P" — a parameter, not a subsystem. Stating that here is a deliberate foreclosure: backfill orchestration is the single most common way a publish tool becomes a platform.

## 5. Dependency ordering — the section that has to say no

A warehouse has edges: Gold depends on Silver. Declaring them is tempting, cheap, and the exact point at which this family could stop being a seam. So the boundary is drawn narrowly, and the workstream is **demand-gated behind a named consumer** rather than shipped with P1.

**In scope, if built:** a `depends_on` declaration between materializations in one service's wiring; topological ordering *within a single durable run*; cycle refusal at freeze time via the existing `check_wiring` dry-run, which is where a cyclic dependency should be caught and is a capability forze already has.

**Permanently out of scope**, each for a reason rather than a shrug:

- **A scheduler.** Durable cron already schedules; a second scheduling vocabulary would be a competing one.
- **Cross-service or cross-repo graphs.** The moment the graph spans deployments it needs a registry, a UI and an owner — that is Airflow/Dagster, and they are good at it.
- **Lineage by SQL parsing.** Declared edges are wiring, which forze owns. Inferred edges require parsing statements the framework has committed — on five separate planes — never to parse.
- **Per-node retry/alert/SLA policy.** Durable execution and the observability plane own those. A per-node policy layer is a scheduler wearing a hat.

The review test, applied honestly: *would a data team adopt this instead of dbt or Airflow?* No — there is no compilation, no ref resolution, no scheduling semantics, no backfill engine, no catalog, no UI, and it cannot see anything outside one service's wiring. It orders three materializations inside one durable run. If a future revision makes that answer even arguable, the revision is wrong.

## 6. Where this lives, and one rejected design

The runbook is a **kit** (`forze_kits/integrations/materialization/`) composing ports it is handed: 0029's admin port plus a 0028 procedure route for warehouse relations, or 0018's catalog/write ports for lake tables. Only two things land on a contract: `PublishAtomicity` and a `swap_relations` primitive on `AnalyticsAdminPort`, both because they need engine implementations.

**Rejected: a shared `RelationPublishPort` implemented by both `contracts/analytics` and `contracts/lake`.** It looks cleaner and it would force two planes to share a vocabulary neither owns — a lake swap is a catalog commit against a snapshot log, a warehouse swap is a DDL rename under a lock. The kit branching on which ports it holds is the smaller lie, and it matches RFC 0019's "kit + doctrine only, zero new ports" posture. Recorded because it is the first refactor a reviewer will propose.

## 7. Acceptance battery

1. Happy path end-to-end: stage → assert → swap → record; the live relation shows new content only after the swap, and the build record carries row count, duration, assertion outcomes. *(mock ≡ real, each engine)*
2. **Assertion failure never publishes**: a failing `RowCount(min=1)` leaves the live relation byte-identical to its pre-run state, the run fails with a mapped code, and the failure is recorded with the failing check named. *(real — the central claim)*
3. Crash between stage and swap: replay resumes at the un-run step, publishes once, produces no duplicates. Crash *after* swap but before the watermark write: the next run reprocesses the overlap and converges to identical content. *(DST forced schedules + real leg)*
4. **Watermark ordering pinned**: an injected fault proves the advance happens strictly after swap commit — a deliberately inverted implementation fails this test. *(DST)*
5. Atomicity refusal: a materialization declaring `MULTI_RELATION` fails at freeze on BigQuery with its code, and wires on Postgres/ClickHouse. *(unit + real)*
6. Concurrent readers observe no partial state across the swap — a reader polling throughout a multi-relation swap sees only the before or after set, never a mix. *(real PG + CH)*
7. Each assertion type: true and false cases, plus the named-route escape valve compared against its bound. *(mock ≡ real)*
8. `RowCountDelta` against the previous build's record; first-ever build (no baseline) is a pass with a recorded reason, not a crash. *(mock)*
9. Partition-scoped re-publish twice → identical logical content, second run replaces rather than doubles (the backfill claim, pinned). *(real, all partition-capable engines)*
10. Lake parity: the same runbook over 0018's ports publishes an Iceberg table, assertions included — the format-neutrality claim proven by reuse rather than asserted. *(real, 0018's Lakekeeper+MinIO fixture)*
11. Cancellation (durable run control) during stage leaves no partial publish; progress (the operation-progress kit) reports step-level fractions. *(mock)*

## 8. Phases

- **P1** — runbook (stage/assert/swap/record) + `PublishAtomicity` + `swap_relations` + the assertion vocabulary + build read model + ClickHouse & Postgres + battery 1–8.
- **P2** — BigQuery (relation scope) + lake backend + partition scope + battery 9–10.
- **P3** — dependency ordering, **demand-gated on a named consumer** with §5's fences; battery 11.

## 9. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Runbook order (stage → assert → swap → record) is code, individually journaled; assertions evaluate the **staged** relation so a failure is a no-op | locked |
| 2 | Staging + atomic swap = write-audit-publish **without branching** — answers what 0018 recorded as unavailable | locked |
| 3 | `PublishAtomicity` is declared, fail-closed at wiring; `NONE` has no spelling that permits a publish. BigQuery's relation-scope ceiling is a freeze-time refusal, not a documented caveat | locked |
| 4 | Assertion vocabulary is closed and adapter-compiled; the escape valve is a **named registered route**, never inline SQL | locked |
| 5 | Watermark advances strictly **after** swap commit — at-least-once reprocessing over silent skips; the credentials-plane ordering deliberately inverted, with the reason recorded | locked |
| 6 | Backfill = a partition parameter on an idempotent publish, never a subsystem | locked |
| 7 | Dependency ordering demand-gated, single-service, declared-not-parsed, freeze-time cycle refusal; scheduler / cross-service graph / lineage inference / per-node policy permanently out | locked |
| 8 | Kit + two contract additions; a shared cross-plane `RelationPublishPort` considered and rejected (lake and warehouse swaps are not the same operation) | locked |
| 9 | Build read model is in this RFC, not deferred — `RowCountDelta` and freshness both depend on build history existing | locked |
| 9a | The build record is explicitly the **builder's** view; freshness for externally-owned relations is RFC 0029 §2.1's engine-metadata observation, never a synthesized build record. Added after the topology audit — the original draft left this implicit and would have read as full freshness coverage | locked |
| 10 | Engine atomicity claims in §2 are drafting-time and **must be re-verified at pickup** | recorded |
| 11 | **The demand gate now has a competitor, and it is the closest one this RFC will meet.** SQLMesh's virtual environments implement build → audit → atomic swap directly, which is this RFC's runbook under other names; where a spec compiler emits SQLMesh models, the publish is already owned and building it here would be a second implementation of the same protocol over the same relations. **The compiler is not the publisher, and the distinction decides the boundary:** `morzecrew/bloomery` compiles specs into SQLMesh/dbt/Cube artifacts and executes nothing — SQLMesh is what publishes them, through its own configured engine. So the overlap is with SQLMesh alone, and only for the models it is given. What survives is the part that never reaches it: forze-owned materializations that no compiler emits, and lake tables publishing through RFC 0018's ports — unambiguously ours, since neither tool implements `LakeCatalogPort` or `LakeWritePort`, and since this RFC records a `PublishAtomicity` ceiling SQLMesh does not model. Treat "which tool owns the swap for *this* relation" as a **precondition of pickup**, not a detail of execution — the answer decides whether P1 is the whole RFC or only its lake half | recorded (2026-08-15) |
| 12 | **Open, and deliberately not designed here.** Review proposed making 11 enforceable — a per-relation ownership declaration (compiler-managed / forze-managed / RFC 0018 lake) with a freeze-time check refusing overlapping writers, in the `check_wiring` shape RFC 0029's `verify` already uses. The problem is real: 11 as written is guidance a reader can silently ignore, and two publishers on one relation is the failure it would prevent. It is recorded rather than specified because designing a new wiring-time mechanism belongs in this RFC's own revision with its own rationale — not appended during a documentation-alignment pass, where it would arrive as a locked decision nobody argued for. Settle it at pickup, alongside 11 | open (2026-08-15) |
