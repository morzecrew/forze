# RFC 0028 — Warehouse transform reach: the procedure plane on analytical engines

- **Status:** 📝 Draft (family head: 0029 = managed relations, 0030 = publish protocol)
- **Scope:** Make the **already-shipped** procedure plane reach the engines a warehouse actually runs on. `ProcedurePort`/`ProcedureSpec` exist and are governed exactly right — registered SQL in wiring, typed params, command-plane write-guard — and have **one backend**: Postgres (`src/forze_postgres/adapters/procedure/adapter.py`), plus the mock. **(W1)** ClickHouse adapter, **(W2)** BigQuery adapter, **(W3)** DuckDB adapter, each with a fail-closed `ProcedureCapabilities` matrix stating what its engine can and cannot promise. **(W4)** a shared **route resource policy** — RFC 0015's limit vocabulary lifted off the dynamic-read plane onto registered routes, because a registered statement burns money and wall-clock exactly like a runtime-authored one. No new plane, no new contract concepts: this RFC is adapter breadth plus one honesty matrix.
- **Related:** RFC 0015 (the limits vocabulary W4 reuses verbatim: mandatory positive timeout, caps ship on, no `unlimited` spelling) and RFC 0016 (the per-engine doctrine-triage shape this RFC copies — enforcement matrix, not uniform code). RFC 0010 is the original triage precedent. RFC 0020 **W2** ("engine-side lake writes are procedures-shaped registered statements if ever built") is **absorbed here** — that workstream is a DuckDB procedure route against an attached catalog, which is W3 plus 0020 W1, not a separate design. RFC 0030 owns idempotency and atomicity; §5 states the boundary so this RFC does not half-solve it.
- **Origin:** An upcoming DWH-shaped backend. A warehouse is a chain of transforms: `INSERT INTO gold SELECT … FROM silver`, `CREATE OR REPLACE TABLE … AS`, `ALTER TABLE … DELETE WHERE`, `OPTIMIZE`. Today forze can *read* BigQuery and ClickHouse through governed named queries and *append rows* to them through the ingest port — and cannot run a single governed transform against either. The only available path is the raw client port, which the escape-hatch policy correctly labels "you own validation, tenancy, portability." So the highest-value, highest-blast-radius statement in a warehouse app — the one that rewrites Gold — is the one statement outside every framework guarantee, while the *read* of the same table is fully governed. That asymmetry is invisible from the outside: the procedures plane presents as engine-neutral and is one-backend.

---

## 1. Why this is a seam and not a data platform

The standing worry — "don't become yet another data platform" — has a sharp test, and this RFC is the easiest case to apply it to:

> **Forze declares and validates; the engine decides and executes.**

A procedure route contains SQL the *author* wrote, stored in *wiring*, executed by the *engine*. Forze contributes: the params are typed and validated, the tenant is resolved into the namespace or bound as a parameter, the route is refused in a read-only operation, failures land in the shared taxonomy, the call is traced and DST-visible, and the whole thing is greppable at freeze time. Every one of those is something the framework already does for Postgres procedure routes and refuses to do for the same statement pointed at ClickHouse.

The platform test — *"would a data team install this instead of dbt?"* — comes back **no**, and must keep coming back no. There is no model graph here, no ref() resolution, no compilation, no scheduler, no catalog. A procedure route is one statement with typed params. That is a seam by construction.

**The coexistence clause, which applies to the whole 0028–0030 family:** an external transform tool owning the warehouse must stay a first-class deployment. If dbt or Spark builds Gold and the forze service only reads it, everything in this family is opt-in per route and nothing regresses. This is not a compatibility footnote — it is the constraint that keeps the family a seam, and every design decision below is checked against it.

## 2. What each engine can actually promise

The 0016 shape: triage by what the server enforces, ship an honest matrix, fail closed where the engine cannot keep the promise. **These are claims to verify at pickup** — they reflect the ecosystem as understood at drafting and each one is load-bearing enough that a stale fact changes an adapter.

| | Postgres (shipped) | ClickHouse (W1) | BigQuery (W2) | DuckDB (W3) |
|---|---|---|---|---|
| Multi-statement atomicity | ✅ real transaction (`in_transaction=True`) | ❌ none across statements | ❌ none (multi-statement scripts are jobs, not transactions) | ✅ transactional |
| Statement timeout | ✅ `SET LOCAL statement_timeout` | ✅ `max_execution_time` setting | ⚠️ job-level timeout, not statement-level | ⚠️ client-side only |
| Cost ceiling | ❌ n/a | ⚠️ memory/rows settings, not currency | ✅ `maximum_bytes_billed` (already on the client) | ❌ n/a |
| **Synchronous completion** | ✅ | ⚠️ **`ALTER … DELETE/UPDATE` are asynchronous mutations** | ✅ job wait | ✅ |
| Tenancy | schema (`query_schema`) | database (`query_database`) | dataset (`query_dataset`) | attached-DB / schema |

**The ClickHouse asynchronicity row is the finding that justifies a capability rather than a docs note.** A `run()` that returns after `ALTER TABLE … DELETE` has been *accepted* — while the mutation is still executing — is a silent lie of exactly the kind this codebase has been burned by before (the `wait_for` cancellation and sealed-field-sort incidents share the shape: a call that reports success for something that did not happen). The adapter's stance: mutation-shaped statements execute with synchronous mutation settings **on by default**, and an author who wants fire-and-forget sets it off explicitly and owns the consequence. A capability flag `synchronous_completion` records which engine/route combination actually waits, and the value is *observable*, not just documented.

```python
@attrs.define(frozen=True, kw_only=True, slots=True)
class ProcedureCapabilities:
    transactional: bool                 # multi-statement atomicity within one run()
    synchronous_completion: bool        # run() returns only after the effect is complete
    enforces_statement_timeout: bool    # server-side, not client-side abandonment
    enforces_cost_ceiling: bool         # engine refuses before spending, not after
```

Fail-closed reading, per the standing "built the mechanism, not the gate" theme: a route may **declare a requirement** (`requires_transactional=True`) and wiring refuses an adapter that cannot meet it — the pattern RFC 0021 uses for isolation tiers and 0015 uses for provenance. A route that declares nothing gets the engine's real behavior and the capability is still queryable for tests and dashboards.

## 3. W1 — ClickHouse

The cheapest adapter in the set: `ClickHouseClientPort.run_command` already exists (`src/forze_clickhouse/kernel/client/port.py`), so the adapter is param binding + tenancy resolution + settings + taxonomy mapping over a primitive that ships today.

```python
@attrs.define(frozen=True, kw_only=True, slots=True)
class ClickHouseProcedureConfig(TenantAwareIntegrationConfig):
    sql: str                                    # registered; `{param}` binding, never f-strings
    query_database: NamedResourceSpec | None = None   # tenant database, mirrors the analytics config
    max_execution_time: timedelta = timedelta(seconds=60)   # ships on, no unlimited spelling
    mutations_sync: bool = True                 # wait for ALTER…DELETE/UPDATE to finish
    settings: StrKeyMapping[str] = {}           # escape valve for engine settings, key-checked
```

Notes that shape the adapter: ClickHouse `INSERT` is not transactional across parts, so an interrupted `INSERT INTO … SELECT` can leave a partial result — this is *the* reason RFC 0030 exists and the reason a bare CH procedure route is documented as at-least-once with visible intermediate state. `OPTIMIZE TABLE … FINAL` is a legitimate procedure route and is long-running; the timeout default must not make it unusable, hence a per-route value rather than a plane-wide constant.

## 4. W2 — BigQuery, and W3 — DuckDB

**BigQuery.** `run_query` plus job handles already exist on the client, and `maximum_bytes_billed` is already threaded through `BigQueryQueryConfig` for reads — the procedure config inherits the same field, which makes W2 mostly a matter of *not* re-inventing what the read side already has. BigQuery has no transaction across a `run()`, so `requires_transactional` is refused at wiring for this adapter; `CREATE OR REPLACE TABLE … AS SELECT` is the atomic unit available and RFC 0030 is where that becomes a protocol rather than a convention. DML quota pressure is real and is a documented operational limit, not something the adapter can paper over.

**DuckDB.** Transactional and in-process, so it is the *most* capable of the three on paper and the most constrained in practice: the single-connection concurrency rule applies (one statement in flight; the pool lives outside), and a long transform holds the connection for its duration. Its value here is twofold — local/embedded warehouse work, and the absorbed 0020 W2: with 0020 W1's REST-catalog `ATTACH`, a DuckDB procedure route *is* the governed engine-side lake write, with no new design. That is worth stating loudly, because "we already have the mechanism" is a better answer than a fourth workstream.

## 5. The boundary this RFC does not cross

A procedure route is **one statement, at-least-once**. Run it twice and an append-shaped transform doubles rows. This RFC does not fix that, and deliberately does not pretend to:

- It cannot inspect the SQL to classify the write shape — that is string parsing, which this codebase has ruled out on every plane (`filters are structured, never strings`; `engine-enforced, never parsed`).
- A declared `write_shape="append" | "replace"` field that nothing verifies is the exact "built the mechanism, not the gate" anti-pattern the 7th-edition audit named. Declaring it here would be worse than omitting it.

So the honest split: **0028 gives you a governed statement; RFC 0030 gives you an idempotent publish.** The blessed shape for a transform that must be re-runnable is the 0030 protocol (stage → assert → atomic swap), which is idempotent *structurally* — the same reasoning that made `overwrite_partition` the blessed lake primitive in 0018 rather than trusting callers to dedup appends. Until then, idempotency for procedure routes is what it is for every command port: the durable step journal.

## 6. W4 — route resource policy

RFC 0015 built a good vocabulary and scoped it to one plane. The reasoning that produced it — *caps ship on with real values; there is no `unlimited` spelling; a caller wanting more sets a bigger number and owns it in review* — has nothing to do with statements being runtime-authored. A registered ClickHouse transform with a bad join burns the cluster identically.

W4 lifts the vocabulary, without lifting the plane:

- Procedure routes get a **mandatory positive timeout** with a real default on every engine that can enforce one; engines that cannot (`enforces_statement_timeout=False`) say so in the capability rather than accepting a value they will not honor.
- BigQuery routes get `maximum_bytes_billed` reachable from the procedure config, matching the read side.
- **Explicitly not built:** a cross-engine currency-normalized "budget" abstraction. Bytes billed, memory limits and wall-clock are not commensurable, and a `QueryBudget` type that pretends they are would be a lie with a nice API. Per-engine knobs with an honest matrix is the correct ceiling.

### 6.1 Read routes are in scope too — and are the weaker half today

The original scoping of W4 was procedure routes only. The topology audit corrected it: **analytics read routes have the mechanism and not the default.** `AnalyticsRunOptions["timeout"]` exists and is threaded correctly through the ClickHouse and BigQuery adapters (`_run_timeout`), but it is **per-call and defaults to `None`**, and no route config carries a default — `ClickHouseQueryConfig` is `sql` + `skip_total` + `cursor_column`, `PostgresQueryConfig` the same. So the shipped default for a registered analytics read is *unbounded*, and safety depends on every caller remembering an optional argument at every call site.

That is the standing "built the mechanism, not the default/gate" theme in currently-shipped code, and it bites hardest in the topology where the service reads relations whose size it does not control — an externally-owned warehouse, where a mart can grow by an order of magnitude without anyone telling the reader.

The fix is small and additive, and deliberately mirrors what BigQuery's read config already does for cost:

- A **route-level default timeout** on each engine's query config, shipping on with a real value; the per-call option clamps *down*, never up (0015's rule verbatim, and the reason it is a rule — a caller raising its own ceiling defeats the point of having one).
- No `unlimited` spelling. A route needing ten minutes says ten minutes and owns it in review.
- BigQuery's existing `maximum_bytes_billed` needs no change; it is the precedent, not an exception.

Explicitly **not** in scope: changing any default that would alter behavior for existing routes without a version note. Where a new default would newly bound a previously-unbounded route, the changelog says so and the value is chosen generously — a resource policy that surprises a deployment is worse than one adopted a release later.

### 6.2 Engine reach: Snowflake and Databricks SQL (recorded, no trigger)

The topology audit surfaced a coverage hole neither this RFC nor its siblings can close by editing: **the two most external-shaped warehouse platforms have no adapter at all.** `grep` finds no Snowflake anywhere and Databricks only as a *future* mention in RFC 0020 W4's Delta trigger. `deltalake` and `pyiceberg` in `pyproject.toml` are dev-only fixture writers — Delta and Iceberg are read at runtime through DuckDB extensions, which is a file-path read that uses neither platform's compute nor its access controls.

The consequence is specific rather than general: on Snowflake or Databricks, a forze service falls back to a raw client for its *reads* — the exact ungoverned posture this RFC exists to eliminate on the write side, and one where RFC 0029's `verify` mode (the whole external-topology story) has no port to run on.

Recorded, not built, with an honest price:

- The read half is the valuable half and is mostly conventional — a `forze_snowflake` / `forze_databricks` analytics adapter over the vendor's Python connector, plus the 0029 admin port. That is the shape every existing analytics package already has.
- The transform half is **not** worth chasing on Databricks: its transform tier is Spark/DLT and its orchestrator is Workflows, so a procedure adapter there would compete with the platform rather than seam into it. Snowflake is different — it is a SQL warehouse, so a procedure adapter is coherent there if a consumer wants one.
- **Trigger:** a named consumer whose warehouse lives on either platform. Absent that, building this is speculative breadth, and the family's value does not depend on it.

## 7. Acceptance battery

1. Typed params bind on every engine; a param not in the spec's model is refused; no SQL string ever crosses the port. *(mock ≡ each real engine)*
2. Write-guard: a procedure route resolved in a `QUERY` operation is refused, unchanged from the Postgres behavior. *(unit)*
3. Tenancy: two tenants, same route, disjoint effects — CH database, BQ dataset, DuckDB attached-db resolution. *(real)*
4. **ClickHouse synchronous mutation:** `ALTER … DELETE` with `mutations_sync=True` — the row count observed *immediately after* `run()` returns reflects the deletion; with it off, the capability reports `synchronous_completion=False` and the test asserts the un-waited behavior rather than hiding it. *(real CH — the mock cannot detect this and does not pretend to)*
5. Timeout fires on each engine that claims to enforce one, maps to the shared taxonomy, connection/session reusable afterwards. *(real)*
6. BigQuery `maximum_bytes_billed` exceeded → refusal *before* the spend, mapped code. *(real BQ)*
7. Capability refusals at freeze: `requires_transactional=True` on BQ/CH fails wiring with its code; non-positive timeout fails. *(unit)*
8. Partial-failure visibility: an interrupted CH `INSERT INTO … SELECT` leaves observable partial state — pinned as a **documented-limitation test**, so the 0030 motivation is an executable fact rather than an assertion. *(real CH)*
9. DuckDB procedure over a 0020 W1 attached catalog writes a lake table that `forze_iceberg` then reads — the absorbed-W2 claim, proven not asserted. *(real, reuses the 0018 Lakekeeper+MinIO fixture)*

## 8. Phases

- **P1** — W1 ClickHouse + capabilities + W4 policy **including §6.1 read-route defaults** + battery 1–5, 7–8, plus: a registered read route with no explicit option is bounded by its route default, and a per-call `timeout` above the route ceiling clamps down rather than up. (ClickHouse first: `run_command` already exists, and Gold-on-ClickHouse is the likeliest first consumer.)
- **P2** — W2 BigQuery + battery 6.
- **P3** — W3 DuckDB + battery 9, gated on 0020 W1 landing.

## 9. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Adapter breadth only — no new plane, no new contract concepts; the procedure contract is already correct and stays untouched | locked |
| 2 | Per-engine triage with a published `ProcedureCapabilities` matrix (0016 shape), fail-closed on declared requirements | locked |
| 3 | ClickHouse mutations execute synchronously **by default**; async is opt-in and the capability reports it — a `run()` that returns before the effect lands is a silent lie | locked |
| 4 | No SQL inspection, ever — write shape is not classified, not declared, not guessed; idempotency belongs to RFC 0030 | locked |
| 5 | RFC 0020 W2 is absorbed: engine-side lake writes = a DuckDB procedure route over an attached catalog, no separate design | locked |
| 6 | 0015's limit vocabulary lifts to registered routes; no cross-engine normalized cost budget (incommensurable units) | locked |
| 6a | **W4 covers analytics *read* routes too** (§6.1), not just procedure routes — the per-call `timeout` option ships with no route-level default, so the shipped default for a registered read is unbounded. Per-call clamps down, never up; no `unlimited` spelling; newly-bounding defaults get a changelog note and a generous value | locked |
| 6b | Snowflake / Databricks-SQL adapters recorded with a trigger (§6.2), unbuilt — the read half is conventional and valuable, the Databricks transform half is deliberately declined (it would compete with Spark/DLT rather than seam into it) | recorded |
| 7 | Coexistence clause: external transform tools owning the warehouse stay first-class; every route here is opt-in | locked |
| 8 | Engine capability claims in §2 are drafting-time and **must be re-verified at pickup** — a stale row changes an adapter | recorded |
