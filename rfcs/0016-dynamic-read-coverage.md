# RFC 0016 — Dynamic read coverage: analytical engines + document pipelines

- **Status:** 📝 Draft (gated on RFC 0015 P1; each workstream demand-gated separately)
- **Scope:** Extend the RFC 0015 dynamic-read contract beyond Postgres, **by doctrine, not uniformly by code** (the RFC 0010 shape): every candidate engine is triaged by *what its server can actually enforce* — read-only, single-statement, timeout, result caps — and ships only with an honest enforcement matrix and a capability that fails closed where the engine cannot keep the promise. Plus one sibling contract for the non-SQL case: `DynamicPipelinePort` for runtime-authored Mongo aggregation pipelines, where the statement carrier is structured data and enforcement is a fail-closed stage allowlist rather than an engine flag.
- **Related:** RFC 0015 (contract, threat tiers, taxonomy — all reused verbatim). RFC 0010 (per-backend doctrine triage precedent). The escape-hatch policy's "document should NOT get raw passthrough" ruling — engaged in §3, which is why the pipeline port is the most demand-gated item here.
- **Origin:** The same Linecust analysis: Gold migrates to ClickHouse post-MVP, and lake-backed widgets (DuckDB over Parquet/Iceberg) are on the roadmap — both would otherwise regress widget execution back onto ungoverned client ports the day they land.

---

## 1. Enforcement matrix — what each engine can actually promise

The RFC 0015 governance shell (caps, clamps, tenancy resolution, taxonomy, capture) is engine-neutral and reused. What varies is the **enforcement column**, and the capability object must tell the truth about it:

| Engine | Read-only | Single statement | Timeout | Row cap | Tenancy container | Untrusted tier? |
|---|---|---|---|---|---|---|
| **Postgres** (0015) | `READ ONLY` txn — server | extended protocol — server | `statement_timeout` — server | client cap+1 probe | schema (+ role) | ✅ with role |
| **ClickHouse** | `readonly=1` per-query setting — server (also freezes settings changes) | single-query protocol — server | `max_execution_time` — server | **`max_result_rows` + `result_overflow_mode='throw'` — server-native**, the strongest cap in the family | `query_database` resolver (database-per-tenant) | ✅ — settings are server-enforced and un-overridable under `readonly=1` |
| **BigQuery** | **dry-run first**: server-side classification; refuse any `statementType` ≠ `SELECT` (incl. `SCRIPT` — which is also the multi-statement refusal) | same dry-run refusal | `jobTimeoutMs` | `maxResults` paging + cap+1 | `query_dataset` resolver (dataset-per-tenant) + IAM as backstop | ⚠️ dry-run classification is server-authoritative but adds one round trip per statement — priced, not hidden |
| **DuckDB** | **cannot enforce** on `:memory:` + views (no read-only mode for the in-process default; `read_only=True` applies to file DBs only) | client-side only | interrupt-based, cooperative | client cap+1 probe | `tagged` ceiling (per the tenancy matrix — in-process, shared connection) | ❌ **refused at wiring, always** |

Consequences, encoded as `DynamicReadCapabilities` on the port (the `QueryCapabilities`/`SearchCapabilities` pattern, fail-closed via the wiring guard):

- ClickHouse is the *best* citizen — every limit is a server-side per-query setting, so even the row cap stops the work server-side instead of discarding rows client-side. Its adapter is the mechanical one.
- BigQuery's enforcement is real (the server parses, we never do — decision 3 of RFC 0015 holds) but costs a dry-run round trip. The config makes that explicit (`enforce_via_dry_run: bool = True`, and setting it `False` demotes the route to `provenance="trusted"`-only at freeze). `maximum_bytes_billed` rides along as the cost cap this engine uniquely offers.
- DuckDB gets the honest downgrade: `provenance="trusted"` only, tenant-aware routes refused above `tagged`'s ceiling exactly as the tenancy matrix already says, and its docs say plainly that DuckDB dynamic read is for single-tenant / per-process-isolated lake reads (the Linecust lake-widget case runs one pool per worker over per-project views — which is a topology statement, not a framework guarantee).

## 2. Workstreams and triggers (each independently demand-gated)

- **W1 — ClickHouse adapter.** Trigger: the first Gold-on-ClickHouse consumer (Linecust post-MVP migration is the named candidate). Smallest diff: shell reuse + settings mapping + taxonomy mapping (CH error codes → `dynamic_read_*`) + the 0015 battery re-run with the server-native cap items swapped in.
- **W2 — DuckDB adapter (trusted-only).** Trigger: lake-backed widgets. Ships with the downgraded capability and the topology doc.
- **W3 — BigQuery adapter.** Trigger: a named consumer; none today. Recorded so the dry-run enforcement design doesn't get re-litigated later.

## 3. The non-SQL sibling: `DynamicPipelinePort` (Mongo) — most gated, specified so the gate is the only thing missing

The escape-hatch policy ruled that the **document plane** must not grow raw passthrough (heaviest invariants: tenant/OCC/history/soft-delete; least portable). That ruling stands. This port is not the document plane: it is the dynamic-read plane's second statement carrier, targeting the same analytics-shaped reads — and its long tail is exactly the policy's admission test: aggregation surface (`$facet`, `$bucket`, `$setWindowFields`, `$densify`) that the querying DSL will never model.

Shape (sibling contract in `contracts/dynamic_read/`, own dep key `dynamic_pipeline_query`):

```python
class DynamicPipelinePort(Protocol):
    def aggregate(
        self, pipeline: Sequence[JsonDict],
        *, options: DynamicReadOptions | None = None,
    ) -> Awaitable[Sequence[JsonDict]]: ...
```

Enforcement is necessarily **allowlist-based** — Mongo has no read-only transaction to lean on — so the doctrine inverts RFC 0015's "no parsing" rule *for structured input only* (parsing JSON stages is exact; parsing SQL is not):

- **Fail-closed stage allowlist.** Known-read stages pass (`$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`, `$unwind`, `$addFields`/`$set`, `$replaceRoot`, `$count`, `$facet`, `$bucket`, `$bucketAuto`, `$sortByCount`, `$densify`, `$setWindowFields`); **unknown stages are refused**, not passed through — a new server version's write stage must not sail through an old allowlist. Named refusals with their own reasons: `$out`/`$merge` (writes), `$where`/`$function`/`$accumulator` (JS execution), `$lookup`/`$graphLookup`/`$unionWith` (**cross-collection — breaks container confinement**; a within-namespace `$lookup` variant is recorded as a follow-up gated on a consumer who needs it and on a resolver-checked target collection).
- Collection from the route config's `RelationSpec` resolver — never from the pipeline. Tenancy container = collection/database-per-tenant (namespace); tagged refused, same clause as 0045.
- `maxTimeMS` from the timeout; row cap by cursor cap+1; `allowDiskUse` a config knob defaulting off.
- Taxonomy reuse: `dynamic_read_stage_refused` (validation, carries the stage name) joins the family.

**Gate:** a named in-repo or flagship-app consumer running analytics-shaped Mongo aggregations from a catalog. None exists today — Linecust's Gold is SQL — so this section is a recorded design, deliberately executable the day the trigger fires and deliberately unbuilt until then (the RFC 0011 "parked by design" posture).

## 4. Acceptance additions (per shipped workstream)

1. ClickHouse: server-native cap fires (`result_overflow_mode='throw'`) → `dynamic_read_row_cap_exceeded`; a `SET` statement inside the query refused under `readonly=1`. *(real CH, testcontainers)*
2. BigQuery: dry-run refuses `INSERT`, `MERGE`, and a multi-statement `SCRIPT` before any billed execution. *(integration, env-gated)*
3. DuckDB: wiring refuses `provenance="untrusted"` and any tenant-aware route above `tagged`; trusted read over a registered Parquet view honors cap/timeout clamps. *(in-process)*
4. Pipeline: each named-refusal stage lands `dynamic_read_stage_refused`; an unknown/future stage name is refused (fail-closed proof); two tenants, same pipeline, different collection resolution → disjoint results. *(mock ≡ real Mongo)*
5. Capability honesty: every adapter's `DynamicReadCapabilities` is asserted against what its battery actually proves — no declared enforcement without a test that exercises it.

## 5. Decision log

| # | Decision | State |
|---|---|---|
| 1 | One coverage RFC, per-engine triage with honest enforcement matrix — not one RFC per adapter, not silent uniformity | locked |
| 2 | `DynamicReadCapabilities` fail-closed; DuckDB permanently trusted-only; BigQuery enforcement = server dry-run, priced explicitly | locked |
| 3 | Allowlist (not blocklist) for pipeline stages; unknown stages refused; cross-collection stages refused in v1 | locked |
| 4 | Pipeline port parked-by-design until a named consumer exists; the escape-hatch policy's document-plane ruling is engaged, not overridden | locked |
| 5 | Triggers: W1 = Gold-on-ClickHouse consumer; W2 = lake widgets; W3/W4 = named demand | recorded |
