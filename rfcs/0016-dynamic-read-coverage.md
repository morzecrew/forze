# RFC 0016 — Dynamic read coverage: analytical engines + document pipelines

- **Status:** 📝 Draft — RFC 0015 P1 has since shipped, so the structural gate is clear. **W1 and W3 clear the tenancy floor unchanged and wait only on their own triggers. W2 does not**: RFC 0037 landed after this RFC and sets an intrinsic `namespace` floor for `origin="compiled"`, which every tier §1 credits DuckDB with is below, so no route wires today. It is re-scoped rather than refused, but **all four of its prerequisites are unmet, one of them is unscheduled, and one is not about credentials at all** — they are listed once, in §2, and deliberately not re-enumerated here. See also the 2026-08-15 addenda in §1 and §2, and decisions 6–9.
- **Scope:** Extend the RFC 0015 dynamic-read contract beyond Postgres, **by doctrine, not uniformly by code** (the RFC 0010 shape): every candidate engine is triaged by *what its server can actually enforce* — read-only, single-statement, timeout, result caps — and ships only with an honest enforcement matrix and a capability that fails closed where the engine cannot keep the promise. Plus one sibling contract for the non-SQL case: `DynamicPipelinePort` for runtime-authored Mongo aggregation pipelines, where the statement carrier is structured data and enforcement is a fail-closed stage allowlist rather than an engine flag.
- **Related:** RFC 0015 (contract, threat tiers, taxonomy — all reused verbatim). RFC 0010 (per-backend doctrine triage precedent). RFC 0037 (the `StatementOrigin` floor every engine here must clear — it post-dates this RFC and re-decided W2, see §1). RFC 0018 (the catalog client, per-tenant warehouse and vended-credential posture W2 now depends on; owned there, not duplicated here). The escape-hatch policy's "document should NOT get raw passthrough" ruling — engaged in §3, which is why the pipeline port is the most demand-gated item here.
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

*(2026-08-15 addendum — the DuckDB row above is wrong in both directions, and W2 does not execute as written. Three findings, each checked against a live engine rather than reasoned about:*

*(a) **The floor forbids every tier this row credits DuckDB with.** RFC 0037 makes `compiled` require `namespace`, intrinsically — there is nothing to lower. Measured: `compiled` is refused at `none` and at `tagged`, and wires only at `namespace` and `dedicated`. A route that is not `tenant_aware` derives `none`, so "single-tenant is fine" is not an escape either. The DuckDB deps module hard-codes `max_supported_isolation="tagged"` and `client_is_routed=False`, so as wired today no DuckDB dynamic-read route can exist at all.*

*(b) **A per-tenant database is not a container, and `read_only=True` is not read-only.** On a `read_only` connection to one tenant's database file, `ATTACH` of a second tenant's database succeeds and reads its rows; `read_csv` of an arbitrary local path succeeds; `COPY … TO` writes a file to disk. DuckDB has no permission system, so a statement leaves the catalog by naming a **path** rather than a relation. `enable_external_access=false` + `lock_configuration=true` closes all three (including the self-unlock `SET`) — and closes `read_parquet` of the lake with them. The setting that confines DuckDB is the setting that removes the reason to use it here, so §1's "cannot enforce" and its `tagged` ceiling are both artefacts of assuming one topology.*

*(c) **A credential-vending catalog resolves it, and RFC 0018 already specifies that.** Where the lake is a REST-catalog lakehouse (Lakekeeper, Polaris) rather than raw object storage, confinement moves off DuckDB entirely: the catalog vends short-lived table-scoped credentials, so a statement naming another tenant's prefix fails at object storage. RFC 0018 §3 already records `dedicated` as "warehouse/catalog-per-tenant via a routed catalog client" and already prefers vended credentials for this reason. **This RFC should not re-specify that story** — W2 defers to it.*

*Vending closes **object-storage** egress and nothing else, which is narrower than it first reads. `COPY … TO '/local/path'` needs no credential at all — probe (b) above wrote its file with none in play — so removing every standing bucket key leaves local-filesystem egress untouched. A statement can still land a tenant's rows on the worker's disk, where whatever runs next can read them. Confining that is a process-level control, not a catalog one: a **read-only** root filesystem with no writable mount the worker can reach, or a mandatory-access-control policy (AppArmor/SELinux/seccomp) denying the write. It has to be a control that denies **writing** specifically — `noexec` is the tempting near-miss and does not help at all, since it blocks executing a file and says nothing about creating one; a `COPY … TO` onto a writable `noexec` mount succeeds and leaves the rows sitting there. W2's battery therefore needs a **local-path** `COPY … TO` case alongside the object-storage one, and it needs to assert the refusal rather than the mount flag. Stated plainly because the appealing version of this sentence — "vended credentials mean the statement cannot write anywhere" — is false, and was in this addendum until review caught it.)*

## 2. Workstreams and triggers (each independently demand-gated)

- **W1 — ClickHouse adapter.** Trigger: the first Gold-on-ClickHouse consumer (Linecust post-MVP migration is the named candidate). Smallest diff: shell reuse + settings mapping + taxonomy mapping (CH error codes → `dynamic_read_*`) + the 0015 battery re-run with the server-native cap items swapped in.
- **W2 — DuckDB adapter (trusted-only).** Trigger: lake-backed widgets. Ships with the downgraded capability and the topology doc.

  *(2026-08-15 addendum — re-scoped, not refused. W2 requires a **routed DuckDB client**, one connection per tenant, over a credential-vending REST catalog; that derives `dedicated` and clears the origin floor, and the enforcement is the catalog's and object storage's rather than a DuckDB promise. Two consequences follow. The first is a prerequisite, and it is later than it looks — later, in fact, than any phase currently scheduled. Vended credentials are **RFC 0018 P3**, not P1, since P1 ships static ones. The routed client is the harder half: **nothing schedules it.** RFC 0020 W1 supplies the `IcebergCatalog` / `ATTACH` vocabulary it would be built from, but W1 is explicitly not it — that section resolves a tenant namespace into qualified names or a per-call `SET schema` on the shared in-process connection, and says in as many words that it "raises what governed *named-query* routes can reach, not what dynamic statements may do." The distinction is the whole point rather than a scoping detail: `SET schema` confines text the framework generated, and a dynamic statement is free to name another schema — which probe (b) is the demonstration of. So W2's real prerequisite is a **per-tenant routed DuckDB client** (a connection per tenant, each holding only that tenant's vended credentials) that no RFC currently owns, and W2 stays blocked until one does. Static credentials are **not** an acceptable fallback for this route: bucket-wide keys put it back in the raw-object-storage case below, which is a refusal, so a W2 that shipped on 0018 P1 would be the unwireable configuration wearing the wiring of a safe one. The second is a limit worth stating rather than discovering — with external access necessarily on, egress is confined only where a credential is the thing being checked. Object-storage destinations are (by vending); the local filesystem is not (it needs no credential), and any *other* standing credential the process holds is not. "The worker holds nothing else, and cannot write to its own disk" is a deployment property the framework cannot check and must not imply it checks. Over raw object storage with shared credentials there is no wireable configuration at all, and that half is a refusal.)*

  **W2 prerequisites — the canonical list.** Every other mention of them (the status line, the index row, decisions 6 and 9) points here instead of restating them. Three review rounds went to summaries that each carried a different subset, which is the ordinary fate of a list kept in four places; one copy is the fix, and this is the copy.

  1. **Vended credentials** — RFC 0018 **P3**. P1 ships static credentials and they are not a fallback: bucket-wide keys put the route back in the raw-object-storage case this RFC refuses.
  2. **A per-tenant routed DuckDB client** — one connection per tenant, holding only that tenant's vended credentials. **No RFC owns this.** RFC 0020 W1 supplies the `IcebergCatalog`/`ATTACH` vocabulary it would be built from and explicitly does not deliver it.
  3. **A process-level filesystem control** refusing local-path `COPY … TO` (decision 9). Not a credential and not the catalog's — vending cannot reach it, because writing to local disk requires no credential to begin with.
  4. **The `duckdb-iceberg` vended-credential probe** (§1 addendum (c)): confirmation that the extension uses vended credentials and does not silently fall back to ambient ones. A fact about a library version, so it is verified at pickup rather than assumed here.

  Items 1–3 are each necessary and none is sufficient; 2 is the one nothing schedules, and 3 is the one a reader is most likely to drop, because it is the only prerequisite that is not about credentials at all.
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
- Collection from the route config's `RelationSpec` resolver — never from the pipeline. Tenancy container = collection/database-per-tenant (namespace); tagged refused, same clause as 0015.
- `maxTimeMS` from the timeout; row cap by cursor cap+1; `allowDiskUse` a config knob defaulting off.
- Taxonomy reuse: `dynamic_read_stage_refused` (validation, carries the stage name) joins the family.

**Gate:** a named in-repo or flagship-app consumer running analytics-shaped Mongo aggregations from a catalog. None exists today — Linecust's Gold is SQL — so this section is a recorded design, deliberately executable the day the trigger fires and deliberately unbuilt until then (the RFC 0011 "parked by design" posture).

## 4. Acceptance additions (per shipped workstream)

1. ClickHouse: server-native cap fires (`result_overflow_mode='throw'`) → `dynamic_read_row_cap_exceeded`; a `SET` statement inside the query refused under `readonly=1`. *(real CH, testcontainers)*
2. BigQuery: dry-run refuses `INSERT`, `MERGE`, and a multi-statement `SCRIPT` before any billed execution. *(integration, env-gated)*
3. DuckDB: wiring refuses `provenance="untrusted"` and any tenant-aware route above `tagged`; trusted read over a registered Parquet view honors cap/timeout clamps. *(in-process)*

   *(2026-08-15 addendum — the first clause is now unreachable: the origin floor refuses every DuckDB route before a provenance or ceiling check is consulted, so an acceptance case asserting the ceiling would pass for the wrong reason. Replacements under the re-scoped W2, each a claim about the deployment rather than about DuckDB: a route without a routed catalog client is refused; two tenants' routed clients resolve disjoint catalogs; a statement naming a relation outside the vended credential's scope fails at object storage rather than returning rows; and `COPY … TO` a **local** path is refused by the process's filesystem control rather than by any credential. The last two are the ones that matter, and they are separate cases because they fail for unrelated reasons — the credential answers the store, and nothing but the filesystem answers the disk. Both belong against a live catalog and MinIO rather than a fake, since a fake would agree with whichever boundary the author believed in.)*
4. Pipeline: each named-refusal stage lands `dynamic_read_stage_refused`; an unknown/future stage name is refused (fail-closed proof); two tenants, same pipeline, different collection resolution → disjoint results. *(mock ≡ real Mongo)*
5. Capability honesty: every adapter's `DynamicReadCapabilities` is asserted against what its battery actually proves — no declared enforcement without a test that exercises it.

## 5. Decision log

| # | Decision | State |
|---|---|---|
| 1 | One coverage RFC, per-engine triage with honest enforcement matrix — not one RFC per adapter, not silent uniformity | locked |
| 2 | `DynamicReadCapabilities` fail-closed; DuckDB permanently trusted-only; BigQuery enforcement = server dry-run, priced explicitly | locked — **superseded in part by 6** (the DuckDB clause only; the capability object and the BigQuery dry-run stand) |
| 3 | Allowlist (not blocklist) for pipeline stages; unknown stages refused; cross-collection stages refused in v1 | locked |
| 4 | Pipeline port parked-by-design until a named consumer exists; the escape-hatch policy's document-plane ruling is engaged, not overridden | locked |
| 5 | Triggers: W1 = Gold-on-ClickHouse consumer; W2 = lake widgets; W3/W4 = named demand | recorded |
| 6 | **Supersedes the DuckDB clause of 2.** DuckDB dynamic read is not "trusted-only at `tagged`" — it is refused outright over raw object storage, and wireable only as a routed client over a credential-vending REST catalog (`dedicated`). The engine enforces neither the read-only nor the confinement property; the catalog and object storage do. Sequenced behind the four prerequisites listed in §2 — not restated here, because a second copy is how they drift apart | locked (2026-08-15) |
| 7 | This RFC does not re-specify lakehouse confinement. RFC 0018 owns the catalog client, the vended-credential posture and the per-tenant warehouse; W2 consumes them. A second copy is how the two drift | locked (2026-08-15) |
| 8 | The origin floor refuses `compiled` at `none`, so **single-tenant deployments are refused too** — the framework cannot distinguish one from an unscoped route in a multi-tenant deployment. Recorded here because it is a general RFC 0037 property surfaced by this triage, not a DuckDB one, and it constrains every future engine in this family | recorded (2026-08-15) |
| 9 | Confinement under W2 is **object-storage-scoped**. Vended credentials bound where a statement reads and writes *in the store*; they do not bound local-filesystem egress, which needs no credential at all. A process-level filesystem control is therefore a named W2 prerequisite with its own acceptance case, rather than something folded into the credential story where it would read as already handled | locked (2026-08-15) |
