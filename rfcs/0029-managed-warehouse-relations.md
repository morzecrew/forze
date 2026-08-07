# RFC 0029 — Managed warehouse relations: attestation first, creation second

- **Status:** 📝 Draft (RFC 0028 family; independent of 0028, required by 0030)
- **Scope:** A control-plane sibling for the analytics plane — `AnalyticsAdminPort` (`describe` / `verify` / `ensure` / `drop`) plus a declared `RelationShape` derived from the spec's own models — so a service that reads or writes a warehouse relation can state what that relation must look like and **refuse to start when reality disagrees**. Four reconciliation modes, `off` by default: **`verify`** (attest at startup, never write DDL — the headline), **`ensure`** (create if absent), **`evolve`** (additive drift applied, everything else fails closed), `off` (today's behavior, unchanged). Plus the `WarehouseTenantProvisioner` that closes a half-built story already in the tree. ClickHouse, BigQuery, Postgres-analytics; DuckDB follows its procedure adapter.
- **Related:** RFC 0018 §"`ensure_table` reconciliation" is the proven precedent — create-if-absent, additive evolution under a declared policy, `lake_schema_drift` otherwise — and this RFC deliberately reuses its doctrine and its failure vocabulary rather than inventing a second dialect for the second plane. RFC 0015's "freeze-time introspection attestation … the startup schema-validation pattern applied to policies" is the precedent for `verify`. `TenantProvisionerPort` (`contracts/tenancy/provisioning.py`) already exists, is already idempotent-by-contract, and currently has no warehouse implementation. The established `*AdminPort` control/data-plane split (`StreamGroupAdminPort`) is why this is a sibling port rather than methods bolted onto `AnalyticsQueryPort`.
- **Origin:** Two findings in the current tree, one of them a live instance of the standing "built the mechanism, not the gate" theme. **(a)** Nothing in forze creates or checks a warehouse relation — `grep ensure_table` outside `contracts/lake` returns only internal store bootstrap (inbox, counters, durable runs). Every analytics route assumes its table exists, with the right columns, and discovers otherwise at query time in production. **(b)** `BigQueryAnalyticsConfig.query_dataset` and `ClickHouseAnalyticsConfig.query_database` ship dataset-per-tenant and database-per-tenant *routing*, documented as isolation — and **nothing provisions the target**. The routing half shipped; the creation half does not exist; the gap surfaces as a runtime error on the first query for a newly onboarded tenant.

---

## 1. Attestation is the headline, and that is the whole seam argument

The instinct is to build `ensure` — the framework creates your tables. That instinct is what turns a framework into a platform, and it is also the *less* valuable half.

The valuable half is this: **a backend service should refuse to start if the warehouse does not match what its code assumes.** Not build the warehouse — *check* it. That is a statement about the application's contract with its data, which is squarely framework territory (it is the same thing `check_wiring` does for operations, and the same thing RFC 0015 proposed for RLS policies), and it is the mode that works when someone else owns the warehouse entirely.

Concretely, `verify` mode buys the deployment that dbt-builds-Gold-and-forze-serves-it — the most common real topology — a guarantee it cannot get any other way: the moment a transform tool renames a column, the *reader* fails at startup with a named drift error instead of returning wrong or empty dashboards at 3am. Forze never issues a line of DDL in this mode. It reads `information_schema` and says no.

`ensure`/`evolve` exist for the case where the service genuinely owns its relations — per-tenant Gold created by a product flow, staging relations for RFC 0030 — and they are the secondary modes on purpose. The **coexistence clause from 0028 §1 holds**: `reconcile` defaults to `off`, every existing route keeps its behavior byte-for-byte, and adopting `verify` never requires adopting `ensure`.

## 2. The declaration

Logical shape derives from the models the route already declares — the analytics spec's `read` type for a queried relation, `ingest` for a written one — through **the same Pydantic→engine-type derivation as RFC 0018, with the same fail-closed rule**: a type that cannot be mapped faithfully refuses at spec validation rather than approximating. The Decimal-union and UUID-write-gap incidents are why derivation refuses; there is no reason for the warehouse plane to relearn that lesson with its own bugs.

```python
class AnalyticsAdminPort(Protocol):                    # control plane, one relation per route
    def describe_relation(self) -> Awaitable[RelationShape | None]: ...   # None = absent
    def observe_relation(self) -> Awaitable[RelationObservation]: ...     # §2.1 — state, not shape
    def verify_relation(self) -> Awaitable[RelationVerdict]: ...          # never writes DDL
    def ensure_relation(self) -> Awaitable[RelationRef]: ...              # create / evolve per policy
    def drop_relation(self) -> Awaitable[None]: ...                       # config-flag gated (0018 posture)
```

### 2.1 Observation — freshness for relations you did not build

Shape is only half of what a reader needs to know. The other half is **state**: is this relation stale? RFC 0030 answers that for relations *this service publishes* — its build record carries the last successful publish — but that record is the **builder's** view and is never written when someone else owns the table. In the topology `verify` exists for, that is precisely the relation you cannot vouch for. A schema check that passes while the data is three days old is a check that inspects the label and not the contents.

So observation is a distinct method with a distinct source: the engine's own metadata, not forze's records.

```python
@attrs.define(frozen=True, kw_only=True, slots=True)
class RelationObservation:
    exists: bool
    last_modified_at: datetime | None       # engine metadata; None where unavailable
    approximate_row_count: int | None
    watermark: datetime | None              # MAX(declared column) when a freshness column is declared
    source: ObservationSource               # ENGINE_METADATA | WATERMARK_COLUMN | UNAVAILABLE
```

Per engine (**verify at pickup**): BigQuery exposes table modification time and row estimates through `INFORMATION_SCHEMA`; ClickHouse through `system.parts`/`system.tables`; Postgres through catalog + statistics (approximate, and labelled so); Iceberg/Delta through the current snapshot's timestamp. Where an engine gives nothing trustworthy, `source=UNAVAILABLE` and the fields are `None` — **never a fabricated estimate**, and never a silent fallback to a full `COUNT(*)` scan on a warehouse table.

`ObservationSource` is on the value object because the two sources have genuinely different meanings and the difference matters when acting on them: engine metadata says *when the table was last written*, a watermark column says *how recent the data in it is*. A daily rebuild that writes nothing because upstream was empty updates the first and not the second. Declaring `freshness_column` opts a route into the second, stronger reading.

**`verify` gains an optional staleness verdict.** A route may declare `max_staleness`, and `verify` then reports stale relations alongside drifted ones. Two knobs, because the right consequence differs: `on_stale="warn"` (default — record and expose it, do not block deployment) or `"fail"` (a service whose correctness depends on fresh data refuses to start). Default is `warn` deliberately: staleness is a *runtime* condition that can resolve on its own, unlike schema drift, which is a deployment-time fact that cannot. Failing startup on a transient upstream delay would make `verify` a liability in the topology it was designed for.

**Physical shape lives in the integration config, not the contract** — the split the analytics configs already draw. `dataset`/`database` and the tenant resolvers are there today; this RFC adds the physical layout beside them: BigQuery time-partitioning + clustering fields, ClickHouse table engine + `ORDER BY` + `PARTITION BY` + TTL, Postgres indexes. Modeling those in a shared contract vocabulary would produce a lowest-common-denominator abstraction that lies about all three engines. The contract knows columns and types; the config knows the engine.

## 3. Drift, and which drift is fatal

Verify produces a verdict, not a boolean, because the interesting part is *which* disagreement:

| Drift | Default | Why |
|---|---|---|
| Relation absent | **fatal** | nothing to read |
| Declared column missing | **fatal** | the registered SQL or the ingest write references it |
| Type mismatch | **fatal** | the read model will mis-validate, or worse, silently coerce |
| Nullability weakened | **fatal** | a non-optional field on the read model will start raising |
| **Undeclared extra column present** | **tolerated** | see below |
| Partition/cluster/`ORDER BY` mismatch | **fatal in `verify`** | see below |

**Extra columns are tolerated by default and that is deliberate.** Forze's analytics reads are registered SQL projecting into a declared model — an extra column is invisible to them. Failing on it would make `verify` unusable in exactly the topology it is designed for, where a transform tool legitimately adds columns the reader does not know about. A `strict_columns=True` flag exists for the service that owns its relations outright.

**Physical mismatch is fatal in `verify` and cannot be silently repaired.** ClickHouse's `ORDER BY` is not alterable after creation; BigQuery partitioning is fixed at table creation. So a partition/sort mismatch is not drift the framework can fix — it is a table that must be rebuilt, which is an operator decision with data movement attached. `evolve` therefore covers **additive columns only**, and never touches physical layout. Anything else fails closed with the drift code and a message naming the required rebuild. Pretending `evolve` could repair a sort key would be the worst kind of convenience.

**Destructive operations are never automatic.** No column drops, no type narrowing, no truncation. `drop_relation` exists for provisioner teardown and is gated behind an explicit config flag, matching 0018's `drop_table` posture.

## 4. Per-tenant provisioning — closing the half-built story

`TenantProvisionerPort` is already the right seam and already carries the two hard-won constraints: provisioners are idempotent, and they receive the `TenantIdentity` explicitly because the tenant being provisioned is generally *not* the ambient bound tenant. A warehouse provisioner needs nothing new from the contract:

```python
WarehouseTenantProvisioner(
    routes=[gold_orders_route, gold_sessions_route],   # the declared relations
    create_container=True,     # the BQ dataset / CH database / PG schema itself
    reconcile=Reconcile.ENSURE,
)
```

`provision(tenant)` resolves each route's `NamedResourceSpec` for that tenant, creates the container if absent, then runs `ensure_relation` per route. Idempotent by construction (both steps are create-if-absent). **`deprovision` refuses by default** and requires an explicit destructive flag — dropping a tenant's warehouse is not a default any framework should ship, and the asymmetry with `provision` is intentional rather than an oversight.

Note the second-order win: with this in place, `query_dataset`/`query_database` finally mean what their docstrings claim. The isolation story stops being "routing exists, provisioning is your problem" — which is precisely the shape of finding the 7th-edition audit kept turning up.

## 5. Out of scope, with reasons

- **A migration framework.** No versioned migration files, no up/down, no history table. `verify` + additive `evolve` is the ceiling. Anything requiring ordered stateful schema history is Alembic's job or dbt's, and forze should be the thing that *checks the result*, not the thing that owns the sequence.
- **Arbitrary DDL.** There is no `execute_ddl` hatch. Anything outside declared shape reconciliation goes through a procedure route (RFC 0028) where it is registered, reviewed, and greppable — or through the raw client, which remains the honest escape hatch it already is.
- **Cross-relation constraints** (foreign keys, referential integrity across warehouse tables). Warehouse engines mostly do not enforce them; a framework that declared them would be declaring fiction.
- **Cost/storage policy** (retention, tiering, TTL enforcement). TTL is expressible as physical config where the engine has it; forze does not schedule or enforce lifecycle. Physical table health (small files, snapshot counts) stays RFC 0019's read model; §2.1's observation is the logical-state twin, deliberately cheap — metadata reads only, never a scan.
- **Row-level erasure across warehouse relations** (tenant offboarding, GDPR-shaped deletion). `drop_relation` and `deprovision` cover the whole-container case; deleting *one subject's rows* across N Gold tables is a different operation with engine-specific mechanics (ClickHouse lightweight deletes vs mutations, BigQuery DML quotas, Iceberg copy-on-write rewrites) and app-specific scope. It belongs to an RFC 0028 procedure route, where it is registered, reviewed and greppable. **Named here rather than left silent**, because an uncovered obligation that nobody has written down is the kind that surfaces during an audit rather than during design.

## 6. Acceptance battery

1. Derivation round-trip: a spec model with UUID / Decimal / tz-datetime / optional / nested-struct fields derives a relation, `ensure` creates it, an ingest write and a registered read round-trip every type. An unmappable model refuses at spec validation. *(mock ≡ real, each engine)*
2. `verify` on a matching relation passes; on a missing relation, missing column, mismatched type, and weakened nullability it fails at **startup** with the distinct drift code per case. *(real)*
3. `verify` tolerates an undeclared extra column by default; `strict_columns=True` fails on it. *(real)*
4. `evolve` applies an additive column and leaves data intact; a type change and a physical/`ORDER BY` mismatch both fail closed under `evolve`, with a message naming the rebuild. *(real CH + BQ — the two engines where physical layout is immutable)*
5. `reconcile="off"` is byte-for-byte today's behavior: no `information_schema` reads, no startup cost, no new failure mode. *(unit — the coexistence clause, pinned)*
   - **Observation, on a relation this service never wrote**: a table written by an out-of-band process is observed with a plausible `last_modified_at` and row estimate; the read is metadata-only — asserted by proving no scan-shaped query is issued (statement log / query-count check), because a `COUNT(*)` fallback on a warehouse table is a cost incident, not a slow test. *(real, each engine)*
   - Where an engine gives no trustworthy metadata, `source=UNAVAILABLE` with `None` fields — never a fabricated number. *(real)*
   - `freshness_column` vs engine metadata **diverge and both are reported**: a rebuild that writes zero new rows advances `last_modified_at` and leaves `watermark` unchanged — the distinction §2.1 claims, given a test. *(real)*
   - `max_staleness` exceeded → `on_stale="warn"` records and starts; `"fail"` refuses startup; schema drift still fails regardless of staleness settings. *(unit + real)*
6. Provisioner: onboarding a new tenant creates the container and every declared relation; re-running is a no-op; a partial failure mid-way is fully recovered by the retry. *(real)*
7. Provisioner scopes by the **passed** identity, not the ambient tenant — provisioning tenant B while bound as tenant A targets B's container. *(unit + real; the contract's stated trap, given a test)*
8. `deprovision` refuses without the destructive flag. *(unit)*
9. Drop is refused when the config flag is unset. *(unit)*

## 7. Phases

- **P1** — `RelationShape` + derivation + `AnalyticsAdminPort` + `verify` mode + **observation and staleness (§2.1)** + drift taxonomy + ClickHouse and BigQuery adapters + battery 1–3 and 5, including its observation and staleness sub-items. *(verify-only ships first — it is the highest-value, lowest-blast-radius half, and it is the half that needs no destructive capability at all. Observation ships **with** it, not after: a schema check that cannot say whether the data is stale is half an answer in exactly the topology this mode serves.)*
- **P2** — `ensure`/`evolve` + physical config + battery 4.
- **P3** — `WarehouseTenantProvisioner` + Postgres-analytics adapter + battery 6–9.

## 8. Decision log

| # | Decision | State |
|---|---|---|
| 1 | **Attestation (`verify`) is the primary mode and ships first**; creation is secondary. A framework that refuses to start on drift is a seam; one that owns your DDL is a platform | locked |
| 2 | `reconcile` defaults to `off` — existing routes unchanged, adoption incremental, coexistence with external transform tools preserved | locked |
| 3 | Control-plane sibling port (`*AdminPort` pattern), not methods on the query/ingest ports | locked |
| 4 | Logical shape in the contract (derived from spec models, fail-closed on unmappable types, 0018's rule verbatim); physical layout in the integration config | locked |
| 5 | Extra columns tolerated by default (registered SQL cannot see them); `strict_columns` opt-in | locked |
| 6 | `evolve` = additive columns only; physical layout is never silently rebuilt — fail closed and name the rebuild | locked |
| 7 | No destructive automation: no drops, no narrowing; `drop_relation` and `deprovision` both flag-gated, `deprovision` refusing by default | locked |
| 8 | Not a migration framework, no DDL hatch — anything else is a 0028 procedure route or the raw client | locked |
| 9 | The provisioner closes an *existing* gap (`query_dataset`/`query_database` route to containers nothing creates), which is why it is in this RFC rather than deferred | locked |
| 10 | **Observation is a distinct method from description**, sourced from engine metadata rather than forze's own records — so freshness works for relations this service did not build. Added after the topology audit found 0030's build record is the *builder's* view and leaves external-owned relations unobservable | locked |
| 11 | Observation is metadata-only and **never falls back to a scan**; unavailable metadata reports `UNAVAILABLE`, never an estimate | locked |
| 12 | `on_stale` defaults to `warn`, not `fail` — staleness is a runtime condition that can self-resolve; drift is a deployment-time fact that cannot. Failing startup on a transient upstream delay would make `verify` a liability where it is most needed | locked |
| 13 | Row-level erasure is explicitly named out of scope (0028 procedure route) rather than left unmentioned | locked |
