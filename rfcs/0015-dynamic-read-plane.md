# RFC 0015 — Governed dynamic read: runtime-authored statements under framework governance

- **Status:** ✅ **Complete — shipped 2026-08-14** (P1–P3). The plane, the Postgres and mock adapters, the wiring guards, the `dynamic_read` conformance plane (mock ≡ Postgres over the governance shell), the engine-enforced battery against a real server, the Tier B role and its provisioning, the §4.11 DST leg (the plane driven from a workload under a perturbed schedule, masking asserted on a real bundle), and `pages/docs/data-events/dynamic-read.md` all ship. Two codes were added beyond §3.4 during execution — see decisions 16 and 17. RFC 0037 (`StatementOrigin` floors) landed first after all: guard 1 is retired, routes declare `origin="compiled"`, and the shared floor refuses any tier below `namespace` — see §3.2 and decision 20. Guard 2 (author trust) stays Postgres-local, since it needs the module's view of whether the client is routed.
- **Scope:** A new read-plane contract — `contracts/dynamic_read/` — for executing **statements whose text is data**: SQL authored at runtime (by a catalog, a semantic-layer compiler, or an agent) rather than registered at wiring. The port does not weaken any existing guarantee: the analytics plane's "handlers never pass SQL" promise stays intact on analytics routes; this is a *separate, opt-in* plane whose whole design is that the framework — not the caller — owns read-only enforcement, tenancy confinement, resource limits, and observability for statements it cannot inspect. Postgres + mock in this RFC; engine breadth (ClickHouse/DuckDB/BigQuery) and a document-pipeline sibling are RFC 0016.
- **Related:** The `GraphRawQueryPort` precedent (opt-in raw hatch, `allow_raw_query=False` default, tenancy-based refusal in `forze_neo4j`) and the recorded escape-hatch policy (raw passthrough only where the neutral surface has a large un-modelable long tail AND few cross-cutting invariants the raw path would bypass). The procedures plane (`ProcedureSpec`, `PostgresProcedureConfig.query_schema`, the `tenancy_sql` helpers) supplies most of the mechanics; the analytics plane supplies the doctrine this plane must *not* erode. `pages/docs/data-events/procedures.md` §"Raw client / Procedures / Analytics" gains a fourth column.
- **Origin:** The Linecust gap analysis (2026-07-31): a BI product whose per-client data model (ЦМД) is assembled at runtime needs to execute catalog-authored widget/measure SQL against per-project schemas on every dashboard read. Today the only path is the raw `PostgresClientPort` — permitted but ungoverned ("you own validation, tenancy, portability"), which puts the product's highest-frequency, most exposed read path outside every framework guarantee. The same shape recurs in any catalog-driven system: rule engines, report builders, semantic layers.

---

## 1. Why the existing three tiers don't cover this

| | SQL known at | Output shape known at | Governance |
|---|---|---|---|
| **Analytics** (`query_key + params`) | wiring | wiring (`select_run` moves it to runtime) | full: tenancy binding, codecs, resilience, OTel, DST |
| **Procedures** (one spec = one statement) | wiring | wiring | full; command-only |
| **Raw client** (`PostgresClientPort`) | runtime | runtime | **none** — by documented policy |
| **Dynamic read** (this RFC) | **runtime** | runtime | read-only enforced by the engine; tenancy = container confinement; limits + taxonomy + capture |

The missing cell is *runtime statement text with framework governance*. The escape-hatch policy is not violated but **engaged**: the long tail here is un-modelable *by definition* (the statement is produced by another program), and the cross-cutting invariants at stake — tenancy above all — are not bypassed but **taken over by the port**, because a statement the framework cannot read must be confined by the container it runs in, not by a predicate we hope it contains.

Why not a method on the analytics port: `AnalyticsQueryPort`'s module docstring — "Handlers must not pass raw SQL strings on these ports" — is a load-bearing promise. Grafting `run_dynamic` onto it would rot that guarantee for every analytics route and make the dangerous capability invisible in wiring. A separate plane keeps the promise intact and makes dynamic read **greppable**: a reviewer finds every wiring that grants it. (Foreclosed; decision 1.)

Why not "just use the raw client": the raw client will remain the right tool for the *write* half of the same product story (runtime DDL, bulk loads — explicitly out of scope here, decision 2). But the read half is the hot path — every dashboard render — and hot paths deserve the same governance the rest of the framework earns its keep with. "Permitted but ungoverned" is the wrong posture for the most-executed statement class in the app.

## 2. Threat model — three provenance tiers, named honestly

The single most important design input: **who authored the statement text**. The port forces the wiring author to declare it; the declaration changes what the wiring guard demands.

| Tier | Statement author | Example | Required confinement |
|---|---|---|---|
| **A — trusted** | The app's own release artifacts, selected at runtime | shared visualization catalog SQL; semantic-compiler output from reviewed templates | engine read-only + namespace routing + limits |
| **B — untrusted (non-adversarial)** | A program whose output is not reviewed per-statement and is not *crafted to escape* | LLM-generated SQL from our own templates, user-configurable report definitions | Tier A **plus** `SET LOCAL ROLE` to a schema-confined role, or a dedicated-tier (routed) client |
| **C — adversarial** | An author who may deliberately construct escape gadgets | end-user free-text SQL console, hostile tenant | **dedicated tier only** (separate credentials/database per tenant — connection identity is the only unforgeable scoping key, see below) |

The honesty behind the tiers (this analysis is the RFC's core and goes into the docs page verbatim):

- **`SET LOCAL search_path` routes; it does not confine.** A statement can schema-qualify `other_schema.table` explicitly. Namespace routing is the *correctness* mechanism for Tier A (the trusted statement references unqualified names and lands in the right project schema); it is not a security boundary against Tier B/C.
- **`SET LOCAL ROLE` confines reads against mistakes, not against gadgets.** A NOLOGIN role with `USAGE` on exactly the tenant's schema (+ `ALTER DEFAULT PRIVILEGES … GRANT SELECT`, so tables created later by the pipeline are covered) blocks cross-schema reads for any statement that simply *references* the wrong relation — which is the entire mistake-class a non-adversarial generator produces. Against a deliberately crafted statement it is porous, and the RFC names the mechanism rather than hand-waving it: **on a shared connection, the statement and the adapter wield the same identity — any privilege the adapter can invoke mid-session, a hostile statement can invoke too.** Two escape classes: (1) a `DO` block can `EXECUTE 'RESET ROLE'` and probe other schemas via error/timing side channels; (2) stronger, *row-returning*: direct `FROM` references are permission-checked at executor startup, but dynamic-SQL builtins (`query_to_xml`, `table_to_xml`) check their **inner** query at execution time — so a single statement can sequence `set_config('role', …)` (the `role` GUC is settable mid-query) before such a builtin and read another tenant's schema, whenever the connection user holds SET-able membership in other tenants' roles, **which the adapter's own `SET LOCAL ROLE` requires it to hold**. PG16's `GRANT … WITH SET FALSE` cannot fix this on a shared connection: it disarms the adapter's role switch symmetrically. What survives every gadget: `SET TRANSACTION READ ONLY` is sticky for the transaction's lifetime — writes stay impossible throughout. Conclusion, written into the tier table: role confinement is **mistake-proofing plus defense-in-depth for non-adversarial generators**; an adversarial author gets Tier C, because the only scoping key a statement cannot forge from inside the session is the connection's login identity. The framework has the right to not know a better answer; it does not have the right to imply one exists.
- **Single-statement enforcement is real, not parsed.** The adapter always executes through the extended query protocol (server-side binding), under which the server itself rejects multi-command strings — `'…; RESET ROLE; …'` chains die at the protocol layer, not at a regex we would have to maintain. SQL parsing/blocklisting appears nowhere in this design (decision 3): every enforcement mechanism is something the **engine** refuses, because a parser we write is a parser an attacker outgrows.
- **Read-only is engine-enforced.** The statement runs inside a `READ ONLY` transaction: `INSERT`/`UPDATE`/`DELETE`/DDL/`nextval` are refused by Postgres with SQLSTATE `25006`, which the adapter maps to a stable code. Volatile-but-read-only functions still run; `pg_sleep` is bounded by the statement timeout.

### 2.1 Why the `tagged` tier is refused — and the one recorded path back

On the tagged tier, isolation *is* a predicate inside the statement — and for a runtime statement, every compensating control the registered-SQL planes rely on is absent at once:

1. **The predicate is unverifiable.** Registered analytics/procedure SQL earns tagged because it is frozen config: reviewed once, placeholder-guarded at freeze, covered by tests. A runtime statement has none of that, and a "must reference `%(tenant)s`" check at call time proves **reference, not scope** (the `bind_tenant_param` docstring's own caveat): the predicate can sit on one branch of a `UNION`, one table of a join, inside a dead `OR`, or after the aggregation it was supposed to bound.
2. **The failure mode is the worst class.** A missing or partial predicate does not error — it *succeeds* with cross-tenant rows in a correctly-rendered widget. Silent cross-tenant reads are the framework's canonical never-again bug (the Mongo history `$exists:false` leak). On namespace/dedicated, the identical authoring mistake either fails loudly (undefined relation) or stays inside the tenant's container: same bug, harmless outcome. Fail-closed doctrine picks the tier where mistakes are loud.
3. **Auto-injecting the predicate would require the parser this RFC forecloses** (decision 3): rewriting arbitrary SQL to scope every relation reference — through CTEs, views, lateral joins, quoted identifiers — is a parser we would maintain forever and statements would outgrow.

**The recorded path back: RLS-attested tagged (demand-gated, trusted provenance only).** Postgres row-level security moves the predicate from the statement into the engine — `FORCE ROW LEVEL SECURITY` + a policy keyed to a GUC the adapter sets (`SET LOCAL`, the existing query-parameters channel) makes *forgetting to scope impossible*, which is engine-enforced and therefore admissible under this RFC's doctrine. The shape, if a consumer materializes: config flag `rls_attested=True` triggers a **freeze-time introspection attestation** (every relation reachable by the route has `relforcerowsecurity` + a policy bound to the adapter's GUC; the connection role is neither `BYPASSRLS` nor policy-exempt — the startup schema-validation pattern applied to policies), and the adapter binds the tenant GUC before the statement. Honest limits, which is why it stays trusted-only and demand-gated: `set_config('app.tenant', …)` is callable *inside* a statement, so RLS-via-GUC is forgeable by an adversarial author (same shared-connection theorem as above — only mistakes are prevented, which for trusted provenance is exactly the threat model); relations created later by a runtime DDL path must ship their policy in the same breath or the attestation goes stale (re-attestation hook required); planner/statistics side channels exist and are documented, not solved. No consumer needs this today — Linecust runs namespace via project-as-tenant — so it is recorded here to keep the refusal honest: tagged is refused because it is unverifiable *as normally deployed*, not because no engine-enforced variant could ever exist.

## 3. Design

### 3.1 Contract — `contracts/dynamic_read/`

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DynamicReadSpec(BaseSpec):
    """One governed dynamic-read surface (a route), not one statement."""
    row_cap: int = 10_000            # hard ceiling on returned rows; exceeding raises
    max_statement_bytes: int = 65_536
    capture_statements: bool = False # DST/trace value capture of statement text (inference
                                     # `capture_inputs` twin: masked unless opted in)
    description: str | None = None
```

No `In`/`Out` generics: both the parameter shape and the row shape are runtime data — that is the plane's definition. No `encryption` field, deliberately (§3.5).

```python
class DynamicReadPort(Protocol):
    def run(
        self, statement: str, params: JsonDict | None = None,
        *, options: DynamicReadOptions | None = None,
    ) -> Awaitable[Sequence[JsonDict]]: ...
    def select[T: BaseModel](
        self, return_type: type[T], statement: str, params: JsonDict | None = None,
        *, options: DynamicReadOptions | None = None,
    ) -> Awaitable[Sequence[T]]: ...
```

`run` returns mapping rows in column order (the widget-rendering shape); `select` is the analytics `select_run` twin — output type as a runtime argument, validated at the port boundary. `DynamicReadOptions` (TypedDict): `timeout: timedelta` (clamped to the route ceiling, never above it), `row_cap: int` (clamped to the spec cap, never above it). Deliberately **no pagination** in v1: a widget/measure read that needs paging through >10k rows is a mis-authored statement, and offset paging over runtime SQL invites the exact fan-out/cost problems the caps exist to surface. A `run_chunked` streaming variant is recorded as demand-gated follow-up (decision 9).

Deps: `DynamicReadDepKey = DepKey("dynamic_read_query")`, accessor `ctx.dynamic_read.query(spec)`, resolved via `resolve_configurable(route=spec.name)` like analytics/procedures — so per-route resilience policies, OTel wrapping, and interceptors apply unchanged. **Resolvable in read-only (`QUERY`) operations** — the whole point is dashboard reads; this is the deliberate inversion of the procedures plane's command-only stance (decision 4).

### 3.2 Postgres route config

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDynamicReadConfig(TenantAwareIntegrationConfig):
    provenance: Literal["trusted", "untrusted"]          # mandatory — no default
    query_schema: NamedResourceSpec | None = None        # SET LOCAL search_path
    role: NamedResourceSpec | None = None                # SET LOCAL ROLE (Tier B)
    statement_timeout: timedelta = timedelta(seconds=5)  # SET LOCAL; always on
```

Safe-by-default is a design requirement here, not a preference — the 7th-edition audit's standing theme is "built the mechanism, not the default/gate", and this plane is exactly where an unsafe default would be found three months later. Hence: `provenance` has **no default** (the author must name their threat tier to wire the route at all); the timeout and row cap ship **on** with real values; there is no `unlimited` spelling for either (a caller wanting more sets a bigger number and owns it in review).

**Wiring guards** (all fail at freeze, in `validate_against_spec` — the `procedures_tenant_param_unreferenced` pattern):

1. ~~`tenant_aware=True` requires `query_schema` (namespace tier) **or** a routed client (dedicated tier) — `dynamic_read_tagged_refused`.~~ **Superseded 2026-08-14 by RFC 0037.** Routes declare `origin="compiled"`, whose floor is `namespace`, and the shared check refuses anything weaker as `statement_origin_isolation_floor`. Same doctrine, wider reach: this guard fired only on `tenant_aware=True` — a route claiming tagged-tier scoping its container could not honour — so a route that scoped itself **not at all** contradicted nothing, wired, and read across every tenant. See §8 decision 20.
2. `provenance="untrusted"` requires `role` or a routed client — `dynamic_read_untrusted_unconfined`. Tier C (hostile) is a documentation stance, not a config value: nothing distinguishes it mechanically from Tier B except the operator's choice of dedicated topology.
3. `statement_timeout` must be positive and the derived isolation for the route (via `derive_tenant_isolation_mode`) participates in the module's `required_tenant_isolation` floor like every other route.

### 3.3 Execution sequence (Postgres adapter)

```
BEGIN;  SET TRANSACTION READ ONLY;                -- sticky; survives role games
SET LOCAL statement_timeout = <route/options>;
SET LOCAL search_path = <resolved schema>;        -- when query_schema configured
SET LOCAL ROLE <resolved role>;                   -- when role configured
<statement> bound via extended protocol           -- single command, server-enforced
```

- Rows are fetched **batched** with a cap+1 probe: the adapter reads up to `row_cap` rows, peeks for one more, and raises `dynamic_read_row_cap_exceeded` (precondition) rather than silently truncating — silent truncation reads as "the data is small" and produces confidently-wrong dashboards.
- Statement byte-length checked before touching the connection (`dynamic_read_statement_too_large`, validation).
- Tenant id is additionally merged into `params` under the shared `TENANT_PARAM` name **when the statement references the placeholder** (reusing the `unreferenced_param_keys` machinery at call time, comments/literals stripped) — advisory convenience for trusted statements that want a predicate *in addition to* the container; the container remains the boundary (decision 6).

### 3.4 Error taxonomy

| Code | Kind | Cause |
|---|---|---|
| `dynamic_read_write_refused` | precondition | SQLSTATE `25006` — the statement attempted a write/DDL inside the read-only transaction |
| `dynamic_read_statement_invalid` | validation | syntax error / undefined relation / undefined column — **caller-caused**, never `internal` (error-code-hygiene rule) |
| `dynamic_read_multi_statement` | validation | server rejected a multi-command string at the protocol layer |
| `dynamic_read_timeout` | timeout | SQLSTATE `57014` from the local statement timeout |
| `dynamic_read_row_cap_exceeded` | precondition | result exceeded the effective cap |
| `dynamic_read_statement_too_large` | validation | statement above `max_statement_bytes` |
| `tenant_required` / configuration codes | (existing) | reused unchanged from the tenancy/wiring vocabulary |

### 3.5 What this plane refuses to know

- **No field-encryption codecs.** A dynamic statement's output shape is unknowable, so sealed columns come back as **ciphertext** — and worse, a statement may `ORDER BY` one (the sealed-field-sort lesson: ciphertext order is a silent wrong answer). The plane's stance: dynamic read targets analytics-shaped relations (Gold/read models) that must not carry field-encrypted columns; the limitation is documented on the spec, on the config, and **pinned by a battery test** so the behavior is at least deliberate, never discovered. A wiring-time check is impossible (there is no statement to inspect) and pretending otherwise would be worse than the honest boundary.
- **No portability/export participation.** Read-only plane; the inventory has nothing to declare beyond its existence.
- **No HTTP route generator, ever.** Statements arrive from app code (catalog rows, compiler output) — never from an HTTP request body. An `attach_dynamic_read_routes` would be an injection endpoint with a framework logo on it. Foreclosed (decision 8); the MCP surface likewise never exposes raw statement input.

### 3.6 Role provisioning (Tier B enabler)

`PostgresSchemaTenantProvisioner` grows an optional `role: NamedResourceSpec | None = None`: when set, `provision` additionally creates a NOLOGIN role, grants `USAGE` on the tenant schema and `SELECT` on its current tables, and issues `ALTER DEFAULT PRIVILEGES IN SCHEMA … GRANT SELECT` so relations created later (by a pipeline) are covered without re-provisioning. `deprovision` drops the role behind the existing `drop_on_deprovision` gate. The connection user must be a member of the created roles — stated as a deployment prerequisite in the docs, checked at first use with a mapped configuration error rather than a raw `42704`.

### 3.7 Mock + DST

Governance shell (byte cap, row cap+1 probe, timeout clamp, tenancy resolution, taxonomy mapping) lives in a **shared adapter base** — the `ObjectStorageAdapter` pattern — so mock and Postgres differ only in the execute step. The mock's execute is **programmable**: the wiring supplies `handler: Callable[[statement, params], Sequence[JsonDict]]` (the mock-procedure precedent); the handler may raise the taxonomy's errors to script refusal paths. Honesty about the split: engine-enforced refusals (`write_refused`, `multi_statement`, real `25006`/`57014` mapping) are **real-Postgres battery territory** — the mock cannot detect a write in a string and does not pretend to; shared-shell behavior (caps, clamps, tenancy resolution, capture masking) is mock ≡ PG differential territory. Statement text enters DST value capture only under `capture_statements=True`, masked otherwise — the inference `capture_inputs` twin.

## 4. Acceptance battery ("reading isn't proof" — this is refusal logic, exactly where reads deceive)

1. `INSERT`/`UPDATE`/`DELETE`/`CREATE TABLE`/`nextval()` each land `dynamic_read_write_refused`; no effect observable afterward. *(real PG)*
2. `'SELECT 1; RESET ROLE; SELECT * FROM other.t'` lands `dynamic_read_multi_statement`. *(real PG)*
3. With `role` configured: cross-schema `SELECT other_schema.t` refused by grants; same statement without role succeeds — the confinement is the role, provably. *(real PG)*
4. Confinement residuals pinned as **documented-limitation tests** (they exist so the boundary is recorded, not so they pass silently): (a) `DO`-block `RESET ROLE` — still cannot write (read-only sticks) and cannot return rows; (b) the `set_config('role', …)` + `query_to_xml` gadget — demonstrates the row-returning escape that membership-based role switching permits, anchoring the tier table's "adversarial ⇒ dedicated" line to an executable fact. *(real PG)*
5. Timeout fires → `dynamic_read_timeout`; the transaction is cleaned up; the connection is reusable. *(real PG)*
6. Row cap: cap-sized result returns whole; cap+1 raises; `options.row_cap` clamps down, never up. *(mock ≡ PG)*
7. Namespace proof: two tenants, identical statement, different `query_schema` resolutions → disjoint results. *(mock ≡ PG)*
8. `tenant_aware` with no bound tenant → `tenant_required` before any connection use. *(mock ≡ PG)*
9. Sealed column read returns ciphertext — pinned deliberate behavior with the doc reference in the assertion message. *(real PG)*
10. Wiring guards: tagged-tier refusal, `untrusted` without confinement, non-positive timeout — each fails at freeze with its code. *(unit)*
11. DST: a scripted mock handler under forced schedules; capture masked by default, present under `capture_statements=True`.

## 5. Phases

- **P1** — contract + deps + shared shell + Postgres adapter (trusted tier: read-only txn, search_path, limits, taxonomy) + programmable mock + guards + battery 1–2, 5–11.
- **P2** — Tier B: `role` config + `SET LOCAL ROLE` + provisioner role grants + battery 3–4.
- **P3** — docs: new `pages/docs/data-events/dynamic-read.md` (threat-tier table verbatim) + fourth column in the procedures doc's tier table + tenancy-matrix row.

## 6. Decision log

| # | Decision | State |
|---|---|---|
| 1 | Separate plane; `run_dynamic` on `AnalyticsQueryPort` foreclosed — the analytics "no SQL strings" promise stays load-bearing and dynamic read stays greppable in wiring | locked |
| 2 | Read-only plane. Runtime DDL and bulk writes stay on the raw client by policy; a governed dynamic-write surface is foreclosed, not deferred | locked |
| 3 | No SQL parsing/blocklisting anywhere — every refusal is engine-enforced (read-only txn, extended-protocol single statement, role grants) | locked |
| 4 | Resolvable in read-only operations (`dynamic_read_query`); the procedures plane's command-only stance is deliberately inverted | locked |
| 5 | `provenance` is a mandatory config field with no default; `untrusted` without role/dedicated fails at freeze; tagged tier refused for tenant-aware routes | locked |
| 6 | Tenancy boundary is the **container** (schema/role/database); the `%(tenant)s` bound param is advisory convenience, merged only when referenced | locked |
| 7 | Caps ship on with real defaults; exceeding the row cap raises — no silent truncation, no `unlimited` spelling | locked |
| 8 | No HTTP route generator, no MCP raw-statement tool, ever | locked |
| 9 | No pagination/streaming in v1; `run_chunked` demand-gated | proposed |
| 10 | No field-encryption awareness; ciphertext passthrough documented + battery-pinned; dynamic read targets unencrypted analytics relations | locked |
| 11 | Named consumer: Linecust widget/measure execution over per-project schemas (the demand gate this RFC ships against); second consumer candidates: any catalog-driven report surface | recorded |
| 12 | Engine breadth (ClickHouse/DuckDB/BigQuery) + document pipelines split into RFC 0016 — different gating, per-engine enforcement mechanics, no shared hard decisions beyond this contract | locked |
| 13 | Tagged refusal rationale (§2.1): predicate unverifiable at runtime + silent-leak failure mode + injection-requires-the-foreclosed-parser; registered-SQL planes keep tagged because their compensating controls (frozen text, freeze guard, review) all exist | locked |
| 14 | RLS-attested tagged recorded as the one admissible relaxation — engine-enforced, freeze-time policy attestation, **trusted provenance only** (GUC forgeable in-statement), demand-gated with no current consumer | proposed (07-31) |
| 15 | Tier B honesty upgraded (07-31): role confinement is mistake-proofing for non-adversarial generators, not an adversarial boundary — the shared-connection theorem (adapter and statement wield the same identity; `set_config('role')` + execution-time-checked dynamic-SQL builtins return rows across schemas); adversarial authors ⇒ dedicated tier | locked |
| 16 | §3.4 gains `dynamic_read_permission_denied` (precondition) for SQLSTATE `42501`, rather than folding a confinement refusal into `statement_invalid` (08-14). Conflating them would make a cross-tenant attempt indistinguishable from a typo in a log, on the one plane where telling them apart is the point | locked (execution) |
| 17 | §3.4 gains `dynamic_read_role_unavailable` (**configuration**, not caller-caused) when `SET LOCAL ROLE` finds no role or no membership (08-14). It fires before the statement is sent; the original taxonomy would have egressed it as `statement_invalid` and blamed a widget's SQL for a missing `GRANT` | locked (execution) |
| 18 | Statements execute through psycopg's row **streaming** (single-row / chunked mode), not a plain fetch and not a server-side cursor (08-14). A plain cursor buffers the whole result before the cap can refuse it, so the cap would bound the answer but not memory; a named cursor wraps the statement in `DECLARE … CURSOR FOR`, where a write is rejected as a *syntax* error before the read-only transaction gets to refuse it — §4.1's clearest guarantee, lost to a mechanism choice. Streaming also forces the extended protocol unconditionally, which is what makes §4.2 a server-side refusal. **Its price, found in the self-audit and mandatory for any engine adapter that copies this:** stopping early at the cap leaves the generator suspended *holding the driver's connection lock* with the server still sending, and closing the cursor does not release it — the stream must be closed deterministically (`contextlib.aclosing`), or the next borrower of that pooled connection blocks forever. It passes without that only by GC timing, so the regression test asserts the connection's lock/transaction state at the moment the fetch returns rather than issuing a later query | locked (execution) |
| 19 | Each statement runs on its **own** connection (`detached()` + a root transaction), not on the caller's (08-14). `READ ONLY` is a root-transaction property: inside a caller's transaction the scope is a savepoint, the mode silently does not apply, and the plane's one engine-enforced guarantee evaporates. Costs one pooled connection and visibility of the caller's uncommitted writes | locked (execution) |
| 20 | Guard 1 is **retired** in favour of RFC 0037's statement-origin floor (08-14): routes declare `origin="compiled"` and the shared check refuses any tier below `namespace`. Deliberately a **widening**, not a rename — the guard only fired on `tenant_aware=True`, so a route that declared no tenancy scoping at all wired cleanly and read across every tenant, which is strictly worse than the wiring beside it that was refused. Origin is a property of the plane, not of a route flag an author can omit. Cost: a non-tenanted deployment must express its containment as a per-tenant `query_schema` or a routed client, because the lattice cannot tell "single-tenant, nothing to isolate" from "multi-tenant, this route is unscoped" — and fail-closed is the side to be wrong on | locked |
