# RFC 0038 — Compiled read: verified plans on the dynamic-read plane

- **Status:** 📝 Draft — **parked by design** (the RFC 0011 posture). The design is recorded so it is executable the day a trigger fires, not because it should run now: its consumer is a compiler that does not yet emit a stable plan shape, and §10 lists the four facts that shape must settle first. Also gated on RFC 0015 P1 existing — this extends that plane and shares its shell.
- **Scope:** A sibling port in `contracts/dynamic_read/` for statements that arrive **with a declared read set the adapter checks against the parsed text before executing**. Adds `CompiledPlan` (statement + declared reads + output columns + compiler identity + fingerprint), `CompiledSurfaceSpec` (a closed relation set and an accepted-compiler set), and a verification step. Reuses RFC 0015's governance shell wholesale — read-only execution, caps, clamps, tenancy-by-container, error taxonomy, capture masking, mock — and RFC 0037's `compiled` origin floor. **Amends RFC 0015 decision 3** (no SQL parsing) with a new row rather than editing it, and argues the amendment in §2. Does **not** add a new plane, a write verb, an engine dependency beyond one optional parser, or a metrics/semantic-layer model.
- **Related:** RFC 0015 — contract, threat tiers, taxonomy, shell, mock, and decisions 1/3/9/10 which this either inherits or amends by name. RFC 0016 §3 — the shipped precedent for inverting decision 3 *for structured input*, and the line this RFC has to argue across. RFC 0037 — supplies the `compiled` origin and its `namespace` floor, so this RFC states no tenancy rule of its own. RFC 0029 — `AnalyticsAdminPort.verify` / `RelationShape` describe "which relations exist and what shape" from the other direction (§6). RFC 0030 — how the relations a surface reads get published safely. [`integrations/analytics/adapter_common.py`](../src/forze/application/integrations/analytics/adapter_common.py) — `validate_fetch_batch_size`, `shape_rows`, `pagination_window`, `parse_count_row` are the row-shaping helpers to share rather than fork. [`inventory/planes.py`](../src/forze/application/contracts/inventory/planes.py) — `plane_of_spec` (isinstance dispatch), `disposition_of`, `DEFAULT_DISPOSITIONS`.
- **Origin:** A lakehouse / semantic-layer design (a query compiler emitting per-request SQL over per-tenant gold marts) reviewed against the tree. Three of its five proposed gaps turned out to be this plane; the one genuinely new mechanism is the verifiable declaration, which is also the one that collides with a locked decision.

---

## 1. Why this extends RFC 0015 rather than adding a plane

RFC 0015's scope line already names the consumer: *"SQL authored at runtime (by a catalog, **a semantic-layer compiler**, or an agent)"*. A compiler's output is not a new category for that plane — it is the category the plane was written for. What is new is that this particular author **declares what it reads**, and a declaration can be checked.

The mechanics are the same plane's, near-completely:

| Concern | RFC 0015 | Here |
|---|---|---|
| Read-only enforcement | engine (`READ ONLY` txn / `readonly=1` / dry-run) | identical, unchanged |
| Single statement | engine (extended protocol) | identical, unchanged |
| Caps, clamps, timeout | shared shell, cap+1 probe | identical, unchanged |
| Tenancy | container (schema / database / routed client) | identical — floor from RFC 0037 |
| Error taxonomy | `dynamic_read_*` | extended by two codes, same family |
| Mock | programmable handler, shared shell | identical, one field added |
| DST capture | masked unless `capture_statements` | identical |

Two ports at that overlap would be same-concern drift with a second set of defaults to keep in sync — which is exactly how `row_cap: 10_000` and a hypothetical `max_rows: 50_000` end up meaning the same thing and disagreeing. So: **a sibling port in the same package, sharing the spec vocabulary, options, taxonomy and shell.**

That shape is not invented here. RFC 0016 §3 already puts `DynamicPipelinePort` in `contracts/dynamic_read/` with its own dep key (`dynamic_pipeline_query`) for the same reason — a different statement carrier over the same governance. This is the third carrier, and it inherits the property RFC 0015 decision 1 bought: a reviewer greps wiring and finds every route that was granted the capability.

## 2. The parsing question, head-on

**RFC 0015 decision 3 is `locked`: "No SQL parsing/blocklisting anywhere — every refusal is engine-enforced."** Its rationale is one sentence and a good one: *a parser we write is a parser an attacker outgrows.* This RFC's central mechanism is parsing SQL. That has to be argued, not stepped around.

The corpus has already drawn one line here. RFC 0016 §3 admits parsing for Mongo pipelines and states the boundary precisely: *"the doctrine inverts RFC 0015's 'no parsing' rule **for structured input only** (parsing JSON stages is exact; parsing SQL is not)."* By that line, this RFC is on the wrong side. So the amendment needs a distinction the existing two rows do not contain.

There are three uses of a parser, and they are not the same thing:

1. **Parsing as the enforcement boundary** — a blocklist deciding whether a hostile statement may run. RFC 0015 forecloses this, permanently. Nothing here reopens it.
2. **Parsing structured input, exhaustively** — a fail-closed allowlist over a finite, exact grammar (Mongo stage names). RFC 0016 admits this because the parse cannot be imprecise.
3. **Parsing to check a trusted party's claim about its own output** — an equality assertion between a declaration and an AST, catching *bugs in a program we ship*, with a container underneath that holds when the assertion is wrong.

This RFC claims only (3), and it is admissible **only** while all three of these hold — which is why they are decisions, not prose:

- **Trusted origin only.** The compiler is our own release artifact (RFC 0015 Tier A). An untrusted or adversarial author gets nothing from this port that RFC 0015 does not already give it, and its floors apply unchanged.
- **The container is the boundary; verification never is.** RFC 0037 puts `compiled` at `namespace` *because* the check can be wrong. If a parse gap ever becomes the only thing preventing a cross-tenant read, this design has failed and the route belongs at `dedicated`.
- **Refusal is fail-closed and total.** Unparseable is a refusal, an unknown dialect is a refusal, a relation outside the surface is a refusal, and a declared-vs-scanned mismatch in *either direction* is a refusal.

That last point deserves its own sentence because it is easy to weaken into uselessness. Checking only `scanned ⊆ declared` lets a compiler over-declare and learn nothing; checking only `declared ⊆ scanned` lets it under-declare and slip a relation through. **Set equality** is what makes the declaration a commitment rather than a hint, and it is what turns a compiler bug into a loud refusal instead of a query that touched a relation nobody expected.

## 3. Design

### 3.1 Values

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RelationRef:
    namespace: str
    name: str

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CompiledPlan:
    """One rendered statement plus the claims its compiler makes about it."""

    statement: str
    dialect: str

    reads: frozenset[RelationRef]
    """Every relation the compiler says this statement scans. Verified for **set equality**
    against the parsed statement (§2); a mismatch either way is a refusal."""

    columns: tuple[CompiledColumn, ...]
    """Self-describing output shape — name, logical type, role. The one thing this port
    gives callers that ``DynamicReadPort`` cannot: typed metadata without knowing the row
    shape at wiring time."""

    compiler: str
    compiler_version: str
    plan_fingerprint: str
```

`statement`, not `sql` — the field names stay carrier-neutral so a future non-SQL compiler reuses the vocabulary instead of forking it (the `DynamicPipelinePort` lesson, applied before it costs anything).

`plan_fingerprint` is the compiler's, and the port **does not cache**. It is carried so an application can key the shipped `set_versioned` two-level cache off it; a result cache inside the port would silently serve one tenant's rows to another the first time a fingerprint omitted the namespace. Recorded as a decision because it is a tempting five-line addition.

### 3.2 Spec

```python
@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CompiledSurfaceSpec(BaseSpec):
    """A governed set of relations a named compiler may generate statements against."""

    relations: frozenset[RelationRef]
    compilers: frozenset[str]
    row_cap: int = 10_000
    max_statement_bytes: int = 65_536
    capture_statements: bool = False
```

The caps keep RFC 0015's names *and its values* — one plane, one default, changed in one place if ever.

**A separate spec type, not a subclass of `DynamicReadSpec`.** Two reasons, neither of them inventory-related today. First, **spec identity**: the two are separate routes with separate dep keys and separate fingerprints, and a subclass makes one a substitutable stand-in for the other everywhere a `DynamicReadSpec` is accepted — including the dep resolution that is supposed to keep the capability greppable in wiring (§1). Second, **field coupling**: a subclass inherits the parent's field set by construction, so a cap or knob that later needs to diverge between a bare statement and a verified plan cannot, without either breaking the parent or growing a field the parent does not want.

There is also a *latent* inventory hazard worth naming rather than overstating: [`plane_of_spec`](../src/forze/application/contracts/inventory/planes.py) dispatches by `isinstance` over `SPEC_TYPE_PLANES`, so a subclass would inherit its parent's plane row silently. That cannot bite today — neither spec is inventoried (RFC 0015 §3.5, and §5 below) — but it becomes live the moment either stance changes, which §5 records as a possible future. Cheap to foreclose now; invisible if it ever activates.

### 3.3 Port

```python
class CompiledReadPort(Protocol):
    def run(self, plan: CompiledPlan, *,
            options: DynamicReadOptions | None = None) -> Awaitable[Sequence[JsonDict]]: ...

    def select[T: BaseModel](self, return_type: type[T], plan: CompiledPlan, *,
            options: DynamicReadOptions | None = None) -> Awaitable[Sequence[T]]: ...
```

Dep key `compiled_read_query`, accessor `ctx.compiled_read.query(spec)`, resolvable in `QUERY` operations — RFC 0015 decision 4 inherited. `DynamicReadOptions` is reused verbatim, not forked.

**Read-only, no command verb, permanently.** A compiler that generates writes is a different risk profile and gets its own RFC and its own argument, exactly as RFC 0015 decision 2 says for the raw path.

### 3.4 Verification

`integrations/dynamic_read/verification.py`, adapter-neutral, called by the shared shell before the statement reaches a connection:

1. `plan.compiler ∈ spec.compilers`, else refuse.
2. Parse `plan.statement` in `plan.dialect`; unparseable or unknown dialect ⇒ refuse.
3. Scanned relations ⊆ `spec.relations`, else refuse.
4. Scanned relations **==** `plan.reads`, else refuse (§2).
5. No DDL/DML nodes present — belt to the engine's braces, and the one place a parse gap is harmless because the engine refuses independently.

New taxonomy codes, joining the `dynamic_read_*` family: `compiled_plan_unverifiable` (validation — unparseable, unknown dialect, unknown compiler) and `compiled_plan_read_set_mismatch` (precondition — carries both sets in the message, since the diff *is* the diagnosis).

The parser lands as an **optional extra** (`forze[compiled]`), following the `authn`/`oidc` pattern, so core stays lean.

## 4. Verification honesty — the risk this RFC must not hide

RFC 0015's aphorism transposes: **a parser we vendor is a parser our dialects outgrow.** Both failure directions are real and neither is hypothetical:

- **False refusal** — the parser renders a valid engine-specific construct imprecisely, and a correct dashboard query stops working. This is an availability incident, it will happen on a Friday, and it is the *more likely* of the two.
- **Under-detection** — the parser folds away or misattributes a relation reference, and the equality check passes over a statement that reads something undeclared.

Under-detection is survivable only because of RFC 0037: at `namespace` the undeclared relation is either inside the tenant's own container or does not resolve. That is the entire reason the floor is where it is, and it is why this RFC cannot be executed without RFC 0037 landing first.

False refusal is the one with no mitigation in the design, only in the posture: the refusal is loud, names the construct, and is a bug report against the parser pin. An escape hatch — "skip verification for this route" — is **foreclosed**: a route that cannot be verified is a route that belongs on `DynamicReadPort`, where the plane's honesty about what it does not check is already written down.

## 5. Inventory participation — and why the source design's disposition table is a category error

The tempting move is `SpecPlane.COMPILED` with a disposition table mirroring analytics (`PROJECTED → REBUILDABLE`, undeclared → `REFUSED`). It is wrong, and the reason is worth recording so it is not re-proposed.

**A compiled surface owns no rows.** It reads relations that some *other* spec owns — an `AnalyticsSpec` whose `provenance` already decides `REBUILDABLE` vs `REFUSED`, or a lake table with its own answer. Giving the surface a disposition asks the export what to do with data the surface does not have. And [`PlaneDisposition`](../src/forze/application/contracts/inventory/value_objects.py) has exactly four members — `EXPORTABLE`, `REBUILDABLE`, `DRAINED`, `REFUSED` — none of which means "owns nothing by construction". The default for a new plane would land on `REFUSED`, so a surface over perfectly exportable relations would refuse the export of an app that is entirely fine.

RFC 0015 §3.5 already reached the right answer for its own spec — *"the inventory has nothing to declare beyond its existence"* — and that stance survives here **for dispositions**. What does *not* survive is the implied "nothing at all", because a compiled surface declares something no other read spec does: **a closed set of relations it depends on.** That is an edge, and edges are what reconciliation is for. A surface bound over relations nobody catalogued is precisely the drift `reconcile_specs` exists to catch, and it is invisible today.

So: **edges, not a plane.** The surface contributes `REBUILDS_FROM`-adjacent edges from itself to the specs owning its relations, registered with `SpecSource.KIT` in the shipped `spec_contributions` shape ([`progress/inventory.py`](../src/forze_kits/integrations/progress/inventory.py), `AggregateKit.spec_contributions`, `RealtimeTransport.spec_contributions`). No `SpecPlane` member, no disposition, no fifth `PlaneDisposition`.

**Alternative considered:** a fifth disposition meaning "carries no data by construction". Rejected for now — it would be the honest modelling if several read-only planes wanted inventory presence, but exactly one does, and inventing an enum member for one caller is the shape this codebase keeps finding in audits. Recorded so the option is not lost if a second reader plane appears.

## 6. Relation vocabulary — do not fork `RelationShape`

RFC 0029 proposes `AnalyticsAdminPort.verify` over a declared `RelationShape` derived from a spec's own models: *a service refuses to start when the warehouse doesn't match what its code assumes.* This RFC's `CompiledSurfaceSpec.relations` is the same subject — which relations exist and what may be assumed about them — approached from the reading side.

Two vocabularies for that would be a genuine defect, and the composition is valuable in both directions: a surface's declared relations are exactly the set worth attesting at startup, and an attested shape is what makes a compiler's column claims checkable rather than trusted. **Unresolved deliberately** (§10), because RFC 0029 is itself a draft and picking its types before it settles would fix the wrong end. What is decided now: this RFC ships **no** relation-shape concept of its own beyond `RelationRef` (namespace + name), so there is nothing to unify later beyond a rename.

## 7. Pagination — this RFC is RFC 0015 decision 9's missing consumer

RFC 0015 decision 9 is `proposed`, not locked: *"No pagination/streaming in v1; `run_chunked` demand-gated."* The reasoning was that a widget reading past 10k rows is mis-authored.

A semantic layer is the case that reasoning did not cover: a compiled result is a table a UI pages through, and the row cap is a ceiling on the *result*, not a statement about how the caller consumes it. This RFC does not silently add paging — it **names itself as the demand gate** and defers to RFC 0015 to open its own decision. If that decision opens, the methods land on both ports with one implementation in the shared shell, not two.

## 8. Acceptance battery ("reading isn't proof" — refusal logic, where reads deceive)

1. A plan whose scanned relations exceed `spec.relations` is refused before any connection use. *(mock ≡ real)*
2. A plan that **under-declares** — scans a relation inside the surface but omits it from `reads` — is refused with `compiled_plan_read_set_mismatch`, both sets in the message. This is the item that fails if step 4 is ever weakened to a subset check. *(mock ≡ real)*
3. A plan that **over-declares** is refused too — the equality is symmetric, and a hint-shaped declaration is not what was designed. *(mock ≡ real)*
4. Adversarial corpus, each refused, each for its own recorded reason: DDL smuggled inside a CTE; a relation reached through a view alias; an unparseable string; a valid statement in an undeclared dialect; a multi-statement string. *(real engine)*
5. An unknown compiler id is refused even when the statement is otherwise perfect. *(unit)*
6. Verification is **not** the tenancy boundary, proven rather than asserted: with verification stubbed to accept everything, two tenants running an identical plan still return disjoint rows, because the container resolves them apart. This is RFC 0037 decision 3 as an executable fact. *(real engine)*
7. Caps, clamps, timeout and capture masking behave identically on this port and `DynamicReadPort` — asserted by running RFC 0015's shell battery against both, so a fork in the shell fails a test rather than drifting. *(mock ≡ real)*
8. Wiring: a `compiled`-origin route below the `namespace` floor fails at freeze with RFC 0037's message. *(unit)*
9. Reconciliation catches a bound compiled surface whose declared relations name a spec nobody catalogued. *(unit)*
10. Documented-limitation test: a dialect construct the pinned parser renders imprecisely is refused loudly, with the parser version in the message — the false-refusal mode of §4, pinned so it is a known cost rather than a discovery. *(unit)*

## 9. Phases

- **P1** — values, spec, port, verification, shared-shell integration, programmable mock, battery 1–3, 5, 7–9.
- **P2** — the first real engine adapter, battery 4, 6, 10. Engine choice follows the consumer, not this RFC.
- **P3** — inventory edges + reconciliation (§5), docs: a section on the dynamic-read page rather than a new page, since this is one plane.

## 10. Open questions — the four facts that un-park this

Deliberately unresolved. Each is a property of a compiler that does not exist yet, and guessing means designing twice.

1. **Read-set granularity.** Relations, or relations plus columns? Column granularity makes the declaration far stronger and the parser far more dialect-sensitive. *No lean — this is the question the consumer answers.*
2. **Verification cost.** Cheap enough per request, or does it need caching by `plan_fingerprint`? A cache here is a cache of a *security-adjacent* check; if it is needed, its key must include the surface, and that belongs in the design rather than bolted on. *Lean: measure before caching.*
3. **Where the tenant predicate lives** — inside the compiled statement, or in the relation the namespace resolves to. Changes nothing about the floor (RFC 0037 §4) but changes what `columns` and `reads` describe.
4. **Dialect set.** One dialect is a pin; four is a compatibility matrix and a standing maintenance cost that §4's false-refusal risk multiplies by.

Un-park when: the compiler emits a stable plan shape answering 1 and 3, **or** a second consumer of "declared read set verified against the statement" appears. Reject if the compiler never stabilises and the only consumer stays hypothetical — `DynamicReadPort` already serves this workload without verification, less safely and honestly so.

## 11. Decision log

| # | Decision | State |
| --- | --- | --- |
| 1 | A **sibling port in `contracts/dynamic_read/`**, not a new plane — the governance shell, options, taxonomy, caps and mock are RFC 0015's and are shared, not forked. Follows RFC 0016 §3's `DynamicPipelinePort` precedent | locked |
| 2 | **Amends RFC 0015 decision 3.** Parsing is admitted for one use only — checking a trusted party's claim about its own output — and never as an enforcement boundary. Parsing as a blocklist stays foreclosed. This row cites decision 3 rather than editing it; that decision stands for every other use | locked |
| 3 | The amendment holds only while all three conditions hold: trusted origin only, container-is-the-boundary, refusal fail-closed and total. Consequence: if a future consumer wants verification at `tagged`, this design is the wrong one and must be reopened, not configured | locked |
| 4 | Declared vs scanned is **set equality**, not containment in either direction — a subset check makes the declaration a hint, and a hint catches no compiler bugs | locked |
| 5 | No verification escape hatch, ever. A route that cannot be verified belongs on `DynamicReadPort`, which is honest about what it does not check | locked |
| 6 | Read-only, no command verb — RFC 0015 decision 2's reasoning, inherited unchanged | locked |
| 7 | Caps and options are RFC 0015's, by name and by value. A second set of defaults for one concern is how they end up disagreeing | locked |
| 8 | The port carries `plan_fingerprint` but **does not cache**. A result cache keyed on a fingerprint that omits the namespace is a cross-tenant read; caching is the application's, over the shipped `set_versioned` | locked |
| 9 | Inventory participation is **edges, not a plane or a disposition** — a compiled surface owns no rows, and `PlaneDisposition` has no member meaning "owns nothing" (§5). A fifth member is the recorded alternative if a second reader plane ever appears | locked |
| 10 | A separate spec type, not a `DynamicReadSpec` subclass — distinct identity and dep key (a subclass would be substitutable wherever the parent is accepted, eroding decision 1's greppability) and no inherited field set to fight when a knob needs to diverge. The `isinstance` plane-dispatch hazard is *latent*, not active: neither spec is inventoried today, and it only becomes live if §5's stance is ever reversed | locked |
| 11 | Ships no relation-shape concept beyond `RelationRef`; unifying with RFC 0029's `RelationShape` is deferred until that RFC settles, so there is nothing to unwind later beyond a rename | recorded |
| 12 | Field names are carrier-neutral (`statement`, not `sql`) so a non-SQL compiler reuses the vocabulary rather than forking it | locked |
| 13 | This RFC is RFC 0015 decision 9's demand gate for pagination; it does not add paging unilaterally, and if that decision opens the methods land in the shared shell once | recorded |
| 14 | Parser ships as an optional extra (`forze[compiled]`), the `authn`/`oidc` pattern; core acquires no parsing dependency | locked |
| 15 | Parked until §10's questions are answered by a real consumer. Named triggers and a named rejection condition, per the RFC 0011 posture | recorded |
