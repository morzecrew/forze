# RFC 0042 — Skills coverage ratchet: every shipped package triaged, every choice point proven

- **Status:** ✅ Complete — executed 2026-08-16 on `feature/skills-coverage-ratchet`, closing the skills family (0040 gates, 0041 structure, 0042 coverage). Five departures are recorded in [`EXECUTION-LOG.md`](EXECUTION-LOG.md) unit 3 (drift count 0), accepted as the §8 decision table; two were **halts on locked material** resolved before any file was written. Read rows 1 and 2 before trusting §4.1's unit count or §5.1's `mock-server` row, and row 3 before following §4's evidence links, which point at skills RFC 0041 deleted. The doctrine and the ratchet are locked; the per-package content work is **triaged, and only the D1 tranche is committed** (§4). Depends on RFC 0040 for the census that measures it and the gate that holds it; composes with RFC 0041 but does not require it — the doctrine applies to 21 skills or to one skill with 48 references equally.
- **Scope:** Deciding what the published skills corpus must cover, and enforcing that decision mechanically. Triages all 29 shipped wheel packages into coverage doctrines, commits to closing the D1 gap, and converts RFC 0040's report-only census into a non-regressing ratchet so a new integration package cannot ship with zero corpus reach. Does **not** rewrite existing covered content, and does **not** decide corpus structure (RFC 0041).
- **Related:** RFC 0040 §3.1 (the import gate whose reach this RFC extends) and §3.5 (the census this RFC ratchets). [`pyproject.toml`](../pyproject.toml) `[tool.hatch.build.targets.wheel]` — the authoritative package list, and per [`AGENTS.md`](../AGENTS.md) the thing agents must read instead of maintaining an integration list by hand. RFC 0010 is the shape this RFC borrows: triage every backend into an explicit doctrine so that "not covered" is always a recorded decision rather than an oversight, plus a conformance floor every future member must clear.
- **Origin:** Running RFC 0040's census for the first time. The headline number is fine — 19 of 29 shipped packages appear in an executable import, and all 236 import pairs resolve. The distribution is not. `forze_postgres` is imported in four skills; **`forze_mongo` in none**, though the same skill's own description advertises "Postgres / Mongo / Firestore / Meilisearch backends". [`forze-graph-contracts`](../skills/forze-graph-contracts/SKILL.md) is a skill *about* the Neo4j integration that never imports `forze_neo4j`. The corpus is Postgres-shaped: the default backend gets code, every alternative gets prose.

---

## 1. The measurement

Against `[tool.hatch.build.targets.wheel]` — 29 packages, the authoritative set:

| | Packages |
|---|---|
| **Reached by an executable import** (19) | `forze`, `forze_kits`, `forze_mock`, `forze_identity`, `forze_bigquery`, `forze_clickhouse`, `forze_fastapi`, `forze_gcs`, `forze_http`, `forze_inference`, `forze_inngest`, `forze_kms`, `forze_postgres`, `forze_rabbitmq`, `forze_redis`, `forze_s3`, `forze_sqs`, `forze_temporal`, `forze_vault` |
| **Never appear in any code block** (10) | `forze_cli`, `forze_dst`, `forze_duckdb`, `forze_firestore`, `forze_kafka`, `forze_mcp`, `forze_meilisearch`, `forze_mongo`, `forze_neo4j`, `forze_socketio` |

Note the earlier extras-keyed count is superseded and was wrong. `[project.optional-dependencies]` has 33 keys but they do not map one-to-one onto packages: `authn` and `oidc` are `forze_identity` submodules, `kms-aws`/`kms-gcp`/`kms-yc` are all `forze_kms`, `inference-http`/`inference-sagemaker` are `forze_inference`, and `observability` and `zstd` ship no package at all. Extras are the *install* surface; wheel packages are the *import* surface, and it is imports the corpus makes claims about. This is recorded because getting it wrong produced a plausible, quotable, incorrect number on the first attempt.

### 1.1 Package granularity hides gaps — the trap this census must not fall into

Keying on packages fixes the denominator and introduces a subtler error. A package scores **covered** the moment *one* symbol from it is imported, and three of the largest are multi-backend:

| Package | Ships | Imported by the corpus | Never imported |
|---|---|---|---|
| `forze_kms` | `local`, `aws`, `gcp`, `yc` | `local`, `aws` | **`gcp`, `yc`** |
| `forze_inference` | `http`, `sagemaker` | `http` | **`sagemaker`** |
| `forze_identity` | `authn`, `authz`, `oidc`, `oauth`, `tenancy`, `builtin` | `authn`, `oidc`, `tenancy` | **`authz`, `oauth`, `builtin`** |

All three score green at package level. `forze_identity.authz` in particular is covered by exactly one sentence — *"`forze_identity.authz` provides document-backed authorization (catalog, bindings, adapters for authz ports)"* — with no code block anywhere in the corpus, which is §2's unverifiable-prose failure wearing a passing grade.

This is the census failure mode the conformance ratchet already taught: **a census that counts at the wrong granularity reports green on an unseeded plane.** The fix is not to abandon packages for extras but to combine them:

> **Census unit = the import root, subdivided wherever an extra draws a boundary inside a package.**

That reconciles the two lists rather than picking one. Extras are exactly the repository's own record of *where an application author makes a choice* — `kms-aws` versus `kms-gcp` is a decision someone makes, which is why they are separate extras — while packages remain the unit an import statement can be checked against. `forze_kms.gcp` becomes a census unit because the `kms-gcp` extra says it is a choice point; `forze_identity.oauth` does not, because nothing marks it as one.

`forze_identity.authz` falls outside that rule (no `authz` extra) and is nonetheless a real gap on its own merit. It is triaged explicitly in §4 rather than being allowed to fall through the mechanism — the rule sets the floor, it does not cap judgement.

## 2. The defect is not "missing pages" — it is unverifiable prose

The uncovered ten are not undocumented. The prose is specific and names real symbols:

> Firestore wires them via `FirestoreDepsModule(ro_documents=..., rw_documents=...)` with `FirestoreReadOnlyDocumentConfig` / `FirestoreDocumentConfig`. — [`forze-documents-search`](../skills/forze-documents-search/SKILL.md)

Nine of ten such prose-only symbols spot-checked against the installed packages resolve correctly. The corpus is not lying. But this shape has two properties that matter more than accuracy:

**No gate can see it.** RFC 0040's import gate walks `ast`-parsed code blocks. A symbol named in a sentence is invisible to it. So the corpus's 236 mechanically-verified claims cover 19 packages, and the remaining ten packages' claims sit in exactly the place where verification cannot reach. Coverage and verifiability are the same problem here: **the gate's reach is bounded by where the corpus puts its symbols.** Every prose-only symbol is a permanent hole in RFC 0040.

**A named symbol without an import path is not actionable.** [`forze-realtime`](../skills/forze-realtime/SKILL.md) names `attach_realtime_ws_route` in a transport table with no module. Checking it during this RFC's preparation, it was not in `forze_socketio` (where the table's neighbouring row would suggest), not in `forze_fastapi`, and not in `forze_fastapi.routes` — it lives in `forze_fastapi.realtime`, findable only by grepping `src/`. The claim is correct and still cost three wrong guesses *with the repository open*. An agent in a consumer repository has nothing to grep. That is the failure mode: not wrong advice, but advice that cannot be executed.

The corollary is that this RFC's unit of work is mostly **converting prose claims into gate-visible code**, not writing new pages. §4 commits to **13 units — 10 D1 plus 3 D2** — and for **12 of the 13** the fix is a four-line import-plus-wiring block next to a paragraph that already exists.

`forze_dst` is the single exception and is flagged rather than smoothed over: it has no prose to convert, because it has no coverage at all. Its content is genuinely new writing, carried by RFC 0041 §6 as two reference files. Estimating it alongside the import-block work would understate it by an order of magnitude.

## 3. Doctrine

Following RFC 0010's pattern — every member gets an explicit doctrine, so "uncovered" is always a decision on the record.

**D1 — Worked example required.** The package is a backend an application author *chooses* at wiring time, where choosing wrong is expensive and the config surface is not inferable from a sibling. Must have at least one importable block showing its deps module and its config type. This is the coverage floor.

**D2 — Import anchor sufficient.** The unit's surface is reached identically to an already-covered sibling and differs only in a config object. A **gate-resolved** import plus the config type, no full walkthrough. D2 is a smaller *example*, never a weaker *proof*: the import must resolve under RFC 0040 §3.1 exactly as D1's does (§5).

**D3 — Out of app-author scope.** Per [`skills/AUTHORING.md`](../skills/AUTHORING.md), skills target engineers building applications *on* Forze, not contributors to it. Packages that only framework maintainers touch are correctly absent, and the census must record them as intentional rather than counting them as debt.

**D4 — Deliberately deferred.** In scope, not yet worth the words, with a recorded trigger.

**No unit currently sits in D4.** `forze_dst` was its only member and was promoted to D1 (§4). The doctrine stays defined because the ratchet needs the vocabulary — the next integration to land will plausibly want it — but a reader should not go hunting for a D4 row that does not exist. An empty doctrine is a healthier state than a populated one: it means nothing in scope is currently being put off.

## 4. Triage

| Package | Doctrine | Rationale |
|---|---|---|
| `forze_mongo` | **D1** | A first-class document backend. [`forze-documents-search`](../skills/forze-documents-search/SKILL.md) advertises it in its own frontmatter description and never shows it. Largest single gap. |
| `forze_meilisearch` | **D1** | The non-Postgres search backend; `MeilisearchDepsModule` / `MeilisearchSearchConfig` / `MeilisearchFederatedSearchConfig` are prose-only. Federated search has no other home. |
| `forze_firestore` | **D1** | Third document backend, distinct config split (`ro_documents` / `rw_documents`) that is *not* inferable from the Postgres example. |
| `forze_neo4j` | **D1** | [`forze-graph-contracts`](../skills/forze-graph-contracts/SKILL.md) exists to cover it and instructs *"prefer `forze_neo4j` when Neo4j fits"* without ever importing it. |
| `forze_socketio` | **D1** | [`forze-realtime`](../skills/forze-realtime/SKILL.md) covers three transports; the gateway is the one with real wiring, and §2's unresolvable-symbol case is here. |
| `forze_kafka` | **D1** | The fourth delivery model (commit-stream groups) — semantically distinct from the queue and stream models the skill does show, so nothing transfers by analogy. |
| `forze_mcp` | **D1** | [`forze-fastapi-interface`](../skills/forze-fastapi-interface/SKILL.md) has an §Exposing operations over MCP section with no import. |
| `forze_duckdb` | **D2** | Reached through the analytics plane like BigQuery and ClickHouse, both covered; only the config object differs. |
| `forze_dst` | **D1** | Promoted from D4. Simulating *your own* application is a first-class part of building on this framework — nine authored docs pages, its own nav section, a 31-name public surface — and it currently has no skill coverage of any kind. RFC 0041 §6 now allocates it two reference files (`dst-simulation`, `dst-invariants`); the D1 floor is met by those, not by a token import. **This is the one D1 unit whose content is net-new writing rather than a move.** |
| `forze_cli` | **D3** | Maintainer tooling. Recorded as intentionally absent. |

And the sub-package units §1.1 makes visible, which a package-level census scores as already covered:

| Unit | Doctrine | Rationale |
|---|---|---|
| `forze_kms.gcp` | **D2** | A KEK backend selected by extra. Reached identically to `forze_kms.aws`, which is covered; the provider construction differs. |
| `forze_kms.yc` | **D2** | Same, Yandex Cloud. |
| `forze_inference.sagemaker` | **D1** | Not an anchor case: SageMaker's endpoint/serialization model differs materially from the covered `forze_inference.http` (KServe-v2 / MLflow), so nothing transfers by analogy. |
| `forze_identity.authz` | **D1** | Outside the extras rule (no `authz` extra) and triaged on merit: authorization is a decision every application makes, and today it is one sentence with no code. §1.1's worked example of granularity hiding a gap. |

Committed scope: **ten D1 units plus three D2 anchors — 13 in all.** `forze_cli` (D3) is the only recorded exclusion.

### 4.1 The other 19 units — doctrine by observation

§5 requires *every* census unit to carry a doctrine, and the tables above assign one only to the gaps. The 19 units already reached by an executable import (§1) are not thereby exempt; leaving them unassigned would make the no-doctrine-is-an-error rule unimplementable, since the checker cannot tell "covered, therefore fine" from "never triaged".

They are assigned **D1 by observation**: each already meets D1's floor, which is what being reached by a gate-resolved import means. The manifest records them explicitly rather than inferring them, so the doctrine map is **total over the unit list** and the enforcement rule has something to check for every entry.

The distinction that keeps this honest: a unit is D1 because someone decided it must be demonstrable, and *separately* it is currently passing. Recording "D1, satisfied" is a decision plus a measurement; inferring "covered, so no doctrine needed" would let a unit that silently loses its last import slip from covered to untriaged without anything failing.

## 5. The ratchet

RFC 0040 §3.5 reports the census. This RFC gives it teeth, in the shape the conformance work already established: **a derived census is the ratchet, and it must measure consumption rather than declaration.**

- The checker derives its unit list from `[tool.hatch.build.targets.wheel]` **cross-referenced against `[project.optional-dependencies]`** per §1.1 — both sides derived, neither hand-maintained. This is the failure mode `AGENTS.md` explicitly warns about and the mechanism by which a new integration would otherwise be invisible.
- Each unit carries its doctrine in a small manifest committed beside the checker.
- **A unit with no doctrine is an error, not a default.** Adding `src/forze_opensearch/` to the wheel targets — or adding a `kms-azure` extra that subdivides an existing package — fails the build until someone writes down which doctrine it falls under. That is the entire point: a new plane must not be able to ship with zero corpus reach *and* zero decision. Note the extra case specifically: it creates a new census unit **without adding a package**, which a wheel-targets-only ratchet would never notice.
- **D1 and D2 both require an import that RFC 0040's import gate resolves.** They differ in how much surrounding material is expected — D1 a worked deps-module-plus-config block, D2 a bare anchor — never in whether the import is *verified*. An earlier draft said D2 needed "an import line", which would have let unresolvable text satisfy the floor and contradicted the consumption rule two bullets down. D3 and D4 require a written rationale, and D4 additionally a trigger.
- The count of D1 and D2 units meeting the floor may not regress.

### 5.1 Extras do not name import paths

`kms-gcp` and `forze_kms.gcp` are related by a convention, not by a rule, and the checker cannot subdivide a package on a hunch. `kms-gcp` → `forze_kms.gcp` reads obvious; `mock-server` → `forze_mock.server` is a guess; `observability` and `zstd` correspond to no module at all; and nothing prevents a future `kms-azure-gov` whose dashes do not decompose.

So the manifest carries the mapping **explicitly, one line per subdividing extra**, and the checker never derives a submodule name from an extra name:

| Extra | Census unit |
|---|---|
| `kms-aws` / `kms-gcp` / `kms-yc` | `forze_kms.aws` / `.gcp` / `.yc` |
| `inference-http` / `inference-sagemaker` | `forze_inference.http` / `.sagemaker` |
| `authn` / `oidc` | `forze_identity.authn` / `.oidc` |
| `mock-server` | *(to be confirmed at execution — the module is not named by the extra)* |
| `observability`, `zstd` | *(no module; recorded as dependency-only, not a census unit)* |

Two validation rules make the mapping self-checking rather than a table that rots:

1. **Every extra must appear in exactly one row** — mapped to a unit, or explicitly marked dependency-only. An extra in neither category fails the build. This is what makes a newly added extra impossible to ignore.
2. **Every mapped unit must be importable.** A row naming `forze_kms.gcp` fails if that module does not exist, so a rename in `src/` breaks the manifest loudly instead of silently dropping a unit from the denominator.

The dash-to-dot convention may be used as a *suggestion* when a new extra appears, never as the answer. Rule 2 is what stops a plausible guess from becoming a phantom unit that always reports covered because nothing ever checks it.

**Consumption, not declaration.** A package counts as covered only when a symbol from it appears in a code block that RFC 0040's import gate actually resolves. Naming it in prose, in a table, in a frontmatter description, or in the `skills/README.md` table does not count — that is precisely the condition the corpus is in today and precisely what a declaration-based census would score as green.

## 6. Execution

Per package, the unit is small: an import, a deps-module construction with its real config type, and one line of what is distinctive about it. Attach to the existing section rather than creating a new page — operating rule 1 in `AGENTS.md`, and these sections already exist.

1. Land RFC 0040 (census exists, report-only, package-keyed).
2. Extend the census to §1.1's unit rule — import root subdivided by extra boundary — and re-measure. This must come **before** the content work, or the content work is aimed at the wrong list.
3. Ten D1 units plus three D2 anchors, in one PR or a small series. `forze_dst` is the outlier: it is net-new writing carried by RFC 0041 §6, so sequence it with that RFC rather than with the import-block additions. Each addition is verified by the import gate the moment it is written.
4. Add the doctrine manifest and flip the census from report-only to enforcing, with the no-doctrine-is-an-error rule.
5. Re-run; every unit either reached by a gate-resolved import or carrying a written doctrine, with `forze_cli` (D3) the only deliberate exclusion.

Sequencing against RFC 0041 is free in either order — coverage added before the consolidation moves with its section; coverage added after lands in the reference file the map already assigns. If both are in flight, do coverage **first**: adding a block to an existing section is a smaller merge conflict than adding one to a file being simultaneously split.

## 7. Success criteria

Stated **by doctrine, not by inventory** — a criterion naming today's exceptions would misjudge the first unit that legitimately lands in D3 or D4 tomorrow:

- **Every D1 and D2 unit** is reached by an import that RFC 0040's gate resolves.
- **Every D3 unit** carries a written rationale for being out of app-author scope.
- **Every D4 unit** carries a rationale *and* a trigger that would move it into D1 or D2.
- **Every unit has a doctrine** — enforced, not reviewed.

Against the current inventory that resolves to: all units gate-resolved except `forze_cli`, with D4 empty. Those are the values today, not the rule.
- **Injected regression, per RFC 0040 §7, and it must be run twice:** add a stub package to the wheel targets and confirm the census fails until a doctrine is recorded — then add a stub *extra* subdividing an existing covered package and confirm it fails too. The second is the one a naive implementation passes by accident, because the package it subdivides is already green. A ratchet nobody has seen catch anything is not known to work — and the specific trap this one must survive is the conformance census's own lesson: *a census that counts declarations instead of consumption reports green on an empty plane.*

---

## 8. Decision log

Added by execution 2026-08-16 — see [`EXECUTION-LOG.md`](EXECUTION-LOG.md) unit 3. The prose
above is left as designed; where a row overrides a section, the section says so and is not
rewritten.

| # | Row | Grade | From |
|---|---|---|---|
| 1 | **Subdivision adds a census unit; it never replaces the root.** The unit list is every wheel package, plus every boundary an extra draws inside one, plus any unit added on merit — 38 today. A package can gain an uncovered submodule while its root stays green on a sibling's import, which is the failure §1.1 exists to catch. §4.1's "the other 19 units" is the superseded package-keyed count. | `LOCKED` | D-011 |
| 2 | **Confirming an extra's module mapping also assigns it a doctrine.** §5.1's open `mock-server` row named a module and thereby created a unit; leaving it untriaged would have failed the build on day one over a decision the RFC never made. `forze_mock.server` is **D2**. | `ASSUMED` | D-012 |
| 3 | **`forze_dst`'s D1 obligation is discharged by RFC 0041's reference pair**, so the content tranche is nine new D1 units, not ten. It reaches 13 again only because row 2 adds one. §4's evidence links point at the 21 skills RFC 0041 deleted; destinations were re-derived against the 43 reference files. | `ASSUMED` | D-013 |
| 4 | **The unit rule is implemented and measured before the content work, and committed after it.** §6's ordering constraint is about aim, not about commit boundaries: an enforcing census with 13 unproven units is a commit that fails its own gate. | `ASSUMED` | D-014 |
| 5 | **A census with no D1 or D2 unit is refused.** "0/0 proven" is a passing ratio, and a manifest triaging every package out of scope would read as full coverage. | `ASSUMED` | C-3 |
