# RFC 0041 — Skills consolidation: one published skill with lazy references

- **Status:** 📝 Draft — design locked including the full reference-file map (§6) and a **hard-cut migration** (§7): the 22 directories are deleted, not deprecated, licensed by there being no external installed base. Breaking, and deliberately so. Depends on RFC 0040 only for its index-parity check; can be written in parallel but should land after it.
- **Scope:** Replacing the 22 independently-published skills under [`skills/`](../skills/) with a single published skill, `skills/forze-skills/`, whose `SKILL.md` is a routing index over **48** lazily-read reference files. Covers the packaging decision, the installer constraints that bound it, the exact file map, the routing-table contract, and the migration for existing installs. Beyond the mechanical split and the seam-repair it forces, the only net-new writing is the **DST pair** (§6) — `forze_dst` is a key building block with a nine-page docs section and no skill at all today. Does **not** add coverage for the other currently-uncovered integrations (RFC 0042).
- **Related:** [`skills/README.md`](../skills/README.md) — the published table and the per-skill install instruction this RFC retires. [`skills/AUTHORING.md`](../skills/AUTHORING.md) — maintainer rules that need rewriting for the new shape, including its "Retired (merged) skill names" section, which already establishes the precedent for this kind of breaking rename. RFC 0040 §3.3 supplies the index↔reference parity check without which this structure silently rots. RFC 0042 adds reference files into the map this RFC fixes.
- **Origin:** A proposal to restructure `skills/` after [`leonardomso/rust-skills`](https://github.com/leonardomso/rust-skills) — one `SKILL.md` index over 265 lazily-loaded rule files. Assessing it produced a split verdict that this RFC encodes: the **packaging** transfers and is an improvement; the **rule atomization** does not transfer, because rust-skills' rules are orthogonal and normative while Forze's skills are compositional and procedural. Taking the first without the second is the whole design.

---

## 1. What is actually wrong with 22 skills

Not "too many files". Three specific defects, in ascending order of how much they cost.

**The advertised per-skill install produces broken navigation.** [`skills/README.md`](../skills/README.md) tells users they can run `npx skills add morzecrew/forze@forze-wiring`. That installs one directory. `forze-wiring`'s six `../forze-*/SKILL.md` links then point at nothing; `forze-fastapi-interface` has eight. The corpus has 55 cross-links total and is a connected graph, so *every* partial install is broken — the feature as documented cannot work. It is not a bug to be fixed in place: per-skill install and a densely cross-linked corpus are incompatible by construction, and one of them has to go.

**Discovery cost is unconditional.** 777 words of frontmatter description (≈1.1k tokens) load into every session in every consumer repository, whether or not the task touches Forze. One skill costs ≈80. This is the smallest of the three wins and should not be the argument — but it is a real, permanent, per-session tax paid by every user.

**Granularity is topic-shaped, not need-shaped — the expensive one.** Two files carry a quarter of the corpus and do many unrelated jobs:

| Skill | Words | `##` sections | Distinct jobs |
|---|---|---|---|
| [`forze-wiring`](../skills/forze-wiring/SKILL.md) | 2,090 | 11 | runtime, inventory, AggregateKit, composition, FastAPI, mapping, mock testing, outbox, search |
| [`forze-auth-tenancy-secrets`](../skills/forze-auth-tenancy-secrets/SKILL.md) | 1,929 | 16 | authn, FastAPI identity, OIDC, authz, tenancy, isolation tiers, admin plane, provisioning, secrets |

"How do I test with the mock?" costs the whole 3k-token wiring file for a ~300-token answer. Worse, this is the *cross-link* cost too: every one of the 55 links that says *see `forze-wiring`* charges 3k tokens to answer a one-section question. The corpus's own internal structure is the thing making it expensive to read.

## 2. Why rust-skills' packaging transfers and its granularity does not

rust-skills is 265 rules across 26 prefix-namespaced categories. Each rule is **normative** (do X, not Y), **orthogonal** (applies independently of every other rule), and **self-contained** (`own-borrow-over-clone.md` needs nothing else to be actionable). At that count an index is not a design choice, it is forced — 265 descriptions cannot be frontmatter. And atomization is free, because the atoms were already independent.

Forze's skills are **procedural** and **compositional**. Declaring a governed aggregate requires the models, the spec, the deps module, the kit and the runtime *together*, in order. The 55 cross-links are the evidence: this is a graph of mutually-dependent procedures, not a flat rule set. Shredding a procedure into rule-shaped atoms would fragment exactly the sequences an agent needs whole, and would replace one over-large read with six under-informative ones.

So: **adopt the index-plus-lazy-references packaging; reject the rule granularity.** The target is ~48 reference files of *one job each*, where a job is a complete procedure or a complete lookup — not ~200 rules. Concretely, the splitting rules are:

1. **One job per file.** A job is something a reader came to do ("wire the runtime", "write a query", "rotate a key").
2. **Never split a procedure across files.** If steps 1–5 must be followed in order, they are one file, however long.
3. **60–250 lines.** Under 60 usually means it belongs with its neighbour; over 250 means it is two jobs — the two files in §1's table are 450 and 327 lines.
4. **A file that is only ever read together with another is not a separate file.** This is the direct guard against rust-skills-style over-atomization.

## 3. Naming, and why the directory is not the interesting part

Locked: repository directory **`skills/forze-skills/`**, frontmatter **`name: forze-skills`**.

The directory choice avoids ambiguity with `src/forze/` and the `forze` PyPI distribution. But the mechanically important fact is that **the directory basename is not what users get.** In the installer (`skills@1.5.22`, `dist/cli.mjs:2047`):

```js
const skillName = sanitizeName(skill.name || basename(skill.path));
```

Frontmatter `name` wins; basename is a fallback. So the installed path is `.claude/skills/forze-skills/` and the command is `/forze-skills`, driven by frontmatter, not by the repository layout. Keeping the two identical is the decision here: it means the `@selector` resolves under either spelling (`cli.mjs:1121-1122` normalizes both), and no maintainer ever has to hold two names in their head.

## 4. Installer constraints — the hard boundaries

Established by reading `skills@1.5.22` (`vercel-labs/skills`; the `add-skill` package rust-skills' README uses is a deprecated alias for the same CLI, so both repositories are subject to identical rules).

**A root-level `SKILL.md` is forbidden here.** Discovery walks to `maxDepth = 5`; if the search root itself contains a `SKILL.md` it is taken as *the* skill and returned immediately. Installation is `copyDirectory(skill.path, dest)` — **fully recursive over the containing directory**. rust-skills can put `SKILL.md` at its repository root because that repository is 443 KB of nothing but rules. Doing the same here would attempt to copy the entire Forze monorepo into users' `.claude/skills/`. The skill must be nested.

**`skills/` is a priority search directory.** `prioritySearchDirs` is `[searchPath, searchPath/skills, skills/.curated, skills/.experimental, skills/.system]`, so `skills/forze-skills/SKILL.md` is found without configuration.

**Everything under the skill directory ships.** A direct consequence of the recursive copy: anything that is not for the reader must live outside it. This is why RFC 0040 places the checker at `tools/skills_check/` rather than `skills/.checks/`, and it fixes the placement of two existing files — [`skills/README.md`](../skills/README.md) and [`skills/AUTHORING.md`](../skills/AUTHORING.md) **stay at `skills/`, one level above `skills/forze-skills/`**. `AUTHORING.md` is explicitly maintainer-facing (*"not published as an installable skill"*) and moving it inside would ship the repository's internal authoring policy into every consumer's `.claude/skills/`. The current layout already gets this right by accident of `*/SKILL.md` discovery; after consolidation it has to be right on purpose.

**The installer cannot uninstall the old skills.** Installation is `cleanAndCreateDirectory(dest)` — `rm -rf` on *the target name*, then copy. Since `forze-skills` is a different name from `forze-wiring`, installing the new skill **leaves all 22 old directories in place**, stale, in any repository that ever installed them: still loading, still consuming description budget, still advising. Removing the sources from this repository does not remove the copies. That is a property of the tool, not something this RFC can design around; §7 records why it is nonetheless not a cost here.

## 5. The index is the product

With 22 skills, routing is done by the harness's own description matcher: it is free, it happens before any tool call, and it is precise. Consolidation **gives that up** and replaces it with model judgment reading an index. This is a genuine regression in the routing mechanism and must be compensated, not hand-waved.

The failure mode is specific: the agent activates `forze-skills`, reads the index, and fetches **one** reference when the task needed three — producing confidently incomplete wiring. rust-skills does not have this problem because its rules are independent; a Rust agent that loads 4 of 6 relevant rules still writes better code. A Forze agent that loads `aggregate-kit.md` without `spec-to-backend-config.md` writes an app that does not start.

So the index carries two things a flat link list does not:

**Bundles, not just entries.** The routing table is keyed by *task*, and a task names every reference needed to complete it:

| I want to… | Read |
|---|---|
| Bootstrap a new service | `architecture` → `spec-naming-and-routes` → `deps-resolution` → `runtime-lifecycle` |
| Add a governed aggregate | `aggregate-models` → `document-spec` → `aggregate-kit` → `spec-to-backend-config` → `testing-with-mock` |
| Write a custom handler | `execution-context` → `handlers` → `query-dsl` |
| Expose it over HTTP | `fastapi-setup` → `fastapi-generated-routes` → `fastapi-identity` |
| Encrypt a field | `field-encryption` → `kms-backends` → `spec-to-backend-config` |
| Simulate my service under faults | `dst-simulation` → `dst-invariants` → `testing-with-mock` |

**A stated read-more-than-one norm.** The index says explicitly that most tasks need a bundle and that reading one reference is usually wrong. That instruction is cheap and directly targets the failure mode.

Consequence for review: the index's routing table, not the file split, is where this RFC's risk concentrates. It should get the most scrutiny.

## 6. The reference map — locked

48 files. Every current `##`/`###` section has exactly one destination; nothing is dropped. Provenance is given so execution is mechanical and so review can argue with the decomposition rather than with an abstraction. Two of the 48 — the DST pair — have **no source skill** and are net-new writing; see the note following the tables.

### Foundations

| Reference | Source |
|---|---|
| `architecture.md` | `framework-usage` §Core concepts → Layered architecture, Contracts and adapters |
| `execution-context.md` | `framework-usage` §Core concepts → Execution context, Handler pattern, Transactions, Identity and tenancy |
| `handlers.md` | `framework-usage` §Common patterns (all five), §Gotchas |

### Specs, deps, wiring

| Reference | Source |
|---|---|
| `spec-naming-and-routes.md` | `specs-infrastructure` §Prefer `StrEnum` names, §Transaction routes, §Gotchas |
| `spec-to-backend-config.md` | `specs-infrastructure` §DocumentSpec vs Postgres/Mongo, §Redis cache/counters/locks/idempotency, §Storage, queue, and workflow routes |
| `deps-resolution.md` | `deps-consumption` (whole) |
| `deps-custom-module.md` | `custom-deps` (whole) |
| `runtime-lifecycle.md` | `wiring` §Runtime setup (all three `###`), §Declare the spec inventory |
| `operation-composition.md` | `wiring` §Document composition (all four `###`), §Mapping steps |
| `testing-with-mock.md` | `wiring` §Testing with Mock |

### Domain

| Reference | Source |
|---|---|
| `aggregate-models.md` | `domain-aggregates` §Document aggregate structure, §Document base fields, §Mixins, §Update validators |
| `document-spec.md` | `domain-aggregates` §DocumentSpec, §SearchSpec, §Database schema alignment, §DocumentDTOs |
| `aggregate-kit.md` | `wiring` §Governed aggregates: AggregateKit |

### Reading and writing data

| Reference | Source |
|---|---|
| `document-facade.md` | `documents-search` §Document access, §Custom operations and raw ports, §Adapter boundaries |
| `query-dsl.md` | `documents-search` §Query DSL (+ `###`), `framework-usage` §Query syntax |
| `search.md` | `documents-search` §Search with `SearchFacade`, §Hub and federated search, §Rebuilding a search index; `wiring` §Search composition |
| `caching.md` | `documents-search` §Cache-aware documents |

### Events, messaging, realtime

| Reference | Source |
|---|---|
| `messaging-queues.md` | `messaging-streaming` §Queue contracts, §SQS and RabbitMQ wiring |
| `messaging-pubsub-streams.md` | `messaging-streaming` §Pub/sub contracts, §Stream contracts, §Processing rules, §Shutdown |
| `outbox-notifications.md` | `wiring` §Transactional notifications |
| `realtime-catalog.md` | `realtime` §Declare the event catalog, §Publish from a handler |
| `realtime-transports.md` | `realtime` §Three transports one protocol, §Offline delivery, §Wiring notes that bite, §Testing |

### Durable execution

| Reference | Source |
|---|---|
| `temporal.md` | `temporal-workflows` (whole) |
| `inngest.md` | `inngest-durable-functions` (whole) |

### Interface

| Reference | Source |
|---|---|
| `fastapi-setup.md` | `fastapi-interface` §Context dependency and lifespan, §Middleware, errors, and docs; `wiring` §FastAPI integration |
| `fastapi-generated-routes.md` | `fastapi-interface` §Generated routes, §Hand-written routes, §Readiness and deadline headers |
| `mcp.md` | `fastapi-interface` §Exposing operations over MCP |

### Identity — the 16-section file, split

| Reference | Source |
|---|---|
| `authn.md` | `auth-tenancy-secrets` §Boundary binding, §Verify-then-resolve pipeline, §Authn dep keys, §AuthnDepsModule wiring, §Authn document specs |
| `fastapi-identity.md` | `auth-tenancy-secrets` §FastAPI identity (+ Cookie mode, Principal eligibility) |
| `oidc.md` | `auth-tenancy-secrets` §External IdPs |
| `authz.md` | `auth-tenancy-secrets` §Authz |
| `tenancy.md` | `auth-tenancy-secrets` §Tenancy and routed clients, §Isolation tiers and the declared floor, §Tenancy deps module |
| `tenancy-admin.md` | `auth-tenancy-secrets` §Tenant selector and admin plane, §Tenant provisioning |
| `secrets.md` | `auth-tenancy-secrets` §Secrets (+ Backends) |

### Encryption

| Reference | Source |
|---|---|
| `field-encryption.md` | `encryption-kms` §Wiring the keyring, §What gets encrypted, §Strict mode after backfill |
| `kms-backends.md` | `encryption-kms` §Choosing a key backend, §Per-tenant keys (BYOK), §Rotation vs replacement |

### Other planes — one job already, moved whole

| Reference | Source |
|---|---|
| `object-storage.md` | `object-storage` (whole) |
| `http-outbound.md` | `http-outbound` (whole) |
| `analytics.md` | `analytics` (whole) |
| `graph.md` | `graph-contracts` (whole) |
| `inference.md` | `inference` (whole) |

### Deterministic simulation testing

The only group with no source skill — see the note below.

| Reference | Source |
|---|---|
| `dst-simulation.md` | **New.** Declaring a `Scenario` over your own operations, `SimulationConfig`, the scheduler choice (`PCTScheduler` / `RandomScheduler` / `FIFOScheduler`), fault and latency environment, running the loop. Grounded in `pages/docs/dst/` → overview, environment, the-loop, exploration. |
| `dst-invariants.md` | **New.** Expressing invariants over your model state, `InvariantWitness`, reading a `ViolationReport`, crashes and clusters. Grounded in `pages/docs/dst/` → invariants, crashes-and-clusters. |

### Running in production

| Reference | Source |
|---|---|
| `errors.md` | `observability-errors` §Error model, §Adapter exception mapping, §FastAPI mapping |
| `logging-metrics.md` | `observability-errors` §Logging, §Operation and resilience metrics |
| `resilience.md` | `resilience-deadlines` §Resilience policies (all five `###`) |
| `deadlines.md` | `resilience-deadlines` §Invocation deadlines |
| `shutdown-fleet.md` | `resilience-deadlines` §Graceful shutdown & readiness, §Quiesce, §Fleet posture |

Each skill's `## Anti-patterns` entries follow their subject matter — an anti-pattern about `route=spec.name` lands in `deps-resolution.md`, not in a corpus-wide anti-pattern file. Each `## Reference` section's published-docs links likewise follow their subject; the versioned-`latest` note moves to the index once, rather than being repeated 22 times.

**The DST pair is net-new content and a deliberate scope addition.** Every other row moves text that already exists; these two are written from scratch, because `forze_dst` has no skill today despite being a key building block with its own nine-page docs section and a 31-name public surface. Two files rather than one follows the splitting rules directly — scenario setup plus schedulers plus invariants plus report-reading in one file would clear the 250-line ceiling — and the boundary is borrowed from the docs' own nav, which already separates *Using DST* from *Evidence*. The Evidence pages (detection statistics, fidelity, campaign results) stay in the published docs and are linked, not summarised: interpreting campaign statistics is not day-one application wiring. Budget this as writing, not moving; the effort estimate for the rest of §9 does not cover it.

**The split forces prose repair, and this is the real work.** A section written as part of a 2,000-word narrative frequently opens mid-thought ("This block continues the dependency-registry snippet above — it reuses its imports…", `forze-wiring` §Lifecycle plan). Every such seam must become self-contained or carry an explicit pointer. Budget for this; the file moves are the easy half.

## 7. Migration — the largest cost

**Hard cut. The 22 directories are deleted in the same change that adds `skills/forze-skills/`.** No stubs, no deprecation window.

This is a decision about the population, not about the mechanism. Per §4 the installer cannot prune a directory it is not overwriting, so *any* migration path leaves stale copies in consumer repositories that already installed — the only question is whether the repository carries transitional scaffolding to signal that. With the framework pre-adoption and its published skills installed only by its author, there is no population to signal to. Carrying 22 stub directories for two minor releases would be paying the full cost of a migration for zero recipients.

So:

1. `git rm -r` the 22 directories; add `skills/forze-skills/`.
2. **One `CHANGELOG.md` entry under `[Unreleased]`** recording the breaking rename, per operating rule 6. Terse — one bullet naming the merge, not a migration guide.
3. **Root [`README.md`](../README.md) Agent Skills section and [`skills/README.md`](../skills/README.md)** updated in the same change: the 22-row table becomes the single install line, and the `@forze-wiring` per-skill instruction is deleted (§1 shows it never worked).
4. **Any local install is cleaned by hand** — `rm -rf .claude/skills/forze-*` in whatever repositories have them. Not a documented procedure; a one-liner in the PR description.

Recorded so the reasoning is not mistaken for a general policy: **this shortcut is available exactly once.** It is licensed by there being no external installed base, and that condition expires the moment the framework has one. A future breaking change to the published skill surface does not inherit this precedent and should expect §7's original stub-and-window shape.

`AUTHORING.md`'s "Retired (merged) skill names" section still gets the 22 names appended — that list exists to stop a retired name being re-created, which is a maintainer-facing concern independent of who has installed what.

## 8. Risks

- **Routing regression (§5).** The one that is not fully mitigable. Bundles reduce it; they do not eliminate model judgment. Accepted knowingly.
- **The index becomes a maintenance surface.** 48 one-line summaries that must stay discriminating and in sync. RFC 0040 §3.3's parity check prevents *structural* drift (orphans, dead links); it cannot prevent a summary going vague. rust-skills solves this by generating the index from each rule's `> summary` line — worth adopting **only if** the reference count grows past ~60. At 48, generation is over-engineering and a hand-written index that CI checks for parity is the right call. Recorded as the trigger, not built.
- **One large PR.** The split touches all 22 files at once. Mitigation: land RFC 0040 first so the import and structure gates are green *before* the move, making the move's correctness mechanically checkable rather than review-checkable.
- **Loss of per-skill install.** Real but small: §1 shows it was already broken, and nobody wants `forze-wiring` without `forze-specs-infrastructure`.

## 9. Execution

1. RFC 0040 lands; gates green on the current layout.
2. Create `skills/forze-skills/` and move content per §6, one commit per group so review is tractable.
3. Repair the seams (§6, final paragraph). This is the bulk of the effort.
4. Write `SKILL.md`: mental model, routing table with bundles, the read-more-than-one norm, the versioned-docs note.
5. Extend RFC 0040's structure gate to index↔reference parity; run the full suite; confirm 236/236 imports still resolve after the move.
6. `git rm -r` the 22 directories (§7). Rewrite `AUTHORING.md` for the new shape and append the 22 retired names to its retired-names list; update both READMEs and `CHANGELOG.md`.

## 10. Success criteria

- Every one of the 236 import pairs still resolves after the move — the split moved text, not meaning.
- No reference file exceeds 250 lines; none is under 60 without a recorded reason.
- Index↔reference parity is machine-checked, so an orphaned or unlisted reference cannot merge.
- **Behavioral, and the one that actually matters:** for each of the five bundles in §5, a cold agent given only the index reaches the complete bundle. If it reaches one file and stops, the routing table has failed and no amount of correct file-splitting compensates.
