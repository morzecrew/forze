# RFC 0040 — Skills corpus integrity gates

- **Status:** 📝 Draft — design locked, ready to execute. **Independent of RFC 0041 and 0042 and deliberately sequenced first**: every gate here is written against the *current* 21-directory layout, so it lands, catches rot, and keeps working whether or not the consolidation ever happens. Nothing in this RFC assumes a restructure.
- **Scope:** Machine-checkable integrity for the published skills corpus under [`skills/`](../skills/): that its Python examples parse, that every `forze*` symbol they import still exists, that its internal links resolve, that its structure is uniform, and that its published-docs links are alive. Adds a checker plus a `justfile` recipe plus CI wiring. Does **not** change any skill's prose, does not restructure the corpus (RFC 0041), and does not decide what the corpus should cover (RFC 0042).
- **Related:** [`skills/AUTHORING.md`](../skills/AUTHORING.md) — the maintainer rules this RFC mechanizes; its step 4 (*"Grep: `rg '../../(pages|src|tests)' skills/` should return nothing"*) is already a hand-run gate that nothing enforces. [`justfile`](../justfile) — where `just quality` composes the existing lint/import/dead-code/security gates. [`pyproject.toml`](../pyproject.toml) — `[tool.hatch.build.targets.wheel]`, the authoritative shipped-package set RFC 0042 ratchets against. RFC 0041 consumes this RFC's structure gate to enforce index↔reference parity after consolidation.
- **Origin:** An assessment of whether to restructure `skills/` along the lines of [`leonardomso/rust-skills`](https://github.com/leonardomso/rust-skills). Measuring the corpus to answer that question turned up something the restructure question had obscured: **the content is currently correct, and nothing whatsoever keeps it that way.** 236 of 236 imported symbols resolve; 94 of 94 published-doc links return 200; and `rg -l 'skills' .github/ justfile` returns nothing. The corpus is 20,329 words of executable claims about a fast-moving API, gated by code review alone. That is the "built the mechanism, not the gate" failure the 7th-edition framework audit named as the current theme, and it is worth fixing on its own merits regardless of what shape the corpus ends up in.

---

## 1. The problem

`skills/` is documentation that ships to other people's repositories and is read by agents that will write code from it. That gives it two properties ordinary docs do not have.

**It is executable in effect.** An agent reading `from forze_kits.aggregates.document import DocumentFacade` will emit that line. When an export is renamed, the skill does not degrade gracefully into vague-but-harmless prose — it actively instructs a downstream agent to write a broken import, in a repository the framework team cannot see and will never get a bug report from.

**It is invisible to every gate the repository already has.** `just quality` runs lint, import-linter contracts, dead-code and dependency/security checks over `src/` and `tests/`. None of them read Markdown. `pyproject.toml`'s import-linter contracts constrain what `src/` may import; they say nothing about what `skills/` *claims* `src/` exports. The docs build (`just build-docs`) covers `pages/`, not `skills/`. So the entire corpus sits outside CI.

The measured state, taken on the current `main`:

| Property | Measured | Gate today |
|---|---|---|
| Published skills | 21 `SKILL.md` files | — |
| Body content | 20,329 words (≈30k tokens) | none |
| Python code blocks | 126, of which **1** does not `ast.parse` | none |
| `from forze* import X` pairs | 236 distinct, **236 resolve** | none |
| Published doc URLs | 94 unique, **94 return 200** | none |
| Cross-skill relative links | 55 | none |
| Mentions in `.github/` or `justfile` | **0** | — |

Two readings of that table are available and only one is right. The wrong one is *"nothing is broken, so no gate is needed."* The right one is *"nothing is broken **yet**, and the corpus has been maintained by hand across 21 files and 29 shipped packages with no mechanical backstop — the next rename is a coin flip."* A gate written while the corpus is green is also cheap to land: it goes in without a backlog of pre-existing failures to triage, which is exactly the moment such gates are easiest to add and hardest to justify. This RFC is the argument for doing it anyway.

## 2. What is deliberately **not** copied from rust-skills

rust-skills validates its corpus with `checks/`: `validate.py` (structure and link parity), `gen_index.py` (regenerates the index from the rule files), and a compile harness — `gen.py` extracts every ` ```rust ` block into `examples/<name>.rs`, `cargo check` compiles them, `analyze.py` buckets the failures into fragment / artifact / low / SUSPECT, and `baseline.txt` records the currently-accepted suspects so CI fails only on *new* ones, all pinned to a specific toolchain that `baseline.txt` was generated against.

That architecture is a rational response to a Rust-specific problem: a doc snippet is not a compilation unit, fragments cannot be type-checked standalone, and wrapping them heuristically produces false failures that must be classified and tolerated. Python has none of those constraints. `ast.parse` answers "is this syntactically a Python module?" directly, and `importlib` answers "does this symbol exist?" directly, against the very packages the repository builds. The equivalent gate is roughly thirty lines and needs no extraction step, no generated example tree, no pinned toolchain, and no build.

**`baseline.txt` in particular is an anti-goal.** A file listing failures the gate has agreed not to report is the silent-cap pattern: it converts "this check passes" into "this check passes except for the parts we stopped looking at", and the exception list only ever grows. Where this RFC's gates cannot check something, they will say so out loud in their output rather than record it in a tolerated-failures file. The one place a suppression is genuinely needed — code blocks that are deliberately fragments — is handled by an explicit in-document marker (§3.2) that a human writes on purpose, not by a generated ledger.

The two rust-skills ideas worth taking wholesale are its **structure validator** and its **index-parity check**. Both are adopted in §3.3.

## 3. The gates

One checker, `tools/skills_check/`, outside `skills/` so it is never copied into a consumer's repository by the installer (RFC 0041 §4 establishes why anything under the skill directory ships). Five independent checks, each reporting every problem it finds rather than stopping at the first.

### 3.1 Import resolution — the load-bearing one

For every ` ```python ` block in the corpus: `ast.parse`, walk the tree, and check each import node against the contract below. The two node types are **not** interchangeable and the gate must treat them separately:

| Node | Example | What is checked |
|---|---|---|
| `ast.Import` | `import forze_postgres` | the **module** resolves. There is no symbol to check. |
| `ast.ImportFrom` | `from forze_postgres import PostgresClient` | the module resolves **and** each name is either an attribute of it or itself an importable submodule (`from forze_kits import aggregates`). |

Three details the one-line version got wrong, each of which would let a broken example through or fail a correct one:

- **Aliases bind the local name, never the source.** For `import forze_postgres as pg` and `from forze_kms import aws as kms_backend`, the gate checks `forze_postgres` and `forze_kms.aws` — `a.name`, not `a.asname`. Checking the alias would look up a symbol that by definition does not exist upstream.
- **Star imports cannot be verified and are rejected outright.** `from forze_x import *` gives the gate nothing to check, so tolerating it would create a silent hole exactly where the corpus is least specific. Fail the block and require explicit names.
- **Match on a module boundary, not a string prefix.** The test is `root == "forze" or root.startswith("forze_")`. Bare `startswith("forze")` would also swallow a third-party `forzex`, and — more likely — would silently skip nothing today while misbehaving the first time such a name appears.

Current corpus shape, so the contract is calibrated against reality rather than imagination: **178 `ImportFrom`, 6 `Import`, 0 star imports, 0 aliases.** The alias and star rules are therefore specification-level precision, not live defects — they are written down because the gate outlives the corpus it was measured on, and an under-specified checker acquires these holes the first time someone writes an ordinary `import ... as ...`.

This is the gate that pays for the whole RFC. It is the only mechanical link between the prose and the API it describes, and it fails precisely when a rename or a re-export removal happens — the change class most likely to slip through review, because the reviewer is looking at `src/` and the stale claim is in Markdown.

It also composes with a hazard the repository has already lived through. The re-export cleanup that removed dead `execution.*` aliases — on the correct principle that `contracts` is the home — is, from the corpus's point of view, indistinguishable from a silent break: a tidy-up in `src/` invalidates an example nobody re-read. This gate makes that class of change loud at the moment it happens.

Run against the **full extras install**, which needs no new provisioning: [`justfile`](../justfile) defines `_uv_sync := "uv sync --all-groups --all-extras"` and [`ci.yml`](../.github/workflows/ci.yml) runs the same at lines 107 and 187. Every shipped package is therefore importable wherever this gate runs. A per-extra matrix would report the wrong thing anyway — a `forze_temporal` import failing in an environment without the `temporal` extra is a harness artifact, not a corpus defect.

The checker still **prints any module it could not import** rather than passing over it, so an environment that silently loses an extra shows up as a visible skip instead of a shrinking denominator. Line 258 of `ci.yml` runs `uv sync --all-groups` *without* `--all-extras`; this gate must not be added to that job.

### 3.2 Syntax

`ast.parse` every ` ```python ` block. 126 blocks today; exactly one fails, and it is an intentional fragment.

The gate is therefore useless in its natural form — "1 known failure" is indistinguishable from "1 new failure". Fix the corpus, not the gate: intentional fragments get an explicit fence marker (` ```python fragment `), the checker parses unmarked blocks strictly, and it **errors on a marked block that actually parses fine**, so the marker cannot be sprinkled defensively. Zero tolerated failures, by construction.

### 3.3 Structure and links

Adapted from rust-skills' `validate.py`:

- every `SKILL.md` has YAML frontmatter with `name` and `description`, and `name` matches its directory (post-0041: matches the frontmatter contract that RFC establishes);
- required sections are present — today `## Anti-patterns` and `## Reference`, which [`AUTHORING.md`](../skills/AUTHORING.md) §"Skill structure" already mandates in prose;
- every relative Markdown link resolves to a file that exists;
- **no link escapes the published tree** — the `../../src/`, `../../tests/`, `../../pages/` prohibition in `AUTHORING.md` step 4, which today is a grep a maintainer is asked to remember. Extend it to `../../examples/`: the rule's rationale (installed skills are copied outside this repository, so those paths break) applies identically, and the list simply predates anyone linking there. [`forze-wiring`](../skills/forze-wiring/SKILL.md) already cites `examples/recipes/aggregate_kit/` — as prose qualified with *"in the Forze repo"*, so it is honest and not a defect, but it is one edit away from becoming a dead link;
- the `Reference` section carries the versioned-docs note `AUTHORING.md` requires, and every `morzecrew.github.io` link includes the `latest` segment (the bare form 404s — a documented trap that nothing checks);
- **index parity** — every skill listed in [`skills/README.md`](../skills/README.md)'s table exists, and every skill exists in the table. Post-0041 this becomes index↔reference parity, the check RFC 0041 depends on.

### 3.4 Published-link liveness

The 94 unique `morzecrew.github.io/forze/latest/...` URLs, checked for HTTP 200.

**Not a per-PR gate.** This was tested during the assessment and the naive form is actively harmful: sweeping all 94 without rate limiting produced a uniform wall of 502s, including the site root — a false-positive rate of 100% that would train everyone to ignore the check within a week. Rate-limited (~0.4s between requests) all 94 return 200.

**Rate limiting is pacing, not a bound.** The 0.4s delay says nothing about how long any single request may take, and a request with no timeout can hang on a stalled socket until the GitHub Actions job timeout kills it — six hours of a runner to learn nothing. Each request therefore carries an explicit connect **and** read timeout, and a bounded retry budget with backoff; a URL fails only once that budget is exhausted. Retries matter here more than in most gates, because §3.4's whole rationale is that this check's transient-failure rate is high enough to destroy trust — retrying is what separates "the page is gone" from "the CDN hiccuped".

It therefore runs on a **schedule, not on PRs**, and simply fails the scheduled job. Docs links break for reasons outside a given PR's control — a page renamed in `pages/`, a mike alias moved — so the failure belongs on a queue, not in a merge gate; but a red scheduled run is already a queue, and auto-filing an issue would add a second thing to close for every transient network failure. Revisit if the corpus ever has more than one maintainer, where a red job nobody owns goes unnoticed and an issue does not.

### 3.5 Package census

For every package in `[tool.hatch.build.targets.wheel]`, report whether it appears in an actual *import* in the corpus, only in prose, or not at all.

Key it on **wheel packages, not extras**. The two are not interchangeable and the difference is not cosmetic: `[project.optional-dependencies]` has 33 keys against 29 packages, because `authn`/`oidc` are `forze_identity` submodules, the three `kms-*` extras are all `forze_kms`, the two `inference-*` extras are all `forze_inference`, and `observability`/`zstd` ship no package at all. Extras describe what gets *installed*; packages describe what gets *imported*, and imports are what the corpus makes claims about. RFC 0042 §1 records the incorrect extras-keyed number this produced on the first attempt.

**Report-only in this RFC.** RFC 0042 owns what the number must be and when it may not regress. Making it report-only is deliberate: a coverage gate landing with ten pre-existing gaps is a gate that acquires a baseline file, and §2 rules those out.

## 4. Wiring

- `tools/skills_check/` — the checker. Standard library only; no new dependency for a Markdown linter.
- `just skills-check` — the local entrypoint, composed into `just quality` alongside the existing gates.
- CI: §3.1–§3.3 and §3.5 run in the existing test/quality job (fast, hermetic, no network). §3.4 runs in a scheduled workflow.

Consistent with operating rule 1 in [`AGENTS.md`](../AGENTS.md) — prefer editing existing files — this adds one recipe to the existing `justfile` and one job to existing workflows rather than a parallel quality system.

## 5. What this does not catch, said out loud

Stating the residue is the alternative to a baseline file.

- **Semantic staleness.** An import can resolve while the surrounding prose describes behavior that changed. `ctx.document.query` existing says nothing about whether the described default is still the default. Only review catches this.
- **Argument-level drift.** The gate checks that a symbol exists, not that a call site passes the right keywords. Signature-level checking is possible (`inspect.signature` against the parsed call) and is *deliberately deferred*: the false-positive rate on partial examples and `...` placeholders is high enough that it would need exactly the suppression ledger §2 rejects. Revisit only if a real defect of this class is observed.
- **Prose accuracy about non-Forze systems** — Postgres, Temporal and Inngest behavior claims are outside any gate here.
- **Whether the corpus covers the right things at all** — RFC 0042.

## 6. Execution

One PR, roughly a day.

1. **Mark the one intentional fragment first** (§3.2). This has to precede the gate's first run: the corpus contains a block that does not `ast.parse` today, so a syntax gate built before the marker exists cannot produce the green baseline step 2 is supposed to confirm — it would open on a known failure, which is how a tolerated-failure list gets born.
2. `tools/skills_check/` with §3.1, §3.2 (zero-tolerance), §3.3, §3.5. Confirm the expected green baseline (236/236 imports, 94/94 links, index parity) so the gate's first run is meaningful rather than a triage session.
3. `just skills-check`, composed into `just quality`.
4. CI: fast checks into the existing job; §3.4 as a scheduled workflow that simply fails on a dead link.
5. Replace `AUTHORING.md` step 4's hand-run grep with a pointer to the recipe — the policy stops being a thing maintainers are asked to remember.

## 7. Success criterion

Deliberately behavioral: a gate is not proven by reading it. **Inject the regression each check exists to catch and watch it fail** — rename an export used by a skill, break a relative link, un-mark a fragment, add a `../../src/` link — then revert. A gate that has never been seen red is a gate nobody has tested.
