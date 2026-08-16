# Execution log

Where building something disagreed with the design for it, written down at the moment it
happened. Nothing here is revised afterwards to agree with what was later settled, and
nothing here has been folded back into an RFC's own text.

Entries are appended, never revised. The decision rows in them are **proposals**: execution
never writes one into an RFC on its own authority. Once the author accepts a row it is
appended to that RFC's decision table citing the entry it came from, and the outcome — every
acceptance and every refusal — is recorded here in a dated table, because a refusal is
written down nowhere else. The RFC's prose still stands as designed; a row that overrides a
section says so, and the section is left alone.

## Classes

| Class | Test | Meaning |
|---|---|---|
| `discovery` | Could not have been known before code existed | Healthy — the RFC was right to be silent |
| `spec-gap` | Could have been known; the RFC was silent or at the wrong altitude | The design process missed something |
| `drift` | The RFC covered it and it was built otherwise anyway | **A defect** |
| `irreducible` | No amount of design settles it | Stop and spike |

---

# Unit 1 · Skills corpus integrity gates

Branch `feature/skills-corpus-integrity-gates`. RFC 0040 in full — §3.1 imports, §3.2
syntax, §3.3 structure and links, §3.4 published-link liveness, §3.5 package census, plus
the §4 wiring and the §6 execution order.

**Drift count: 0.** Seven entries, none of them `drift`: two `discovery`, five `spec-gap`.

RFC 0040 carries no graded decision table — it is prose-locked ("design locked, ready to
execute"). Every §2/§3 commitment was therefore read as `LOCKED` and every silence as an
unlisted gap, which is why several entries below have no row to cite.

## D-001 — The corpus has no unparseable block; the "one intentional fragment" is an extractor artifact

- **Touches:** RFC 0040 §3.2 and §6 step 1. Nothing in a decision table covers this — the
  RFC has none.
- **RFC said:** "126 blocks today; exactly one fails, and it is an intentional fragment",
  and §6 step 1 makes marking that fragment the *first* thing execution must do, before
  the gate can produce a green baseline.
- **Found:** 127 python blocks, **all of which parse**. The block that fails
  ([`forze-fastapi-interface/SKILL.md:92`](../skills/forze-fastapi-interface/SKILL.md))
  is a fence nested inside a list item. Its body carries the list's indentation, and
  `ast.parse` on the raw body raises `IndentationError` for that reason alone. CommonMark
  defines the opening fence's indentation as the amount stripped from each body line;
  dedenting accordingly, the block is ordinary, valid Python.
- **Because:** the measurement was taken with an extractor that did not dedent. The
  failure was the extractor's, and it reads exactly like a failure of the corpus — which
  is the shape a tolerated-failure list grows from, and the shape §2 exists to refuse.
- **Class:** `discovery`. Not `spec-gap`: no amount of reading the corpus reveals this,
  because the artifact only exists once something extracts blocks. Writing the correct
  extractor is what surfaced it.
- **Consequence:** §6 step 1 has nothing to do and is skipped. The fragment mechanism is
  built exactly as §3.2 specifies — including the rule that a marked block which parses
  is itself a failure — but **no block in the corpus is marked**, which is the only state
  consistent with that rule. The mechanism is now specification for a case that does not
  yet occur, which is what §3.2 said it was for ("the gate outlives the corpus it was
  measured on").
- **Proposed row (RFC 0040):** `ASSUMED` — fenced blocks are dedented by the opening
  fence's own indentation before parsing. An extractor that does not dedent reports every
  fence nested in a list item as a syntax failure, and that false positive is
  indistinguishable from the real thing it would hide.

## D-002 — Corrected baseline numbers: 127 blocks, 237 distinct imports, 64 published URLs

- **Touches:** RFC 0040 §1's measured-state table and §6 step 2, which asks execution to
  "confirm the expected green baseline (236/236 imports, 94/94 links, index parity)".
- **RFC said:** 126 python blocks, 236 distinct `forze*` import pairs, 94 unique published
  doc URLs, 55 cross-skill relative links.
- **Found:** **127** python blocks; **237** distinct import pairs across **302** import
  assertions; **64** unique published URLs (109 total occurrences); **203** links checked,
  of which 90 are relative.
  - The 237th import pair is precisely the one inside the block D-001 could not parse.
    Fixing the extractor did not change the corpus; it revealed one more claim in it.
  - 94 → 64 is a distinct-vs-occurrence difference; nothing in the corpus changed.
- **Because:** each number came from a different counting rule than the checker uses. The
  checker's rules are now written down in code and re-derived on every run, so this is the
  last time the two can drift apart silently.
- **Class:** `discovery`.
- **Consequence:** the green baseline §6 step 2 asks to confirm is
  `127/127 parsed · 302/302 resolved (237 distinct) · 0 skipped · 64/64 URLs live · index
  parity`, and that is what the gate now prints on every run. A reader comparing the RFC's
  table to the gate's output will see different numbers; this entry is why.
- **Proposed row (RFC 0040):** `ASSUMED` — §1's measured-state table is superseded by the
  checker's own output. Counts in prose go stale; the gate prints its denominators.

## D-003 — `forze-specs-infrastructure` had no `## Anti-patterns` section

- **Touches:** RFC 0040 §3.3 (required sections), §2 (no tolerated-failure file), and
  §Scope ("does not change any skill's prose").
- **RFC said:** all three, and they cannot all hold: the structure gate requires
  `## Anti-patterns`, one skill does not have it, a baseline is forbidden, and prose is
  out of scope.
- **Built:** a four-bullet `## Anti-patterns` section in
  [`forze-specs-infrastructure`](../skills/forze-specs-infrastructure/SKILL.md), scoped to
  mistakes an app team can make, per `AUTHORING.md`'s anti-patterns policy.
- **Because:** §2 removes the option of tolerating it and §3.3 removes the option of not
  checking it, so the corpus is what gives. §6 step 1 already establishes prose-edit-
  before-gate as this RFC's own answer to exactly this shape — the RFC anticipated the
  case, just not this instance of it.
- **Class:** `spec-gap`. Knowable before any code existed: §1's measured-state table
  counts blocks, imports, URLs and links, but never checks the property §3.3 would go on
  to require. The one structural claim the gate makes was the one thing not measured.
- **Consequence:** §Scope's "does not change any skill's prose" is now false as written.
  The gate opens green with zero tolerated failures, which was the point of landing it
  while the corpus is otherwise clean.
- **Proposed row (RFC 0040):** `ASSUMED` — the corpus is fixed to meet a gate, never the
  gate softened to meet the corpus. Where a check finds a pre-existing failure, the
  failure is repaired before the gate lands; the alternative is a baseline file by another
  name.

## D-004 — A skipped import is a failure by default, not a silent pass

- **Touches:** RFC 0040 §3.1, final paragraph.
- **RFC said:** the checker "still **prints any module it could not import** rather than
  passing over it, so an environment that silently loses an extra shows up as a visible
  skip instead of a shrinking denominator."
- **Built:** skips are printed *and* fail the run, with `--allow-skips` to accept a
  partial local install. Separately, an unimportable module is classified rather than
  uniformly skipped: a root that is **not in the wheel's package list** is a corpus defect
  and always fails, and a **missing `forze*` submodule of a root that imports fine** is a
  corpus defect too — only a third-party import failure inside a shipped package counts as
  environmental.
- **Because:** "visible" is a property of a green run's output, and nobody reads the output
  of a green run. The RFC's own words name the risk — a shrinking denominator — and
  printing does not stop it shrinking. The classification matters more: without it, a
  deleted `forze_postgres.kernel` would be indistinguishable from a missing `postgres`
  extra, and the gate's single load-bearing check would have a hole exactly where a rename
  lands. In the environment §3.1 specifies (`uv sync --all-groups --all-extras`, which
  both `just skills-check` and the CI quality job run) the skip count is zero, so the
  default never fires in normal use.
- **Class:** `spec-gap`. Knowable at design time — the difference between "reported" and
  "enforced" is the theme the 7th-edition framework audit named, and §3.1 lands on the
  wrong side of it in one sentence while the rest of the RFC is on the right side.
- **Consequence:** running the gate in an environment without the full extras set now
  fails rather than passing over a subset. That is louder than the RFC describes and is
  the intended trade.
- **Proposed row (RFC 0040):** `ASSUMED` — an unchecked import is a failed import. Skips
  are reported and fail by default; `--allow-skips` exists for a partial local install and
  is never used in CI. A module whose root is absent from the wheel, or a missing `forze*`
  submodule of a root that imports, is a corpus defect and is never classed as a skip.

## D-005 — The gate is added to the CI path filter, or it is skipped by exactly the changes that break it

- **Touches:** RFC 0040 §4 ("CI: §3.1–§3.3 and §3.5 run in the existing test/quality
  job"). Unlisted — the RFC does not mention the filter.
- **RFC said:** compose into `just quality`, which the `quality` job runs.
- **Built:** `skills/**` and `tools/**` added to the `changes.code` paths filter in
  [`ci.yml`](../.github/workflows/ci.yml).
- **Because:** the `quality` job is conditional on `needs.changes.outputs.code == 'true'`,
  and that filter lists `src/**`, `tests/**`, `pyproject.toml` and friends. A skills-only
  pull request — the single change class most likely to break the corpus — would not have
  run the gate at all. The filter already carries a comment describing this exact failure
  mode for the gitmoji-excerpt guard; this is the same defect one file over.
- **Class:** `spec-gap`. Fully knowable from `ci.yml`, and already known: the precedent is
  written down in the file being edited.
- **Consequence:** a skills-only or checker-only change now runs the full quality job.
- **Proposed row (RFC 0040):** `ASSUMED` — a gate composed into `just quality` is
  incomplete until its inputs are in the `changes.code` filter. A conditional job is only
  as good as the paths that trigger it.

## D-006 — §3.4 gets its own scheduled workflow rather than a job in the nightly one

- **Touches:** RFC 0040 §4, which says this addition "adds one recipe to the existing
  `justfile` and one job to existing workflows rather than a parallel quality system",
  consistent with `AGENTS.md` operating rule 1.
- **RFC said:** one job, in an existing workflow.
- **Built:** a new [`skills-links.yml`](../.github/workflows/skills-links.yml), scheduled
  daily at 05:00 UTC.
- **Because:** the only existing scheduled workflow is `nightly.yml`, whose name, header
  and concurrency group are the DST matrix, and whose red/green means "the simulator found
  an interleaving". Folding a network sweep into it makes that signal ambiguous: a red
  nightly would no longer tell a reader which of two unrelated things failed without
  opening it. The rule §4 invokes is about not building a parallel quality *system*; one
  scheduled job with a different failure meaning is not that.
- **Class:** `spec-gap`. Knowable from `.github/workflows/` at design time.
- **Consequence:** one more workflow file. The sweep runs no `uv sync` and creates no
  virtualenv — this mode is standard-library only, following the precedent set by
  `nightly.yml`'s verdict job — so `just skills-links` and the scheduled run execute the
  same line, and the check cannot fail for want of an extra.
- **Proposed row (RFC 0040):** `ASSUMED` — §3.4 runs in its own scheduled workflow. A
  scheduled job's colour is its whole interface, so two failures that mean different
  things do not share one.

## D-007 — The local pre-commit gate never ran on a skills-only commit

- **Touches:** RFC 0040 §4. Unlisted — the RFC wires `just quality` and CI, and says
  nothing about the pre-commit hook that also runs `just quality`.
- **RFC said:** compose `just skills-check` into `just quality`.
- **Found:** [`.pre-commit-config.yaml`](../.pre-commit-config.yaml) opens with
  `exclude: ^(\.agents/)?skills/`, which applies to every hook including the local
  `quality` one. Because that hook was selected by staged paths like any other, a commit
  touching only `skills/` matched no files and ran **no hooks at all** — the corpus gate
  included.
- **Built:** `always_run: true` on the `quality` hook.
- **Because:** the exclude is right for the file-scanning hooks it was written for
  (gitleaks, end-of-file-fixer) and wrong for a whole-repository gate, which has no
  business being selected by staged paths at all. This is D-005 one layer down: the same
  blind spot, in the local gate rather than the CI one, and reached by the same change
  class.
- **Class:** `spec-gap`. Knowable from `.pre-commit-config.yaml` before any code existed.
  It surfaced during execution only because the corpus fix and the gate landed in
  different commits, and the commit carrying the fix visibly ran nothing.
- **Consequence:** `just quality` now runs on every commit rather than on commits that
  happen to touch a matched path. Slightly more work per commit; the alternative is a
  gate that skips its own subject.
- **Proposed row (RFC 0040):** `ASSUMED` — a whole-repository gate is `always_run`. Path
  selection is for hooks that read the paths; a gate that reads the repository must not
  inherit a file-scoped exclude.

**Deliberately not applied:** §3.4 specifies "an explicit connect **and** read timeout".
The standard library expresses both as one socket timeout, which bounds establishing the
connection and every subsequent read. Both are bounded; they share one number. No third-
party HTTP client was added for the sake of splitting them, since §4 requires the checker
be standard library only.

**Also deliberately not applied:** no `CHANGELOG.md` entry. `CONTRIBUTING.md` → Changelog
excludes CI updates and test-only changes, and this unit is a gate plus its tests plus a
maintainer-facing `AUTHORING.md` edit. The one shipped-artifact change (D-003's
anti-patterns section) is a small docs addition inside a skill, not a public API or
migration fact.

## Decision-row outcomes — 2026-08-15

All seven proposals accepted, one at a higher grade than proposed, plus one refusal that
exists only here. RFC 0040 had no decision table before this; §8 is the table these rows
opened, and every row in it carries the entry it came from.

| RFC | Row | Outcome | Grade | Decision | From |
|---|---|---|---|---|---|
| 0040 | 1 | Accepted | `ASSUMED` | Fenced blocks are dedented by the opening fence's own indentation before parsing | D-001 |
| 0040 | 2 | Accepted | `ASSUMED` | §1's measured-state table is a snapshot superseded by the checker's output, not corrected in place | D-002 |
| 0040 | 3 | Accepted, **grade raised** | `LOCKED` (proposed `ASSUMED`) | The corpus is fixed to meet a gate, never the gate softened to meet the corpus | D-003 |
| 0040 | 4 | Accepted | `ASSUMED` | An unchecked import is a failed import; skips fail by default, `--allow-skips` never used in CI | D-004 |
| 0040 | 5 | Accepted | `ASSUMED` | A gate in `just quality` is incomplete until its inputs are in the `changes.code` filter | D-005 |
| 0040 | 6 | Accepted | `ASSUMED` | §3.4 runs in its own scheduled workflow, not as a job in `nightly.yml` | D-006 |
| 0040 | 7 | Accepted | `ASSUMED` | A whole-repository gate is `always_run` in pre-commit | D-007 |
| 0040 | — | **Refused** | — | Correcting §1's counts to 127 / 237 / 64 in place. A reader cannot distinguish a corrected measurement from a laundered one, and the distinction is the only thing this log protects — the numbers live in D-002 instead | D-002 |

Two notes on the grades, because the reasoning is the part that does not survive in a
table:

- **Row 3 is the only `LOCKED` one, and deliberately.** It is §2's no-`baseline.txt`
  argument in operational form, and the moment it binds is when an executor under time
  pressure finds a pre-existing failure and relaxing one predicate looks obviously
  correct. That is the case that wants a second reader, which is what the grade buys.
- **Rows 4 and 6 are departures from the RFC's letter**, accepted as such rather than
  quietly absorbed. §3.1's final paragraph stops at reporting a skip; §4 asks for a job in
  an existing workflow. Both sections stand as written and both rows override them.

## Audit findings — 2026-08-15

Adversarial pass over the whole branch: 5 commits, 15 files, +1985/-6, against merge base
`381eb39d5`. Ran the suite, measured patch coverage over `tools/` specifically, and swept
the real corpus with hand-injected mutations. Seven findings, all fixed on the branch —
the last of them found by re-auditing one of the fixes.

| # | Severity | Finding | Status |
|---|---|---|---|
| A-1 | **High** | The gate passes green on a corpus it never read. Two ways in: a block fenced ` ```py ` (a normal spelling) matched no language and was invisible to both the syntax and import checks, so a corpus whose examples all used it reported `0/0 resolved · ok`; and a zero denominator was never itself refused, so any future extractor break — a renamed fence convention, a glob that stops matching — turns the gate silently green rather than red. Reproduced: a `py`-fenced `from forze_nonexistent import Broken` exited 0 | Fixed — `PYTHON_LANGS = {python, py, python3}`, and both `check_syntax` and `check_structure` refuse an empty denominator at the seam that knows it |
| A-2 | Medium | §3.3 requires the `latest` segment on every published-docs link; the check read only parsed Markdown links, so a bare URL in prose — the easier spelling to write by accident — was unguarded and would 404 for every reader | Fixed — `_check_version_segment` scans the raw text, matching what `collect_published_urls` already did for the liveness sweep |
| A-3 | Medium | `__main__.py` was 0% covered. Both its detection branches — the `--allow-skips` policy from D-004 and the empty-URL refusal in the liveness sweep — are code that only runs when something is wrong, which is exactly the code that must not be dead | Fixed — CLI tests, including a stub package that is installed and genuinely will not import, which drives the skip policy deterministically instead of skipping when the local environment happens to be complete |
| A-4 | Low | `Document.section()` matched a heading by text at any depth, so `#### Anti-patterns` satisfied a check whose message says `## Anti-patterns`. The checker's own error message was untrue of what it required | Fixed — the level is matched, not just the text |
| A-5 | Low | An unclosed fence silently absorbed the rest of the file, taking its headings and links out of every other check with it — a structural failure presenting as a missing `## Reference` section somewhere unrelated | Fixed — `CodeBlock.closed`, reported directly |
| A-7 | Low | **Found by re-auditing the A-3 fix.** The CLI tests it added resolved `skills/` and `pyproject.toml` against the working directory, so `pytest` invoked from anywhere but the repository root failed three tests. Every sibling guard test in `tests/unit/` anchors on `Path(__file__).resolve().parents[2]`; these did not | Fixed — anchored the same way. Reproduced by running the suite from `tests/` (3 failed), then green from both directories |
| A-6 | Low | The scheduled sweep had no `timeout-minutes`. Per-request timeouts and pacing bound the rate and each attempt, but the worst case (every URL exhausting its retry budget) is ~50 minutes, and nothing bounded the job below the runner's six-hour default | Fixed — `timeout-minutes: 20` |

**Sabotage sweep: 8 mutations, 8 killed.** Against the real corpus, reverted after each:
a removed `src/` re-export (`VaultClient`), a broken cross-skill link, a link escaping the
published tree, a `fragment` marker on a healthy block, a skill deleted from the index
table, plus three against the fixes above — a `py`-fenced broken import, a `latest`
segment dropped from a prose URL, and a required heading demoted to `####`. Each was
reported with the file, line and specific cause; the corpus returned green after every
revert. This is RFC §7's behavioral criterion, discharged against the real corpus rather
than only the synthetic one the unit tests use.

**Patch coverage over `tools/`: 79.9% → 94.2%** (52 tests). Residue: `links._fetch` (the
real network call — exercised by `just skills-links`, not by the suite) and the
environment-skip branches that need a partly-installed environment to reach.

**What remains distrusted.**

- **Nothing lints `tools/`.** `ruff`, `ruff format` and `mypy` were run over it by hand
  and are clean, but `just quality` scopes all three to `src/`, so the next edit has no
  gate. Same standing gap as `.github/scripts/`; see *Carried into the next unit*.
- **The scheduled workflow has never run.** Its logic was exercised locally
  (`just skills-links`: 64/64 live) and `zizmor` passes, but cron firing, the step summary
  and the job timeout are unverified until the first scheduled run.
- **The `--allow-skips` path is proven against a stub, not a real missing extra.** The
  stub reproduces the failure shape (a shipped package whose third-party import is
  absent), which is the mechanism that matters, but no run in a genuinely partial
  environment has happened.
- **Semantic staleness is out of scope by design** (RFC §5): an import can resolve while
  the prose beside it describes behavior that changed. Nothing here narrows that.

## Review findings — 2026-08-15

PR #376. Five findings from CodeRabbit, CodeAnt, Greptile and CodeFactor, all fixed; two
of them are defects the self-audit above missed, which is why they are recorded beside it
rather than only in the pull request.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | **High** | `_symbol_exists` resolved a submodule with `find_spec`, which locates a file without executing it. A submodule that exists and raises during initialization therefore has a spec and no binding, so the gate reported `1/1 resolved · ok` for an example that raises for any reader who runs it. Reproduced with a stub package whose `broken.py` raises at import time (CodeAnt) | Fixed — the check now runs the same import the corpus line does, classified through the shared `_import_status` so importing for real cannot reclassify a missing extra as a corpus defect |
| R-2 | **High** | The scheduled sweep's `timeout-minutes: 20` is below its own worst case. 64 URLs x (3 attempts x 15 s + 6 s backoff) plus pacing is ~55 minutes, and `_run_liveness` prints only after finishing — so a total outage burned an hour and reported nothing. **The audit above computed the same ~50 minutes and then set 20 anyway**, which is the finding within the finding (Greptile, twice) | Fixed — `LinkPolicy.budget_seconds` (900 s) bounds the sweep itself and every URL it did not reach is reported **unchecked**, neither dead nor live; the job limit is now a backstop around the sweep rather than the thing deciding how much got checked |
| R-3 | Medium | `--pyproject` passed straight to the loader, so a missing, malformed or structurally different file raised `FileNotFoundError` / `TOMLDecodeError` / `KeyError` while `--corpus` returned a controlled 2. Both are path arguments; only one failed like one (CodeAnt) | Fixed — guarded, exits 2 naming the file and the error |
| R-4 | Medium | The log and `INDEX.md` both said an RFC's text is never edited, while this branch appended a decision table to RFC 0040 — consistent only because of the acceptance step neither of them named (CodeRabbit) | Fixed — the carve-out is stated in both: prose never, an accepted row citing its entry yes |
| R-5 | Low | `INDEX.md`'s row for 0040 still described the pre-execution world in the present tense — "zero CI coverage", 236/236, 94/94 (CodeRabbit); plus bandit `B310` on `urlopen` and a complexity flag on `check_structure` (CodeFactor) | Fixed — row reframed as motivation with the gate's live denominators; scheme allowlist enforced in the fetcher and annotated for bandit; `check_structure` split into shape, link and parity passes |

**Refused nothing.** Every finding this round was real. One reviewer suggestion was
answered rather than applied: retiring RFC 0040 from `rfcs/` per the index's lifecycle
rule, because that rule fires "once their work has landed" and the branch is unmerged —
see *Carried into the next unit*.

**R-2 is the entry worth re-reading.** The audit did not miss the number; it wrote the
number down and then chose a bound that contradicted it. A residue note ("worst case ~50
minutes") sitting next to a 20-minute limit is not a record of a decision, it is a defect
with a paper trail — and nothing in the audit's own checks looks for a claim contradicting
a value in the same document.

## Rules distilled

- **A doc-extraction bug and a doc defect are indistinguishable in the report, so
  calibrate the extractor before believing its census.** The corpus's "one known failure"
  was an `IndentationError` the extractor caused by not dedenting nested fences — and a
  gate built on that measurement would have opened with a tolerated failure on day one.
  (D-001, D-002.)
- **"Reported" is not "enforced" — a skip that prints is still a pass.** A gate that
  narrates what it could not check, in the output of a run nobody reads because it was
  green, has a shrinking denominator and no alarm on it. (D-004.)
- **A conditional CI job is only as strong as its paths filter, and the filter's blind
  spot is always the new thing.** A gate over `skills/` composed into a job triggered by
  `src/**` runs on every change except the ones that can break it. (D-005.)
- **Composing a check into `just quality` buys you every path filter that guards it —
  audit all of them, not the one you were looking at.** The same gate was skipped twice
  over for the same change class, once by `ci.yml`'s `changes.code` filter and once by
  `.pre-commit-config.yaml`'s top-level exclude, and finding the first did not surface the
  second. (D-005, D-007.)
- **A gate that selects what it checks has a spelling it does not recognize, and that
  spelling is a hole shaped exactly like a pass.** ` ```py ` is Python to every reader and
  was Python to nothing in the checker; the corpus happened not to use it, which is luck,
  not coverage. Enumerate the aliases of anything you filter on. (A-1.)
- **Refuse the empty denominator at the seam that computes it.** "0/0 checked, ok" is what
  every extractor looks like after it breaks, and no caller downstream can tell it apart
  from a clean corpus. (A-1.)
- **Where a check's message names a shape, check that shape — not a weaker one that
  happens to be easier to match.** `## Anti-patterns` was enforced as "a heading with this
  text at any depth", so the error message was a claim the code did not keep. (A-4.)
- **A number in prose and a number in config are a claim and a setting, and nothing checks
  them against each other.** The audit wrote "worst case ~50 minutes" into its own residue
  and set a 20-minute job limit on the same branch; both survived a full self-audit because
  no pass asks whether a document contradicts a value beside it. (R-2.)
- **Bound the work where the work is, not where the runner kills it.** A sweep whose only
  duration limit is the CI job's gets terminated mid-run having reported nothing — the
  budget belongs in the policy, and everything past it is reported unchecked rather than
  dropped. (R-2.)
- **An audit fix is new code and inherits the surroundings' discipline like any other.**
  The tests added to close a coverage gap resolved paths against the working directory
  while every sibling guard test anchors on `__file__` — a defect introduced by the pass
  that was hunting defects, and found only because the fixes were audited too. (A-7.)
- **Measure the property the gate will require, not the properties that are easy to
  count.** §1 measured blocks, imports, URLs and links and pronounced the corpus green;
  the one structural rule §3.3 went on to require was the one thing never counted, and it
  was already failing. (D-003.)

## Carried into the next unit

- **`tools/` is outside every linter's scope**, as `.github/scripts/` already is:
  `just quality` runs `ruff check src`, `mypy src` and `vulture` over `src/` only. The new
  checker is kept clean against `ruff`, `ruff format` and `mypy` by hand (only `T20`,
  print-in-a-CLI, is suppressed by not linting), but nothing enforces that. Extending the
  lint scope to `tools/` and `.github/scripts/` is a repository-wide policy change this
  RFC does not authorize.
- **The census is report-only and currently reads 19 imported / 3 prose-only / 7 absent**
  of 29 wheel packages. Absent: `forze_cli`, `forze_dst`, `forze_duckdb`,
  `forze_firestore`, `forze_kafka`, `forze_meilisearch`, `forze_mongo`. Prose-only:
  `forze_mcp`, `forze_neo4j`, `forze_socketio`. RFC 0042 owns what that number must be.
- **RFC 0041's index↔reference parity check** is the extension point: `_check_index_parity`
  in `tools/skills_check/checks.py` currently reads `skills/README.md`'s table, which is
  the pre-0041 shape.
- **The `fragment` marker has zero users.** If the corpus never acquires one, a later unit
  should decide whether the mechanism earns its place or the rule becomes "every block
  parses, full stop".

---

# Unit 2 · Skills consolidation

Branch `feature/skills-consolidation`. RFC 0041 in full — the §6 map, the §5 routing index,
the §9 execution order, and the §7 hard cut. Extends RFC 0040's gate to the new shape.

**Drift count: 0.** Three entries: one `spec-gap` pair that halted execution before any file
was written (D-008, D-009), and one `discovery` (D-010).

Both halts were surfaced and resolved by the author before execution started, which is the
whole point of the plan gate — §6.1 makes RFC 0041 the sole authority over the file set, so
neither was execution's to decide.

## D-008 — The locked 48-file map contradicts §2's own splitting rules

- **Touches:** RFC 0041 §6 (locked map), §2 rules 3 and 4, §10's size criterion.
- **RFC said:** 48 reference files; and separately, "60–250 lines. Under 60 usually means it
  belongs with its neighbour", plus "a file that is only ever read together with another is
  not a separate file".
- **Found:** projecting every mapped section before writing anything, **20 of the 46 mapped
  files came out under 60 lines**, and `mcp` and `authz` at **4 lines each** — the exact
  shape rule 4 exists to forbid. The two locked statements could not both hold.
- **Because:** the map was built by routing sections, and §2's floor was written about the
  files those routes produce. Nothing had multiplied the two together.
- **Class:** `spec-gap`. Fully knowable at design time — it is arithmetic over the map and
  the corpus, both of which existed when the RFC was written.
- **Consequence:** halted before writing any file; the author merged five destinations named
  in **no §5 bundle**, so the routing table is untouched: `mcp` → `fastapi-generated-routes`,
  `authz` → `authn`, `deadlines` → `resilience`, `caching` → `document-facade`,
  `tenancy-admin` → `tenancy`. Count 48 → 43. Ten files remain under 60 and are recorded in
  D-010 rather than merged, because each is a distinct job or a §5 bundle member.
- **Proposed row (RFC 0041):** `LOCKED` — the reference count is a *consequence* of §2's
  splitting rules, never an input to them. A map is checked against the floor before it is
  called locked.

## D-009 — One section had no destination, so §6's "nothing is dropped" was false

- **Touches:** RFC 0041 §6, first line.
- **RFC said:** "Every current `##`/`###` section has exactly one destination; nothing is
  dropped."
- **Found:** `forze-resilience-deadlines` §Gotchas — 7 lines, four bullets — appears nowhere
  in the map. Every other section of every other skill does.
- **Because:** the map routes `resilience-deadlines` into three destinations by name and the
  §Gotchas heading is not among them. A completeness claim was asserted rather than checked.
- **Class:** `spec-gap`. A script over the map and the corpus finds it in seconds, and that
  is what found it.
- **Consequence:** amended §6 to route it — the retry, rate-limiter and bulkhead bullets to
  `resilience.md`, the `mutates_shared_state` bullet to `shutdown-fleet.md`. The extraction
  script now asserts zero orphaned sections, so the claim is mechanically true rather than
  asserted.
- **Proposed row (RFC 0041):** `ASSUMED` — a completeness claim over a map is checked by
  script before the map is locked. "Nothing is dropped" is a testable statement.

## D-010 — Eleven references remain under the 60-line floor, with reasons

- **Touches:** RFC 0041 §10 ("none is under 60 without a recorded reason") and §2 rule 3.
- **RFC said:** a reason must be recorded. This is that record.
- **Found:** after the D-008 merges, seam repair, routed anti-patterns and reference links,
  eleven files sit below the floor. Each is one complete job, and six of the eleven are named
  in a §5 bundle — merging them would fold two jobs into one file to satisfy a line count.

  Line counts as of `05d004c`, and they have already moved once inside this branch: B-3's
  import repairs added a line or two to several of these files. That is the carried item
  below arriving early rather than a separate defect — a hand-maintained table of numbers
  nothing recomputes is wrong by default, and the reasons, not the counts, are the record.

  | Lines | Reference | Why it stays |
  |---|---|---|
  | 34 | `architecture` | A primer, read first and once. Length is the point. |
  | 37 | `testing-with-mock` | Named in two §5 bundles. "Test it with the mock" is a whole job that is genuinely short. |
  | 43 | `outbox-notifications` | Stage-in-transaction/relay-after-commit is one procedure with one correct shape. |
  | 47 | `deps-resolution` | §5 bundle member; folding it into `runtime-lifecycle` would recreate the over-large file this RFC split. |
  | 47 | `oidc` | External IdPs are a distinct decision from the authn pipeline beside it. |
  | 49 | `aggregate-kit` | §5 bundle member and the single-declaration story; deliberately not the models. |
  | 51 | `shutdown-fleet` | Drain, quiesce and fleet posture are one operational procedure. |
  | 51 | `spec-naming-and-routes` | §5 bundle member, and the rule the whole corpus leans on. |
  | 52 | `secrets` | One lookup, cleanly separable from tenancy and authn. |
  | 54 | `logging-metrics` | Instrumenting a registry is one job; splitting logging from metrics would create two files always read together. |
  | 59 | `query-dsl` | §5 bundle member. Filter, sort, projection and paging are one vocabulary; the port that runs it is its own file. |

- **Class:** `discovery`. The exact residue is only knowable once the text has been moved,
  the seams repaired and the tails routed.
- **Consequence:** §10's criterion is met by record rather than by merging. If a later unit
  finds two of these always read together, rule 4 applies and they merge — that is the
  trigger, not the line count on its own.
- **Proposed row (RFC 0041):** `ASSUMED` — the 60-line floor is a smell, not a gate. A file
  below it is kept when it is one job and merging would fold two jobs together; the reason
  is recorded per file.

**Deliberately not applied:** §9 step 2 asks for "one commit per group so review is
tractable". The corpus move and the gate extension shipped as **one** commit instead. They
are not separable: `load_corpus` could not see `references/` at all, so any ordering that
splits them leaves a commit where the corpus is unchecked and the gate reports green over an
empty denominator. That was observed, not reasoned about — pre-commit stashed the gate change
and the old gate ran against the new corpus, reporting `0/0 python block(s) parsed`. RFC
0040's zero-denominator refusal (unit 1, A-1) is what turned it into a failure instead of a
pass.

## Audit findings — 2026-08-16

Adversarial pass over the branch: 3 commits, 60 files, +2952/-2603, against merge base
`668ca42f4`. Two findings, both fixed. B-3 was added later, from the §10 agent runs rather
than from this pass — three findings now, all fixed.

| # | Severity | Finding | Status |
|---|---|---|---|
| B-1 | Medium | **39 of the 43 references opened cold on a `##` heading**, with no sentence saying what the file covers or when a sibling is the better read. §6 calls prose repair "the real work" and the execution did the mechanical half: sections were moved and links rewritten, but the orientation those sections had from their surrounding narrative was left behind. A reader arriving from the index landed mid-topic in 39 of 43 cases | Fixed — an orienting paragraph per file, each naming the neighbouring reference where the boundary is easy to get wrong |
| B-2 | Medium | A file named `SKILL.md` nested under `references/` was matched by **no** loader branch: the skill glob is one level deep and the reference walk excluded by filename. It would ship — the installer copies the skill directory recursively — and be checked by nothing. Reproduced: a nested `SKILL.md` containing an unparseable block passed every gate | Fixed — references are excluded by *identity* (the set of loaded index paths) rather than by filename |
| B-3 | Low | Examples **used framework symbols they never imported**, so they could not be pasted into a file and run. Neither gate can see this: `ast.parse` accepts an undefined name and the import check resolves only the imports that *are* written. Surfaced by the §10 agent runs, not by the audit — three of the six independently reported it as a gap in the skill | Fixed, corpus now at zero. Measured against `main` first, because that decides scope: 2 were introduced (`record_event`, `SimulationConfig`, both in the net-new DST prose — the split itself introduced none) and 11 were inherited (`build_runtime`, `PostgresDepsModule`, `RedisDepsModule`, `S3DepsModule`, `SQSDepsModule`, `TemporalDepsModule`, `RoutedPostgresClient`, `attach_authn_routes`, `temporal_worker_lifecycle_step`, `DocumentSpec`, `ExecutionContext`). The inherited eleven are outside this RFC's scope and were fixed on request after being reported |

**Sabotage sweep: 5 mutations, 5 killed.** Against the real corpus, reverted after each: a
reference dropped from the index, an unindexed reference added, a second skill directory left
behind (§7's post-condition), a link escaping the published tree from a *reference* rather
than the index, and a broken reference-to-reference link. The fourth is the one worth naming
— it fails only because B-2's sibling defect was caught in the same pass: the escape rule was
originally keyed on `is_skill`, which left 43 of 44 published files unguarded, and a synthetic
test caught it before the sabotage did.

**Fidelity, mechanically:** 127 python blocks and 302 import assertions before the move; 135
and 318 immediately after it, with the deltas being exactly the new DST pair. All **237**
distinct import pairs from the old corpus are present in the new one, and the extraction
script asserts zero orphaned sections rather than trusting the map. The 318 is the figure at
the move, which is what makes it a fidelity measurement; the branch now reports **336**,
because B-3 added the fifteen imports those examples were calling without.

**What remains distrusted.**

- ~~**§10's behavioural criterion is not verified.**~~ **Verified 2026-08-16 — 6 of 6 rows,
  zero missed references.** Six agents, one per row, each given an isolated copy of the skill,
  a realistic request naming no file, and no access to this repository or the web. Measured
  with an inotify watcher on the six directories rather than by self-report, because
  `noatime` on both mounts rules out access times and an agent's account of what it read is
  the thing under test. Every agent opened every reference its row promises. Each also read
  1–10 beyond it, which is the routing table working as §5 describes rather than drift: run 3
  added `operation-composition` to wire the operation, run 4 added `authn` for the plane under
  the middleware, run 6 added `resilience` because the task mentioned retries. The self-reports
  matched the watcher exactly in all six.

  Two caveats on how much this establishes. The rows were exercised **one task each**, so what
  is verified is that each row is followable and was followed once, not that it survives every
  phrasing of its task. And the agents are the same model family as the author, which is the
  standing limit on any evaluation of routing written by the thing being routed.
- **The DST pair is new prose about a real API.** Every snippet in it was executed against
  the installed `forze_dst` — three errors were found and fixed that way, including an
  invented `probability=` kwarg — but the surrounding claims are review-checked only, and
  RFC 0040 §5 rules argument-level drift out of scope by design.
- **Ten references sit under the 60-line floor** with reasons recorded in D-010; nothing
  mechanically re-checks those reasons if a file later grows or shrinks.

**Also deliberately not applied:** RFC 0041 §1 and §7 still link to
`../skills/forze-wiring/SKILL.md` and name the 21 directories. Those links now dangle. The
prose describes the pre-execution state and is left exactly as written — correcting it would
make the RFC's own argument unreadable and is the laundering this log exists to prevent.
- **The scheduled sweep is unproven in CI** until its first cron firing (audit residue).
  Watch the first run for the step summary and the job timeout.
- **Mechanize row 5 rather than leaving it as prose.** Accepted 2026-08-15 with the
  follow-up scheduled, not done: a guard test that fails when a `just quality` input is
  absent from `ci.yml`'s `changes.code` filter. The rule was missed once already, and a
  rule that lives only in a decision table will be missed again the same way.
  `tests/unit/test_ci_matrix_guard.py` is the precedent for mechanizing a CI-config
  invariant. Out of scope for RFC 0040.
- **Row 6's trigger: a third scheduled check.** Folding the sweep into a renamed, general
  nightly workflow with per-job meaning was considered and deferred — worth doing when
  there is a third tenant, not a second, since it rewrites an unrelated workflow's
  identity.
- **The pre-commit exclude covers `.agents/skills/` but not `.agent/skills/` or
  `.claude/skills/`**, so the three byte-identical mirrors `AGENTS.md` says to treat as one
  unit get different pre-commit treatment. Predates this branch, left alone deliberately
  (row 7 fixed the gate without disturbing the exclude), and worth a look by whoever next
  touches the mirrors.

## Review findings — 2026-08-16

PR #377. Five findings from CodeRabbit and Greptile, all fixed, none refuted. Two are code
defects; three are the record contradicting itself.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-6 | **Medium** | Index parity matched references **one level** under the skill directory while the loader walks recursively, so a nested file was in neither set — routed by nothing, reported by nothing, and copied out by an installer that recurses. Every other check passes such a file (Greptile) | Fixed — parity compares paths relative to the skill directory at any depth. Reproduced first: the obvious repro is caught by *other* checks, so it took a nested file with a `## Reference` section and no relative links to show the hole |
| R-7 | **Medium** | The link-escape rule drew its boundary at the corpus root, so a shipped link to `skills/README.md` or `skills/AUTHORING.md` resolved here and dangled wherever the skill is installed — an install copies `forze-skills/` and leaves its siblings behind (Greptile) | Fixed — the boundary is the skill directory, the unit the installer actually copies |
| R-8 | Low | §6's opening still claimed every section has **exactly one** destination, which D-009's own fix falsified: routing `resilience-deadlines` §Gotchas split it by bullet across two files (CodeRabbit) | Fixed — "at least one", with the split named in the opening and its provenance already per-bullet in the map |
| R-9 | Low | D-010 said "eight of the ten" references under the floor are named in a §5 bundle. Three things were wrong: the count is **six**, the set is **eleven** (`query-dsl`, 59 lines, was missing from the table entirely), and every line number had drifted (CodeRabbit found the first) | Fixed — table recomputed, `query-dsl` added with its reason, counts stamped with the commit they were taken at |
| R-10 | Low | The audit summary said "Two findings, both fixed" above a table listing B-1 to B-3, and the fidelity paragraph reported 318 imports where the branch reports 336 (CodeRabbit) | Fixed — B-3's later provenance stated; 318 kept as the figure *at the move*, which is what makes it a fidelity measurement, with 336 named as the current count and why |

**R-6 and R-7 are one defect wearing two faces, and it is B-2's.** B-2 taught the *loader*
that a nested file ships. Neither downstream check learned it, so the audit fixed the
component it was looking at and left two consumers holding the old assumption. A fix that
changes what a shared structure means has to be followed to every reader of that structure —
the audit checked that the loader was right, not that anything else still was.

**R-9 is the cost of a hand-maintained table.** Its numbers went stale inside the same
branch, from a commit whose subject says nothing about line counts. The carried item calls
for a check; until there is one, every number in that table is wrong by default.

## Rules distilled

- **Multiply a locked map out against its own sizing rule before calling it locked.** Both
  halves of RFC 0041 were written carefully and neither was checked against the other; the
  arithmetic that showed 20 files under the floor and two at 4 lines took one script and
  existed to be run at design time. (D-008.)
- **A completeness claim over a map — "nothing is dropped" — is a testable statement, so
  test it.** One section of one skill had no destination, and the sentence asserting
  otherwise had been true of every earlier draft. (D-009.)
- **Moving text is not migrating it.** Links can be rewritten mechanically and imports
  verified mechanically, and the corpus can still be worse to read: 39 of 43 files kept
  their content and lost the orientation their surrounding narrative gave them. The
  mechanical half passing every gate is what makes this easy to call done. (B-1.)
- **Exclude by identity, not by name.** A loader that skips "files called `SKILL.md`" leaves
  one nested somewhere in neither bucket; a loader that skips "the index files I loaded"
  cannot. The same shape as keying a published-file rule on `is_skill`. (B-2.)
- **A structural change and the gate that checks it are one commit.** Splitting them leaves a
  commit where the corpus is unchecked and the gate green over an empty denominator — which
  is not a hypothetical here: pre-commit stashed the gate change and the old gate reported
  `0/0 python block(s) parsed` against the new corpus. (D-010's note.)

## Carried into the next unit

- **Nothing mechanically catches a used-but-unimported symbol.** The corpus is now at zero
  (B-3), but that was measured by a throwaway script, not a gate: the count can regress on the
  next edit and no check would notice. A real check has to resolve each block's free names
  against the imports its own file supplies, and must distinguish a framework symbol from the
  reader's own placeholder — `Project` and `ResourceName` are *meant* to be undefined, which is
  why the naive version reports 128 findings for 13 real ones. That discrimination is the whole
  design problem, and it is why this is a note rather than a gate.
- **A row is verified at one phrasing, not as a contract.** The §10 runs cover six tasks. A row
  reworded, or a seventh added, is an unexercised case, and nothing re-runs the check.
- **RFC 0042 lands on this shape.** Its work is import blocks in existing reference files,
  which §6.1 says needs no amendment; a new file does. The parity gate now enforces that. Its
  own evidence table still cites the 21-directory corpus (`forze-fastapi-interface/SKILL.md`
  at §81), so whoever executes it re-measures against `references/` first — the D1/D2 grades
  were assigned to files that no longer exist.
- **RFC 0041 §1/§7 links dangle by design.** They describe the pre-execution state. If a
  future gate ever checks links under `rfcs/`, these are the expected failures and the
  answer is to exempt historical prose, not to rewrite it.
- **The reference-size reasons in D-010 are a record, not a check.** Nothing notices if
  `secrets` grows to 300 lines or `architecture` shrinks to 10.

---

# Unit 3 · Skills coverage ratchet

Branch `feature/skills-coverage-ratchet`. RFC 0042, all of it — the 13-unit content tranche
and the ratchet that holds it.

**Drift count: 0.** Two halts on `LOCKED` material were surfaced before any file was
written and resolved by the author (D-011, D-012); three further departures are `discovery`
or `spec-gap`.

## D-011 — A subdivided package keeps its own census unit

- **Touches:** RFC 0042 §1.1 (the unit rule) and §4.1 ("the other 19 units"). `LOCKED` —
  §Status locks the doctrine and the ratchet, so the executor halted rather than deciding.
- **RFC said:** two things that do not reconcile. §1.1 defines the unit as "the import root,
  subdivided wherever an extra draws a boundary inside a package". §4.1 then counts "the
  other 19 units", which is the *package* count, and assigns them D1 by observation.
- **Found:** the two readings give different denominators, and the difference is not
  cosmetic — it decides what the no-doctrine rule fires on. Replacing a subdivided root with
  its submodules drops `forze_kms` from the list entirely; keeping both makes 38 units where
  §4.1's arithmetic implies 29.
- **Because:** a package can gain an uncovered submodule while its root stays green on some
  other submodule's import. That is the exact failure §1.1 exists to catch, and dropping the
  root trades one blind spot for another.
- **Class:** `spec-gap` — knowable from the document alone, and found by multiplying the
  rule out against the real package and extras lists before writing the manifest.
- **Consequence:** 38 units — 29 packages, 8 extra-drawn sub-units, and
  `forze_identity.authz` on merit. §4.1's "19" is a stale figure describing the
  package-keyed census that preceded the rule.
- **Proposed row (RFC 0042):** `LOCKED` — subdivision *adds* a unit and never replaces the
  root; the doctrine map is total over that combined list.

## D-012 — `forze_mock.server` is a census unit nobody triaged

- **Touches:** RFC 0042 §4 (triage) and §5.1 (the extras mapping). `LOCKED`.
- **RFC said:** §5.1 left the row as *"`mock-server` — to be confirmed at execution, the
  module is not named by the extra"*. §4 triages ten packages and four sub-units, and this
  is in neither table.
- **Found:** the module exists and is `forze_mock.server`, confirming the row. Nothing in
  the corpus imports it. So the extras rule makes it a unit, §5's no-doctrine rule fails the
  build on it, and the RFC supplies no answer — the build would have broken on day one over
  a decision the document never made.
- **Because:** §5.1's open row was read as a naming question. It was also a triage question,
  because confirming the mapping creates a unit that then needs a doctrine.
- **Class:** `spec-gap`.
- **Consequence:** D2, and covered. Serving the mock over HTTP is a choice an application
  author makes, which rules out D3, but it is reached like the rest of `forze_mock`, which
  is already well covered — the anchor is the whole requirement.
- **Proposed row (RFC 0042):** `ASSUMED` — confirming an extra's module mapping is also a
  triage decision, and §5.1's open rows should carry a doctrine alongside the name.

## D-013 — The triage table's evidence describes a corpus that no longer exists

- **Touches:** RFC 0042 §2 and §4, which cite `forze-documents-search`,
  `forze-graph-contracts`, `forze-fastapi-interface` and `forze-realtime`.
- **RFC said:** for 12 of the 13 units, "the fix is a four-line import-plus-wiring block
  next to a paragraph that already exists".
- **Found:** those four skills were deleted by RFC 0041, so every destination had to be
  re-derived against the 43 reference files. The premise survives the move — every backend
  is still discussed by name — but five packages (`forze_mongo`, `forze_meilisearch`,
  `forze_firestore`, `forze_kafka`, `forze_duckdb`) had **no occurrence of their package
  name anywhere in the corpus**, only of the backend's human name.
- **Class:** `discovery` — RFC 0042 was written against the pre-consolidation corpus and
  §6 explicitly allows either sequencing.
- **Consequence:** destinations re-derived; recorded here so the next reader does not try
  to follow §4's links. Also: `forze_dst` was promoted to D1 by §4 and **satisfied by RFC
  0041**, so the committed tranche is nine D1 units of new content, not ten — and lands at
  13 again only because D-012 added one.

## D-014 — Content committed before the gate that enforces it

- **Touches:** RFC 0042 §6, execution steps 2 → 3 → 4.
- **RFC said:** extend the census to the unit rule and re-measure *before* the content
  work, "or the content work is aimed at the wrong list".
- **Built:** the measurement came first, as instructed — the unit rule was implemented and
  run, and it is what produced the list of 13. But it was **committed** after the content,
  in the order content → mechanism.
- **Because:** an enforcing census with 13 unproven units is a red build. Committing it
  first leaves an intermediate commit that fails its own gate, which is the mirror image of
  unit 2's D-010 note: there the risk was a green gate over an empty denominator, here it
  is a red gate over incomplete content. Both are commits that cannot be checked out and
  trusted.
- **Class:** `discovery`.
- **Consequence:** every commit on this branch is green. The RFC's ordering constraint is
  about *aim*, and aim was preserved; only the commit boundary moved.

**Deliberately not applied:** the two keyword-argument errors below argue for a gate that
checks call kwargs against real signatures, and RFC 0040 §5 rules argument-level drift out
of scope by design. It stays out of scope. A one-off script did the checking instead, and
the case for promoting it is recorded under *Carried into the next unit* rather than
acted on here.

## Audit findings — 2026-08-16

Adversarial pass over the branch: 3 commits, 14 files, against merge base `e9e909781`.
Four findings, all fixed. Two are defects in content the import gate reports as green.

| # | Severity | Finding | Status |
|---|---|---|---|
| C-1 | **Medium** | Two of the new blocks called a real function with **keyword arguments it does not accept** — `register_tools(exposed=…, auth=…)` and `ForzeSocketIOAdapter(server=…, router=…)`. Both import cleanly, so the gate resolved every symbol and reported green on a snippet that raises `TypeError` for anyone who runs it. RFC 0040 §5 rules argument-level drift out of scope by design, so **nothing in CI can see this** | Fixed against the real signatures and the authoritative docs page. Then swept: a one-off checker resolved every call in the corpus whose callee was imported from a `forze*` module and compared its keywords to the live signature — **284 calls, 0 bad keywords** remaining |
| C-2 | **Medium** | A Meilisearch claim was wrong in the consequential direction. I wrote that an attribute the engine was never told about cannot be filtered — implying the lists must be declared. They **default** from the `SearchSpec`; the hazard is the opposite, that *pinning* one overrides the derivation and silently drops a declared field | Fixed by reading `_filterable_attributes` rather than the config docstring, which says only "override … for ensure_index". The corrected text also carries the facetable-field exception the adapter enforces |
| C-3 | **Medium** | A manifest with every unit in D3 or D4 would report `0/0 D1+D2 unit(s) proven` and pass — the zero-denominator vacuous pass RFC 0040's A-1 refused everywhere else, reintroduced in the one check whose entire job is coverage. Not reachable by deleting rows (totality catches that), only by triaging everything out of scope | Fixed — a census with nothing to prove is refused, with a test that is red without it |
| C-4 | Low | The summary line printed `37/37 D1+D2 unit(s) proven` while the check was failing on manifest violations — a ratio computed from a unit list that had just failed to validate | Fixed — a broken manifest is the headline, because the denominator is not yet trustworthy |

**Sabotage sweep: 5 mutations, 5 killed.** Against the real corpus and the real manifest,
reverted after each: a D1 unit losing its last import, a doctrine row deleted, a D3 with no
rationale, a subdividing extra pointed at a module that does not exist, and a D2 anchor
demoted from an import to a sentence.

**§7's injected regression, run twice against this repository — not only against the
fixture.** `src/forze_opensearch` added to the wheel targets fails with *"census unit
`forze_opensearch` has no doctrine"*. A `kms-azure` extra added to
`[project.optional-dependencies]` fails with *"extra `kms-azure` is in no table"* — and that
is the one worth naming, because `forze_kms` is already green, so a wheel-targets-only
ratchet sees nothing at all. **The first attempt at this injection silently did not
apply** — the string it patched did not match the file's actual indentation, and the run
came back green. A sabotage that does not land looks exactly like a gate that works.

**What remains distrusted.**

- **Nothing checks call arguments, and C-1 shows one hour of writing produces two errors.**
  The 284-call sweep was a scratch script; the corpus can regress to a `TypeError` example
  on the next edit with every gate green. This is the strongest candidate the skills work
  has produced for a new gate, and it is deliberately not one here.
- **Positional arguments and value types are unchecked even by that script.** It compares
  keyword names only, so `MockApp(build_app=…, deps=(), seed=…)` is verified to have those
  parameters and not that a `SeedPlan` is what `seed` wants.
- **The prose beside each block is review-checked only.** Every distinctive claim was
  traced to a source docstring or an adapter before being written — which is how C-2 was
  caught — but that is a procedure, not a gate.

## Rules distilled

- **Confirming a mapping is also a triage decision.** §5.1's open row asked only what module
  `mock-server` names. Answering it created a census unit, which then needed a doctrine the
  RFC never assigned — and the build would have failed on day one over a decision nobody
  made. An open row that produces a new entity leaves two questions open, not one. (D-012.)
- **When a rule and a count disagree, the count is the stale one.** §1.1 stated the unit rule
  and §4.1 counted units the old way. Prose survives a rule change; arithmetic derived from
  the superseded rule does not, and reads as authoritative. (D-011.)
- **A gate's blind spot migrates into whatever it green-lights.** The import gate checks
  symbols, so the errors that survived were in arguments. Writing to a gate optimises for the
  gate; the unchecked dimension is where the defects go. (C-1.)
- **Read the implementation, not the field's docstring, before writing what a knob does.**
  `filterable_attributes` is documented as "override … for ensure_index"; only the adapter
  says what happens when it is `None`, which is the case every reader is in. (C-2.)
- **Every new coverage number needs its zero case refused explicitly.** "0/0 proven" is a
  passing ratio. This is A-1 from unit 1 arriving in a new check, which is the fourth time
  the same shape has appeared in this file. (C-3.)
- **A sabotage that does not apply reports green.** The first stub-package injection patched
  a string that did not match the file, and the run passed. Assert that the mutation landed
  before believing what the gate says about it. (Audit note.)

## Carried into the next unit

- **The skills epic closes here.** RFC 0040 built the gates, 0041 restructured the corpus,
  0042 decided what it must cover and made that decision enforceable. `INDEX.md`'s next free
  number is 0043 and no further RFC in this family exists.
- **A call-argument gate is the outstanding candidate.** C-1 plus unit 2's B-3 are the same
  category — a snippet that imports cleanly and cannot be run. B-3 was free names, C-1 is
  keyword arguments; both were found by scratch scripts and neither is enforced. RFC 0040 §5
  put argument-level drift out of scope deliberately, so promoting it is an RFC, not a patch.
- **The extras suggestion in the failure message can be wrong.** `kms-azure` produces
  *"perhaps `forze_kms_azure`"* where the answer is `forze_kms.azure`. It is labelled a
  suggestion and rule 2 refuses a wrong guess, so this is a rough edge rather than a defect —
  but the first thing a reader sees is a name that does not exist.
- **~~RFC 0042's evidence table cites the deleted 21-skill corpus.~~** Closed by D-013: the
  destinations were re-derived against the 43 reference files. The RFC's own links are left
  as written, per the rule that historical prose is not rewritten.
- **The doctrine map is now the thing that rots.** Nothing checks that a D1 unit's block is
  still a *worked example* rather than a bare import that happens to resolve; the distinction
  between D1 and D2 is enforced only by review.
