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

## Decision-row outcomes — 2026-08-16

Both halts were put to the author before any file was written, with a recommendation and the
alternatives. Both recommendations were accepted unchanged; the remaining three rows were
proposed at reconciliation and accepted. Nothing was refused this unit.

| RFC | Row | Outcome | Grade | Decision | From |
|---|---|---|---|---|---|
| 0042 | 1 | accepted | `LOCKED` | Subdivision adds a unit, never replaces the root — 38 units | D-011 |
| 0042 | 2 | accepted | `ASSUMED` | Confirming an extra's mapping also assigns a doctrine; `forze_mock.server` is D2 | D-012 |
| 0042 | 3 | accepted | `ASSUMED` | `forze_dst` discharged by RFC 0041; §4's evidence links are stale | D-013 |
| 0042 | 4 | accepted | `ASSUMED` | Measure before the content, commit after it | D-014 |
| 0042 | 5 | accepted | `ASSUMED` | A census with no D1 or D2 unit is refused | C-3 |

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

---

# Unit 4 · Durable-store tenant resolution

Branch `feat/durable-tenant-resolution`. RFC 0046 in full — §5.1 the effective tenant,
§5.2 the worker surface, §5.3 the seal, §5.4 the wiring floor.

**Drift count: 0.** Four entries: two `OPEN` rows the RFC delegated (D-015, D-016) and two
unlisted decisions the plan needed and the document does not settle (D-017, D-018).

## D-015 — The control surface keeps the exact tenant match

- **Touches:** RFC 0046 §5.2 and decision row 9. `OPEN` — the row asks execution to decide
  whether the untagged allowance (`tenant_id IS NULL`) extends to `request_cancel`,
  `refuse_cancel` and `list_runs`, which today match the bound tenant exactly.
- **RFC said:** "making them consistent with §5.2 is defensible, and so is leaving the
  control surface strict. Whichever, the battery states it."
- **Found:** the two surfaces answer different questions. The worker verbs are reached with
  a run id the caller already holds, and the allowance exists there to stop an untagged run
  being stranded in a reclaim loop — a liveness argument. The control verbs are reached by
  an operator asking about runs it has *not* seen: `list_runs` enumerates, and
  `request_cancel` is documented at
  [`configs/durable.py`](../src/forze_postgres/execution/deps/configs/durable.py) as granting
  run control to anyone who can resolve the admin key. Extending the allowance there would
  put every untagged run into every bound operator's listing, which no failure mode requires.
- **Because:** the allowance is a liveness fix, and the control surface has no liveness
  problem — an untagged run nobody can cancel from a bound context is still cancellable from
  an unbound one, and still completes on its own.
- **Class:** `discovery` — the asymmetry only becomes a question once §5.2's predicate
  exists to be copied.
- **Consequence:** `request_cancel`, `refuse_cancel` and `list_runs` are untouched. The
  battery asserts the difference rather than leaving it to be inferred: an untagged run is
  completable under a binding and invisible to that binding's `list_runs`.
- **Proposed row (RFC 0046):** `LOCKED` — the untagged allowance is a worker-surface
  property. The control surface matches the tenant exactly, because widening an enumeration
  is a disclosure and widening a terminal write is a liveness fix.

## D-016 — The step journal gets a route group it does not need for enforcement

- **Touches:** RFC 0046 §5.4 and decision row 10. `OPEN`.
- **RFC said:** the group "adds no enforcement — but omitting it leaves one durable route
  invisible to a declared floor".
- **Found:** confirmed that the journal's key already carries the tenant
  ([`function_step.py`](../src/forze_mongo/adapters/durable/function_step.py), `_doc_id`;
  the Postgres journal's primary key does the same), so the group changes no runtime
  behaviour. What it changes is what `required_tenant_isolation` *means*: without the group,
  a deployment declaring `tagged` gets a tier derived from its other planes and is told
  nothing about a `durable_step` wired with `tenant_aware=False`.
- **Because:** the floor is a statement about the module's routes. A route the validator
  cannot see makes the statement narrower than it reads, and that gap is invisible at the
  call site — which is the failure this whole RFC is about.
- **Class:** `discovery`.
- **Consequence:** all three durable configs get groups. `durable_step` with
  `tenant_aware=False` under a declared `tagged` floor now fails at wiring, which is a
  deployment-visible change and is named in the changelog.
- **Proposed row (RFC 0046):** `ASSUMED` — a route joins the wiring floor because the floor
  describes the module, not because the route needs the check.

## D-017 — §5.1's table does not hold for a tenant-aware store

- **Touches:** RFC 0046 §5.1, specifically the table's last row ("unbound + requested B →
  **B**, and the relation now resolves under B"). Unlisted: the interaction between
  `_effective_tenant` and `tenant_aware=True` appears nowhere in the document.
- **RFC said:** the table presents five cases as total.
- **Found:** they are not. `_effective_tenant` reads the binding through
  `TenancyMixin._tenant_id_for_resolve()`
  ([`mixins.py:43-60`](../src/forze/application/contracts/tenancy/mixins.py)), which on a
  `tenant_aware=True` store with nothing bound **raises** `authentication` /
  `tenant_required` before any comparison happens. So on such a store the last row is
  unreachable: `enqueue(tenant_id=B)` unbound raises rather than resolving under B.
- **Because:** the table was written about the *explicit-versus-bound* question and did not
  account for the mixin's pre-existing fail-closed read, which fires first.
- **Class:** `spec-gap` — knowable from the document plus the mixin, and found by walking
  the five rows against the real helper before writing it.
- **Consequence:** fail-closed wins, and the helper is written so that it does: it calls
  `_tenant_id_for_resolve()` first and never catches. §5.1's last row is true of stores that
  are not `tenant_aware`, which is every durable store a cross-tenant deployment can use
  (§3: `tenant_aware=True` on `durable_run` is incompatible with unbound recovery). The
  battery pins the raise so the narrower claim is checked rather than assumed.
- **Proposed row (RFC 0046):** `LOCKED` — a `tenant_aware` store refuses an unbound call
  before it considers an explicitly passed tenant. The fail-closed read is the mixin's
  canonical contract and this RFC does not carve an exception into it.

## D-018 — The schedule store gets no untagged allowance, and the asymmetry is deliberate

- **Touches:** RFC 0046 §5.2, which specifies the `tenant_id IS NULL` arm for the run
  store's worker surface and says nothing about the schedule store. Unlisted.
- **RFC said:** nothing. §5.1 covers `put`; the other schedule verbs are out of its frame.
- **Found:** the schedule store has no tenant *predicate* to widen — `advance`, `load` and
  `delete` scope the **id** (`scope_tenant_key(schedule_id, bound)`), so tenancy is expressed
  in the key rather than in a filter. An untagged schedule is therefore stored under
  `:{schedule_id}` and is unreachable from a bound caller, which looks like the stranding
  §5.2 exists to prevent.
- **Because:** it is not the same failure. A run is claimed by a scanner and *must* reach a
  terminal state or it re-runs forever; a schedule that a bound caller cannot see simply
  does not fire for that caller, and still fires for the unbound one that registered it.
  Adding a fallback lookup would make `load` ambiguous — `:sid` and `A:sid` can both exist —
  and ambiguity in the key that `advance` compare-and-sets is how a schedule fires twice.
- **Class:** `spec-gap` — the asymmetry is derivable from the two stores' code without
  building anything.
- **Consequence:** the schedule store keeps exact id scoping. Only `put` changes, per §5.1.
  The battery states the difference so a later reader does not "fix" it into the run store's
  shape.
- **Proposed row (RFC 0046):** `ASSUMED` — where tenancy lives in the key rather than in a
  predicate, there is no untagged allowance: a fallback lookup trades a liveness gap for an
  ambiguous key, and the compare-and-set that makes firing exactly-once depends on that key
  being exact.

## D-019 — The rule lives in a free function; the mixin method delegates to it

- **Touches:** RFC 0046 §5.1 and decision row 6, which locates `_effective_tenant` on
  `TenancyMixin`. `ASSUMED`.
- **RFC said:** one method on the mixin, shared by every adapter built on it.
- **Found:** the mock stores are not built on it. `MockDurableRunStore` and
  `MockDurableScheduleStore` carry their own `tenant_provider` and a local `_bound_tenant`
  ([`run_store.py`](../src/forze_mock/adapters/durable/run_store.py)), and row 8 requires
  all three implementations to move together — so a method reachable only through the mixin
  would have left the oracle applying a second copy of the rule.
- **Because:** the design was written from the two relational stores, where the mixin is
  universal. The oracle is the one implementation that deliberately is not an adapter.
- **Class:** `spec-gap`.
- **Consequence:** the comparison is a module-level `effective_tenant(bound=…, requested=…)`
  in the same tenancy package, and `TenancyMixin._effective_tenant` is a one-line delegation
  that supplies the bound value. Row 6's placement decision is unchanged — the rule is still
  tenancy's, not durability's — and there is still exactly one implementation of it.
- **Proposed row (RFC 0046):** `ASSUMED` — a rule the mock must apply too is written as a
  free function that takes both values, with the mixin method as the adapter-side shorthand.

## D-020 — Only one of the two modules uses `TenancyRouteGroup`

- **Touches:** RFC 0046 §5.4 and decision row 7 ("the durable configs join the existing
  `TenancyRouteGroup` machinery unchanged, via a small helper"). `ASSUMED`.
- **RFC said:** one shared helper lifting a scalar config into a one-entry route map, used by
  both modules.
- **Found:** `PostgresDepsModule` does not use `TenancyRouteGroup` at all. It appends
  `PostgresTenancyRouteSpec` values to a list and calls `validate_postgres_tenancy_wiring`
  ([`module.py`](../src/forze_postgres/execution/deps/module.py), the `rotating_credentials`
  block is the precedent for a single optional field). Only Mongo builds groups. A shared
  helper would therefore have had exactly one caller.
- **Because:** §5.4's snippet was written against the Mongo module and the RFC's §3 did not
  check the Postgres one, which reads the same from the outside — both take
  `required_tenant_isolation` and both fail with a `*_tenancy_validation_failed` code.
- **Class:** `spec-gap`.
- **Consequence:** each module follows its own idiom — three `TenancyRouteGroup`s and a local
  `_route` helper on Mongo, three `PostgresTenancyRouteSpec` appends beside
  `rotating_credentials` on Postgres. That required one unplanned edit outside both modules:
  `PostgresTenancyRouteKind` is a closed `Literal` and gains `durable_step`, `durable_run`
  and `durable_schedule`.
- **Proposed row (RFC 0046):** `ASSUMED` — a route joins each module's own validation idiom.
  There is no shared shape to extend: Postgres's route kinds are a closed `Literal` and
  Mongo's are free-form strings, and unifying them is its own change.

## D-021 — The worker verbs are pinned in the shared battery, not in the Postgres tier class

- **Touches:** RFC 0046 §6 ("the tagged-tier isolation class is extended … the worker verbs
  join it, and Mongo gets the mirror"). §6 is prose, not a graded row, so this is unlisted.
- **RFC said:** extend `TestTaggedTierRunControlIsolation` with the worker verbs.
- **Found:** the shared battery covers them against both engines already — `run_tenancy_scenario`
  runs over one static relation, which *is* the tagged tier, and both legs assert the same
  observables. Extending the Postgres class as well would have been a per-engine copy of a
  comparison the battery already makes against the oracle.
- **Because:** §6 was written before the battery scenario's shape was settled, and assumed the
  worker verbs would need a place the tagged tier already had.
- **Class:** `discovery`.
- **Consequence:** the engine-specific files carry only what the battery cannot: namespace
  *placement*, which is invisible over a static relation and therefore invisible to the
  battery. `TestTaggedTierRunControlIsolation` is untouched.
- **Proposed row (RFC 0046):** `ASSUMED` — a property observable over a static relation
  belongs in the shared battery; an engine's own file carries only what a shared relation
  cannot express.

## Audit findings — 2026-09-05

Adversarial pass over the whole branch (8 commits, 28 files, +1908/-71) after execution.
These are departures the executor did not notice while making them; the entries above are the
ones it did.

**F1 — the stored-tenant read was unproven (fixed).** Reverting `complete`'s
`_stored_tenant(run_id)` back to the binding passed all 25 durable integration tests on both
engines. Every fixture builds its store with `cipher=None`, and the read exists only for the
sealed case: an untagged run stays completable from any binding (§5.2), so sealing under the
binding writes ciphertext whose AAD names a tenant the row does not, and `load` — which
unseals with the column — can never open it. The run completes and its result is
unreadable. Both engines now carry
`test_an_untagged_run_completed_under_a_binding_still_decrypts`, verified red without the
read (`core.crypto.aead_auth_failed`) and green with it.

**F2 — both run stores still called their tenancy "Non-enforcing" (fixed).** True before this
branch, false after it; the sentence sat three lines above the predicate that contradicts it.
Both class docstrings now state the split by verb, and both schedule stores say why a
key-scoped tenancy has no untagged allowance.

**F3 — the tenant provider was declared twice (fixed).** Both conformance legs carried an
identical four-line `_provider`, for a scenario whose entire subject is two stores differing
only by that value. It is now `tenant_provider_for` beside the scenario.

Sabotage sweep, all killed: the mismatch refusal (4 legs red), the untagged arm (2 red),
the worker predicate (red), the relation resolution on both engines (2 red each), the
route-group registration (6 red), and — after F1 — the stored-tenant read (2 red).

**Residue.** `claim_abandoned` still matches the tenant exactly, so a bound scanner never
reclaims an untagged run; only an unbound one does. That follows D-015 and is unchanged by
this branch, but it means an untagged run in a deployment whose scanners are all bound is
recovered by nobody — worth a look when the control surface is next revisited.

## D-022 — Corrections to this unit's own record

Appended rather than edited, per the rule that an entry is never revised: what was written
stands, and what turned out to be true is written next to it.

- **Drift count: 0.** Seven entries, not the four the unit header claims — the header was
  written before the audit added D-019, D-020 and D-021, and the count line is revised by
  appending this one rather than by editing that one. Classes: three `discovery`, four
  `spec-gap`, no `drift`.
- **D-017's citation is stale.** It cites `mixins.py:43-60` for `_tenant_id_for_resolve()`.
  That range was correct when the entry was written and stopped being correct in the same
  branch: inserting `effective_tenant` above it moved the function to
  [`mixins.py:91`](../src/forze/application/contracts/tenancy/mixins.py), and lines 43-60 now
  cover `require_tenant_if_aware`. The claim the entry makes is unaffected — the fail-closed
  read happens first either way, and
  `tests/integration/test_forze_postgres/test_pg_durable_tenancy_integration.py::TestNamespacePlacement::test_a_tenant_aware_store_refuses_the_unbound_call_first`
  is the executable form of it.
- **A line number in a citation goes stale from the citing branch's own edits**, which is
  the general shape worth carrying forward: cite the symbol, and add a line only when the
  claim is about a specific line rather than about a function.

## D-023 — The kit method that calls the store needed the rule three times

Appended after review, which found what the audit did not: three defects, all in
`DurableScheduler.ensure_schedule` and none in the stores this unit's design is about.

- **Touches:** RFC 0046 §5.1 (the effective-tenant rule) and D-018, which recorded that the
  schedule store's tenancy lives in its *key* rather than in a predicate.
- **Found:** `ensure_schedule` reads, returns early if nothing changed, and otherwise
  writes. (1) Reading unbound while writing for a named tenant looked up a key the write
  never used, so it re-put on every call and reset `next_fire_at` — the schedule skipped its
  own due fire, silently. (2) Binding the named tenant to make both halves agree replaced an
  existing binding, so the store's contradiction check compared a tenant against itself and
  asking for a tenant became a way to write as it. (3) Settling it at the write left the
  early return unchecked, handing back the *bound* tenant's schedule as the answer to a
  question about another one.
- **Because:** D-018 is what makes this method fragile. Where tenancy is a predicate the
  store applies it on every path; where it is *in the key*, a caller that composes the key
  twice — once to read, once to write — has to get the same tenant both times, and every
  exit needs the rule independently.
- **Class:** `spec-gap`. The RFC scoped its callers as "the shipped paths never take the
  split" and stopped there; it never asked which callers *compose the key twice*.
- **Consequence:** fixed in 2c1bef933, 79fbfad02 and 5807e5406; the rule is now settled
  before the read by calling `effective_tenant`, with the stores still enforcing it. The
  general shape: **settle identity before the read in a read-then-write helper, and test
  every exit — the create path and the no-op path are different tests.**
- **Proposed row (RFC 0046):** `ASSUMED` — where a store expresses tenancy in its key, any
  caller that both reads and writes must settle the tenant once, before the read. Enforcing
  it only at the write leaves every early return unguarded.

---

# Unit 5 · RFC 0078 P0 — pickup probes

Branch `feat/rfc-0078-p0-probes`. RFC 0078 (routed DuckDB lake reads, Draft) §8 P0: every 🔎
in the RFC, run against `duckdb==1.5.6` with the `iceberg` build the core repository serves
for it (`890b78a9c`) and `httpfs` `4bc690d`, and against a vending stack P0 had to build
first (D-024). The stable facts are pinned in
[`test_lake_vending_probes.py`](../tests/integration/test_forze_duckdb/test_lake_vending_probes.py)
— 24 tests on one module-scoped stack that starts in about 30 s. Facts that take minutes to
show (D-028) and the memory footprint were measured with one-off scripts and live here only.

**Drift count: 0.** Seven entries: five `discovery`, two `spec-gap`. Nothing was built against
the RFC, so nothing could drift. Every entry is a fact that came back different from what the
RFC assumed or left open.

**Gate (§8 P0): items 1, 5 and 11a pass by hand, two of them with a caveat the RFC did not
expect.** Battery 1 passes outright. Battery 5 passes, but in the §3.4 profile the refusal comes
from public AWS, not from the store (D-027). Battery 11a passes for the catalog path. A
by-path read inside the revoked table still succeeds until the vended credential expires
(D-025).

**Confirmed, so no entry.** These 🔎s came back as the RFC expected:

- RustFS 1.0.0 honours STS `AssumeRole` session-policy downscoping, and so does the MinIO
  mirror (`RELEASE.2025-09-07`). A vended credential reads its own table and nothing else: not
  tenant B's prefix, not another table in A's own warehouse, and no bucket listing.
- §9 q2 / §3.5: credentials vended to a `select`-only principal are read-only. The store
  refuses `PUT` and `DELETE` from boto3, and it refuses the data-file `PUT` of a DuckDB `INSERT`.
- §3.4 step 4: `READ_ONLY` on the iceberg `ATTACH` is honoured. `duckdb_databases()` reports it,
  and writes fail with `InvalidInputException` before any request goes out.
- §9 q1: the REST catalog's traffic does not go through the filesystem layer. This covers the
  per-statement `LoadTable` and the OAuth2 token request. It survives
  `disabled_filesystems='LocalFileSystem,HTTPFileSystem'` set before or after `ATTACH`. It
  also survives the P2 profile (`allowed_directories` + `enable_external_access=false`) set
  before `CREATE SECRET`, with neither the catalog nor the IdP allow-listed. One ordering rule
  applies: `allowed_directories` must come first. Setting it afterwards raises
  `Cannot change allowed_directories when enable_external_access is disabled`.
- §3.2: every autocommit statement re-issues `LoadTable`, so a revocation reaches the
  catalog path at the next statement. With `MAX_TABLE_STALENESS '1 hour'`, the second
  statement is served from the cached load, and a revocation inside that window goes unseen.
  The reason §3.2 gives for never setting it holds.
- §2.3: the external file cache belongs to one `DatabaseInstance`. A second instance in the
  same process shows zero entries and goes to the network.
- §2.2 / §9 q4: idle RSS per `:memory:` instance with `httpfs` and `iceberg` loaded is about
  3.4–3.8 MiB, the same as a bare instance, because the extensions' code is shared. After
  attaching and running one read with the file cache off, it is about 7.1 MiB. Measured with
  `threads=2`, `memory_limit=1GB`, N=30–50, after `malloc_trim`.
- §4.3: in this build `cursor.execute()` returns a streaming result, and `fetchmany(n)` pulls
  only what it needs. The first 11 rows of a 4.8 s query arrived in 12 ms, with 0.2 MB held.
- §3.2: the catalog token refreshes with a non-empty `CLIENT_ID`. With an 8 s token lifespan,
  the next statement refreshed ahead of expiry and saw no 401.
- §3.2: duckdb-iceberg#1352 is still open. The bundled build carries the re-vend-on-expiry
  path (`refresh_info`) in its source, but it was not exercised live, because 11b runs nightly.
- Battery 11b: the shortest vended-credential life the chain produces is 900 s. Requests for
  1 s and for 30 s both came back as 900 s.
- The RFC's ✅ [probe] claims touched here all reproduce, with one nuance. A connect-time
  `enable_global_s3_configuration` is refused only under `autoload_known_extensions=false`,
  which §3.4 sets. With autoload on, connect loads `httpfs` itself and accepts the option.

## D-024 — RFC 0018 P3's vended-credential fixture does not exist; P0 built a minimal one

- **Touches:** RFC 0078 §7 ("extending RFC 0018 §7's fixture") and §8. P0's gate runs
  "against … the fixture", and P1 "needs RFC 0018 P3's vended-credential fixture".
- **RFC said:** the fixture is consumed, not specified: Lakekeeper with OpenFGA or Cedar, an
  OAuth2 IdP, and an STS-capable store.
- **Found:** RFC 0018 has not started. Nothing under `tests/` runs Lakekeeper, OpenFGA, Cedar
  or an OAuth2 IdP, and no fixture uses STS. P0 built
  [`tests/support/lakekeeper.py`](../tests/support/lakekeeper.py) from four parts:
  - Lakekeeper `v0.14.2` on `ghcr.io/morzecrew/postgres:18`.
  - OpenFGA `v1.22.0` with its in-memory datastore and no authn. The authorizer is real; only
    its storage is not.
  - Keycloak `26.8.1` in `start-dev`, with one confidential client per principal and an
    audience mapper for `lakekeeper`.
  - RustFS `1.0.0`, the store the repository already uses, chosen after a probe showed it
    honours session-policy downscoping.

  The fixture sets up two tenants, each with one warehouse and `select` on its own warehouse
  only. A third principal holds no grant. Each warehouse has one `gold.orders` table, seeded
  through the REST catalog by the operator.
- **Because:** the P0 gate cannot run by hand without a fixture, and three of the RFC's 🔎s can
  only be answered against a real catalog.
- **Class:** `spec-gap`. RFC 0018's status was knowable when RFC 0078 was written.
- **Consequence:** P1 inherits three choices.
  1. Addressing. Lakekeeper hands the storage profile's endpoint to the client as it is, and
     it checks the token issuer against the URL it discovered. So the store and the IdP each
     need one address that both the containers and the test process can reach: the Docker
     network's gateway IP plus the published port. That works on a Linux host and on CI
     runners, but not on Docker Desktop.
  2. `OAUTH2_SCOPE` is `profile`. Keycloak rejects unknown scopes with `invalid_scope`, and
     the realm defines no `lakekeeper` scope, so a mapper on each client supplies the
     audience. Leaving the option out does not help: duckdb-iceberg then sends its own
     default, `PRINCIPAL_ROLE:ALL` (a Polaris role), and Keycloak refuses that at
     `CREATE SECRET`. So `LakeTenantCatalog.oauth2_scope=None` (§2.1) cannot mean "send no
     scope". It can only mean "send duckdb's default".
  3. OpenFGA keeps its store in memory, so a restarted container loses every grant. The
     fixture lives for one test module, so this never shows.
- **Proposed row (RFC 0078):** `ASSUMED` — P1's battery runs on `tests/support/lakekeeper.py`
  until RFC 0018 P3 ships its fixture, then moves onto that one. The gateway-IP addressing is a
  property of the fixture, not of a deployment. `oauth2_scope` is required, not optional,
  because a missing scope sends duckdb-iceberg's Polaris default.

## D-025 — Vended secrets outlive their statement, so revocation does not reach a by-path read

- **Touches:** RFC 0078 §3.2: the 🔎 that vended secrets are "scoped to the transaction and
  discarded after it", and the consequence "Revocation takes effect at the next statement,
  without eviction". Also §2.4's last bullet, decision 14, and battery 11a.
- **RFC said:** each statement starts from a fresh `LoadTable`, and the previous statement's
  credentials are gone.
- **Found:** half of that holds. Every autocommit statement does re-issue `LoadTable`. But the
  S3 secret it vends is an ordinary temporary catalog entry. It is named
  `__internal_ic_<table>__<schema>__<name>__<txid>_…` and scoped to the table location. The
  secret is kept when the statement's transaction commits, and dropped only when the
  transaction rolls back:
  - A fully consumed result commits. That includes `fetchall`, and a `fetchmany(n)` that
    exhausts the stream, which is what the shell's cap+1 probe does on any result inside the cap.
  - A stream closed before it is exhausted rolls back, and so does an explicit `ROLLBACK`. Either
    one discards the secret.

  In normal operation the instance therefore gains one secret per statement per table: three
  statements leave three secrets. After A's grant is revoked, the next catalog statement is
  refused (`CatalogException`, D-029). But in the same instance,
  `read_parquet('s3://lake/tenant-a/<table>/data/…')` still returns rows: a real request,
  signed with a leftover secret and answered 200. The window lasts as long as the vended
  credential: 3600 s by default, 900 s at the floor (unit header).
- **Because:** duckdb-iceberg names the secret per transaction but never drops it. The
  maintainer's description in #1352 matches only the rollback case.
- **Class:** `discovery`.
- **Consequence:** two effects follow.
  - The secret list grows without bound in a long-lived tenant instance. It grows slowly, but it
    has no ceiling.
  - Revocation is total for the catalog path only. A statement that names an `s3://` path
    inside a table A could read keeps working until the credential expires. This is not a
    cross-tenant hole: the leftover secret is scoped to A's own table, and a trusted compiler
    never emits paths.

  The fix is cheap and was probed. `DROP TEMPORARY SECRET "<name>"` still works after
  `lock_configuration=true`, so the client can drop every `__internal_ic_%` secret after each
  statement. The same probe shows that `lock_configuration` does not cover secret DDL; only
  classification stops a caller from issuing it. Battery 11a should assert both legs, and
  `test_lake_vending_probes.py` pins today's behaviour.
- **Proposed row (RFC 0078):** `ASSUMED` — after every statement, `run_dynamic` drops the
  vended `__internal_ic_%` secrets. Revocation then reaches by-path reads at the next
  statement, and the instance never holds more than one statement's credentials. This
  overrides §3.2's "discarded after it", which stays as written.

## D-026 — A namespace the session found missing stays missing for the instance's life

- **Touches:** RFC 0078 §2.4, which forces eviction "on … a grant revocation the app learns
  about" and says "Revocation does not depend on eviction". Unlisted: how a restored grant, or a
  namespace created later, looks to a live instance.
- **RFC said:** nothing. The RFC treats the catalog as re-read on every statement.
- **Found:** after a 404, the attached iceberg catalog caches the namespace as absent. A session
  that saw the 404 keeps refusing with `schema "gold" does not exist`, and sends no HTTP
  request at all. That stays true after A's grant is restored. It also holds for a namespace
  created after the session first asked for it. A fresh instance sees the change at once.
  Tables behave differently: a table created later, inside a namespace the session already
  resolved, is visible to the next statement.
- **Because:** duckdb-iceberg loads the schema set once per attached catalog, but re-reads the
  table set on every lookup.
- **Class:** `discovery`.
- **Consequence:** the failure is closed, so confinement is not at risk. The problem is
  operational. A grant or namespace change reaches a tenant only after `evict_tenant` or a
  `fingerprint_ttl` rotation, and a transient 404 sticks.
- **Proposed row (RFC 0078):** `ASSUMED` — every grant restoration or namespace creation is
  followed by `evict_tenant(tenant_id)`. The routed client documents the rule, and battery 13
  asserts that a rebuilt instance sees the change.

## D-027 — With step 2, an unmatched `s3://` path goes to public AWS, unsigned — not to the store

- **Touches:** RFC 0078 §3.3, which says "the request goes out unsigned" and "The last link is
  outside DuckDB: the store must refuse unsigned requests". Also §3.4's P2 paragraph, §8's P2
  bullet, decision 8, and battery 5.
- **RFC said:** after `SET enable_global_s3_configuration=false`, an unmatched read goes out
  unsigned, and a store that refuses anonymous access closes it.
- **Found:** the first half reproduces. With the store's root key in `AWS_*`, no request is
  signed with it. But "out" means the default AWS endpoint. With global configuration off, the
  `s3_endpoint` setting is ignored as well. So tenant B's paths, named from A's session, went to
  `https://lake.s3.amazonaws.com/tenant-b/…`, and AWS refused them. The fixture's store never
  saw those requests, so the store refusal §3.3 relies on was never exercised. The requests
  carried the bucket name and B's path names onto the public internet.

  The leak is real, and step 2 does close it. Without step 2, and with `s3_endpoint` pointed at
  the store as an operator's environment would set it, the same read succeeds, signed with the
  root key. The P2 profile is stronger again: `allowed_directories=['s3://lake/tenant-a/']`, set
  before `enable_external_access=false`, refuses the same reads inside the engine with a
  `PermissionException` and sends no request. §9 q1 resolved in that profile's favour (unit header).
- **Because:** the RFC's probe stood a local listener in for S3. A listener cannot show where an
  unmatched path is sent once the endpoint setting is ignored.
- **Class:** `spec-gap`.
- **Consequence:** in the §3.4 profile, the outside control for this row is the network egress
  policy, not the store. A deployment without one sends tenant path names to AWS. If a public
  bucket with the same name exists there, the read returns someone else's objects. The test
  keeps this leg offline with a local `http_proxy` that refuses every request, and asserts that
  the `CONNECT` target is the AWS host.
- **Proposed row (RFC 0078):** `ASSUMED` — P1 adopts the allowlist profile instead of leaving it
  to P2. It sets `allowed_directories` to the tenant's warehouse prefix and then
  `enable_external_access=false`, both before `CREATE SECRET`. `disabled_filesystems` stays as a
  second layer, and step 8 reads back both settings. This overrides §3.4's "not the P1 default"
  and §8's P2 bullet, which stay as written.

## D-028 — Neither `http_timeout` nor `cursor.interrupt()` bounds a hung catalog or store request

- **Touches:** RFC 0078 §4.3's timeout cell ("A blocking HTTP read is bounded by
  `http_timeout` (whether interrupt cuts it short is 🔎) … Worst case ≈ timeout +
  `http_timeout`"). Also §3.4 step 6, decision 10, decision 14's formula, and battery 12's
  timeout case.
- **RFC said:** step 6 sets `http_timeout` no higher than the route ceiling, so a hung read
  ends by then even if interrupt cannot cut it.
- **Found:** pausing the store or the catalog container mid-session makes the next statement
  hang for **120.5 s**. It then fails with `IOException: … Timeout was reached`. That happens
  with `SET http_timeout=2; SET http_retries=0` applied after `ATTACH`, where step 6 puts it,
  and again before `CREATE SECRET`. 120.5 s is httpfs's default: 30 s × (1 + 3 retries).

  Calling `cursor.interrupt()` one second in changes nothing. The statement returns 119.5 s
  later with the same `IOException`, not `InterruptException`. A CPU-bound statement stops at
  once on interrupt, so interrupt itself works, just not inside a blocking HTTP call.

  The settings do take effect elsewhere. A plain `TYPE s3` secret honours them, but only when
  they are set before the secret is created; an `http://` read honours them too. The iceberg
  extension's catalog requests and vended secrets never pick them up.
- **Because:** the iceberg path builds its HTTP parameters without the session's
  `http_timeout`/`http_retries`, and neither the iceberg secret nor `ATTACH` exposes a timeout.
- **Class:** `discovery`.
- **Consequence:** the honest worst case for one hung request is about 120 s, and a statement
  that makes several requests in sequence can take longer. The caller sees the timeout late as
  well: after interrupting, `DuckDbClient.__submit` awaits the worker before it raises
  (`client.py:276`), so `dynamic_read_timeout` lands about 120 s after the statement started,
  not at `statement_timeout`. Each hung tenant holds an executor thread and a slot of
  `max_concurrent_queries_total` for that whole time. Decision 14's validity formula still
  holds at the default 3600 s, since 10 × (5 + 120) = 1250 s, but it fails at the 900 s floor.
- **Proposed row (RFC 0078):** `OPEN` — choose one of three options:
  - (a) state the ~120 s bound in the capability row, and size `max_concurrent_queries_total`
    for it;
  - (b) get the iceberg extension to honour `http_timeout` upstream before P1 ships;
  - (c) move the engine out of process (RFC 0021).

  Until one is chosen, step 6 bounds nothing on this path, and the §4.3 cell overstates the
  guarantee.

## D-029 — Catalog refusals arrive as the bare `duckdb.Error`, and Lakekeeper never answers 403 to `LoadTable`

- **Touches:** RFC 0078 §4.5: the row that maps "`HTTPException` 401/403 from the store or the
  catalog" to `dynamic_read_permission_denied`, the row that defers `IOException` to
  infrastructure, and the 🔎 on how a Lakekeeper 403 or 404 on `LoadTable` surfaces. Also
  battery 16.
- **RFC said:** a catalog refusal surfaces as `CatalogException`, `HTTPException` or
  `IOException`.
- **Found:** observed with Lakekeeper v0.14.2 under OpenFGA. Every class below is in `duckdb`:

  | Refusal | Lakekeeper answers | DuckDB raises |
  |---|---|---|
  | `ATTACH` by a principal with no grant, or of another tenant's warehouse | 404 `NoSuchWarehouseException` on `/v1/config` | `Error` itself, not a subclass; the message carries `NotFound_404` |
  | `ATTACH` with a wrong client secret | Keycloak 401 | `Error`, `Unauthorized_401` |
  | the next statement after a revocation | 404 on `LoadTable`, then 404 on the namespace | `CatalogException` ("schema … does not exist") |
  | a `describe`-only principal | **200** on `LoadTable`, with metadata and no credentials | `Error`, "No region was provided via the vended credentials" |
  | `CREATE TABLE` by a `select`-only principal | 403 | `Error`, `Forbidden_403` in the message |
  | `INSERT` by a `select`-only principal | store 403 on the data-file `PUT` | `HTTPException` |
  | a hung catalog or store (D-028) | none | `IOException` |

  No probe produced a 403 on `LoadTable`. Lakekeeper hides what a principal cannot describe
  (404), and withholds credentials from what it cannot read (200 with none).
- **Because:** duckdb-iceberg raises its own catalog-request failures as the plain `Error`. And
  `HTTPException` is a subclass of `IOException`.
- **Class:** `discovery`.
- **Consequence:** three of the refusals the battery cares about fall under no §4.5 row.
  Mapping by class alone would send them to infrastructure, which is the one wrong answer.
  "Never rows" still holds, but a confinement refusal would be retried as if it were an outage.
  Because `HTTPException` is an `IOException`, the HTTP row must be matched before the
  infrastructure row. The `describe`-only case also shows that `describe` discloses table
  metadata, file paths included, to a principal who cannot read the data. That is Lakekeeper's
  model, and worth knowing when grants are handed out.
- **Proposed row (RFC 0078):** `ASSUMED` — `_fetch_rows` maps the bare `duckdb.Error` by its
  message: `NotFound_404`, `Unauthorized_401`, `Forbidden_403` and "No region was provided via
  the vended credentials" all become `dynamic_read_permission_denied`. An `HTTPException` with
  401 or 403 is checked before the `IOException` row, and battery 16 pins the table above.

## D-030 — The 1.5.6 iceberg build has neither credential fix the RFC asked about

- **Touches:** RFC 0078 §3.2. One bullet says "The fix for the empty-`CLIENT_ID` case is
  duckdb-iceberg#1378 … The build that contains it is 🔎". Another asks whether the pinned
  build has #1536, the fix for #1390, and adds "Battery item 17 pins it either way". Also
  decision 6 and battery 17.
- **RFC said:** both builds are open questions for P0.
- **Found:** for v1.5.6 the core repository serves iceberg `890b78a9c` (2026-09-14). That is
  what `FORCE INSTALL` fetches today, and what is installed. #1378 was fixed by PR #1379,
  merged to `main` on 2026-09-11. #1390 was fixed by #1536, merged to `main` on 2026-10-10.
  Neither merge commit is an ancestor of `890b78a9c`, and neither is on the current head of
  `v1.5-variegata`. The source agrees: `CanRefreshUnlocked` still requires a non-empty
  `client_id`, and `GetVendedCredentials` still adds the table location to a nested
  credential's scope.

  Neither bug bites here. With a non-empty `CLIENT_ID`, the token refreshes (probed, unit
  header). The broadening needs two or more credentials for one table. Lakekeeper vends
  exactly one, and with one the build ignores the prefix and scopes the secret to the table
  location.
- **Because:** both fixes landed on `main` after the 1.5 line was cut, and neither was
  backported.
- **Class:** `discovery`.
- **Consequence:** two of the RFC's assumptions become hard requirements on this build.
  - `CLIENT_ID` must be non-empty. Otherwise the token never refreshes, and the instance fails
    at expiry with `Unauthorized_401`.
  - Battery 17 cannot be produced against Lakekeeper, which never returns nested credentials.
    It needs a catalog that does, or it waits for the DuckDB 2.0 re-probe.
- **Proposed row (RFC 0078):** `ASSUMED` — the routed client refuses an empty `client_id` at
  construction, as a configuration error. Battery 17 is deferred to the 2.0 bump, and the
  one-credential fact is pinned in its place
  (`test_a_vended_credential_reads_its_own_table_only_and_writes_nowhere`).

## D-031 — Corrections to this unit's own record

Appended rather than edited, per the rule that an entry is never revised. A review of the branch
raised these, and each was checked against the probes before it was written here.

- **D-025's "3600 s by default" has no source in this unit.** The 3600 s is Lakekeeper's
  default for `sts-token-validity-seconds`, from its storage-s3 docs, which RFC 0078 §3.2 also
  cites. The 900 s floor is this unit's own measurement (unit header).
- **D-026's two cases without a revocation are now pinned, not only probed.** Those are a
  namespace created after a miss, and a table created later.
  `test_a_namespace_created_after_a_miss_stays_missing_but_a_new_table_appears` covers both.
- **D-028's arithmetic was off.** 30 s × 4 attempts is 120 s, not 120.5 s. The observed
  120.5 s is about 120 s plus retry waits, and I did not break those waits down. On a plain
  `TYPE s3` secret I counted the attempts: four connections reached the blackhole listener. On
  the iceberg path I did not count them, so "four attempts of 30 s" there is inferred from the
  total.
- **D-029's describe-only row.** The test no longer asserts the "No region was provided"
  message, which depends on `AWS_REGION` being unset. With a region set, the scan carries on
  unsigned to the default AWS endpoint (D-027), so the test still unsets it. The class
  assertion, the bare `Error`, stays.

  A review proposed mapping that message to a configuration error rather than
  `dynamic_read_permission_denied`. The row stays as proposed, because D-032 shows the same
  message is what a revocation in progress looks like under OpenFGA caching. The message is
  ambiguous, though: a warehouse with no region and no STS produces it too. So the mapping must
  carry the message into the error details, where a misconfigured profile stays diagnosable.
- **Two fixture claims were weaker than written.**
  - The fixture never installed the extensions. The module passed only because the user cache
    already held them. The fixture now installs them, and the module passes against an empty
    extension directory (30.4 s, including the downloads).
  - D-024 documented the gateway addressing but nothing detected a host that cannot use it. The
    stack now probes the gateway once and skips.
- **The cost, measured.** The header said "about 30 s". With images already pulled, three runs
  measured 18.2–19.0 s of stack setup and 24.5–25.2 s for the whole module (26 tests). The
  duckdb CI shard already pulls RustFS. On a cold runner it would add these pulls, compressed:
  Lakekeeper 76 MB, OpenFGA 28 MB, Keycloak 265 MB and Postgres 195 MB, about 0.56 GB in all.
  The module stays in the duckdb shard, which runs alone on its own runner with a 30-minute
  budget.
- **The header's "24 tests" is the count before review.** The fixes above added two tests,
  the version pin and the pin for D-026's two cases, so the module holds 26. Every count
  in this entry is the module's, at 26.
- **D-027's "step 2 does close it" overstates step 2.** What step 2 closes is the signed read:
  no request carries the ambient key, so the read fails. It does not stop the request leaving.
  Unsigned, it still carries the bucket name and tenant path names to public AWS. Only the
  network egress policy or the allowlist profile D-027 proposes keeps those names in.
- **Drift count: 0.** The unit holds nine entries, not seven: D-031 and D-032 were appended
  after the header. Classes: six `discovery`, two `spec-gap`, plus this record.

## D-032 — With OpenFGA's check cache on, a revocation lands about 10 s late

- **Touches:** RFC 0078 §3.2 ("Revocation takes effect at the next statement, without
  eviction") and battery 11a. Unlisted: the authorizer's own caching.
- **RFC said:** the next statement re-loads the table with the principal's current grants.
- **Found:** that holds only while OpenFGA answers every check fresh. Under OpenFGA, Lakekeeper
  keeps no authorization cache of its own: its docs say its user-assignment and role-ancestor
  caches are never populated there. OpenFGA does cache, and Lakekeeper's production guidance
  recommends turning that on, with `OPENFGA_CHECK_QUERY_CACHE_ENABLED`,
  `OPENFGA_CHECK_ITERATOR_CACHE_ENABLED` and `OPENFGA_CACHE_CONTROLLER_ENABLED`. Probed on this
  stack, three trials each, one statement every 0.25 s:
  - With the check-query cache on, the refusal came 10.0–10.2 s after the revoke, and 38–40
    statements were served in between. The cache's default TTL is 10 s.
  - With all three of the recommended settings on, the refusal came 8.0–8.5 s after the revoke,
    and 31–33 statements were served in between. The first refusal was sometimes a
    `CatalogException`. Other times it was the bare `Error` "No region was provided via the
    vended credentials": a half-expired cache let `LoadTable` answer without credentials.

  OpenFGA ships with all three off, and the fixture now pins them off. So the battery's "next
  statement" assertion holds for the fixture, not for every deployment.
- **Because:** OpenFGA caches `Check` results for `--check-query-cache-ttl`, 10 s by default,
  and Lakekeeper's docs say it sends most checks with `MINIMIZE_LATENCY` consistency.
- **Class:** `discovery`.
- **Consequence:** in a deployment that follows Lakekeeper's advice, the check-cache TTL bounds
  revocation, not the next statement. `evict_tenant` does not help, because the stale answer
  lives in OpenFGA, not in DuckDB.
- **Proposed row (RFC 0078):** `ASSUMED` — revocation latency is the longer of "the next
  statement" and the OpenFGA check-cache TTL. The deployment contract (§3.1) names the TTL.
  Battery 11a runs with caching off, and a second leg asserts the TTL bound with caching on.

---

# Unit 6 · RFC 0078 P1 — the routed read path

Branch `feat/rfc-0078-p1-routed-reads`, stacked on `feat/rfc-0078-p0-probes`. RFC 0078 §8 P1:
`RoutedDuckDbClient` (§2), the §3.4 sequence and its attestation, `run_dynamic` (§4.2),
`DuckDbDynamicReadAdapter` (§4), the wiring (§2.5), the taxonomy (§4.5), docs, and battery items
1–10 and 12–17. Same engine as Unit 5 (`duckdb==1.5.6`, iceberg `890b78a9c`), same stack
(`tests/support/lakekeeper.py`, reused unchanged).

**Working assumptions.** The author has not answered Unit 5 yet. Every `ASSUMED` row there is
taken as the design: `oauth2_scope` required (D-024), vended secrets dropped after each statement
(D-025), `evict_tenant` after a grant or namespace change (D-026), the allowlist profile in P1
(D-027), catalog refusals mapped by message with the message in the details (D-029 and D-031), an
empty `client_id` refused and battery 17 deferred (D-030), revocation at the next statement or
the authorizer's cache TTL (D-032). D-028 is `OPEN`. P1 works under option (a): the docs state the
~120 s bound for a hung request and size `max_concurrent_queries_total` from it. The DuckDbClient
early return is the last commit on the branch, on its own, so it can be dropped (D-042). D-047
widens D-025's drop from dynamic reads to every read, D-048 withdraws D-044's idle rule, and
D-049 sets what a failed drop does.

**Drift count: 0.** Seventeen entries, D-033 to D-049: seven `discovery`, ten `spec-gap`. Most
were forced by a probe result or by a sketch that did not fit the shipped code. Five were not.
D-039, D-042, D-043 and D-045 are design choices that follow from working assumption (a) of
D-028, and D-044 set out to close a concurrency gap in D-025's row; D-048 withdraws it, because
the gap does not exist. D-043 to D-046 came out of a review of the branch, D-047 and D-048 out
of P2's run of battery 11b, and D-049 out of a review of that fix, all before the branch was
published.

**Where each battery item lives.** The real leg is
[`test_lake_routed_reads.py`](../tests/integration/test_forze_duckdb/test_lake_routed_reads.py):
74 tests on one module-scoped stack (75 with the D-042 commit), about 29 s including the stack.
It starts its own stack rather than share the P0 module's, which costs one more stack start
(about 20 s) in the duckdb shard. Items 3–10, 12–14 and 16 are classes there, named by number. Item 1 is P0's
`TestFixtureSelfTest`, imported so it runs first against this module's stack. Item 2 is in the
unit suite (`test_duckdb_dynamic_read_wiring.py`, `test_duckdb_routed_client.py`). Item 17 is
deferred (D-030). Item 15 has no test of its own. Each §4.3 cell maps to the tests that exercise
it:

- read-only: `TestReadOnly`, all three legs;
- single statement: `TestClassification`;
- timeout: `TestShell.test_a_long_statement_times_out_within_its_bound` for a CPU-bound statement.
  The hung-request bound is D-028's measurement and is not pinned, because a pin takes two minutes;
- row cap: `TestShell`, together with P0's streaming probe;
- container: `TestDisjointTenants`, `TestCrossTenantThroughTheCatalog`, `TestCrossTenantByPath`;
- untrusted refused: the unit wiring tests.

The RFC 0015 shell battery runs mock ≡ DuckDB in
[`test_duckdb_dynamic_read_conformance.py`](../tests/unit/test_forze_duckdb/test_duckdb_dynamic_read_conformance.py).
It uses one in-process `:memory:` instance per tenant, each with an attached `lake` catalog,
because the battery seeds arbitrary tenants and a Lakekeeper warehouse cannot be made per check.
DuckDB joins the manifest's `dynamic_read` engines through that leg. The DST of §4.6 is
`test_the_pool_holds_under_perturbed_schedules`. It runs `run_simulation` with a PCT scheduler
over 20 seeds and a fake engine, covering eviction under a running statement, the guarded drain,
and a rotate-and-evict race. With guarded eviction sabotaged, all 20 seeds fail.

**Confirmed, so no entry.**

- The full hardened sequence with the allowlist profile attaches, attests and serves, and
  dropping the vended secrets under `lock_configuration` works from a cursor (D-025's probe was
  on the parent connection).
- `READ_ONLY` refuses `INSERT`, `UPDATE` and `DELETE` with `InvalidInputException` before any
  request. Without it, the store refuses the data-file `PUT` (`HTTPException` 403), and the
  catalog refuses `CREATE TABLE` with the bare `Error`, whose message carries `Forbidden_403`.
- `ATTACH` of a local file, `INSTALL`, `COPY TO` a local path, `read_text('/proc/self/environ')`
  and `https://` reads are all `PermissionException` in the locked session, and no file appears.
- A wrong client secret fails at `CREATE SECRET`, not at `ATTACH`: a bare `Error` carrying
  `Unauthorized_401`, raised while the instance is built.

## D-033 — A setting `SET` on a cursor stays on that cursor, so §3.4 runs as `SET GLOBAL`

- **Touches:** RFC 0078 §1 ("Every setting that matters is `GLOBAL` scope"), §3.3, and §3.4
  steps 2 and 6. Also decision 7 and battery 14.
- **RFC said:** a plain `SET` per instance, shared by every connection of the instance.
- **Found:** that is true of the engine's own global options (`threads`, `disabled_filesystems`,
  `lock_configuration`, `allowed_directories`, `enable_external_access`). It is not true of the
  `httpfs` options. Set without `GLOBAL`, on the parent connection or on a cursor,
  `enable_global_s3_configuration`, `s3_access_key_id`, `s3_secret_access_key`,
  `s3_session_token` and `http_timeout` change only that connection. A fresh cursor still reads
  `true`, `NULL` and `30`. `DuckDbClient` runs every statement on a fresh cursor, so the sequence
  as written would leave ambient S3 configuration on for every statement it serves. P0's probes
  ran their statements on the parent connection, where the session value applies, so none of
  them could see it.
- **Because:** DuckDB scopes an extension option set with the automatic scope to the session
  that set it.
- **Class:** `discovery`.
- **Consequence:** every hardening statement is `SET GLOBAL`. The attestation reads settings
  from a fresh cursor, which is the view a statement gets. Battery 14 has two cases that drop
  `GLOBAL` from step 2, and both are refused.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.4 is issued as `SET GLOBAL`, and step 8 reads back
  from a fresh cursor. This overrides §1's sentence and §3.4's spelling, which stay as written.

## D-034 — `USE lake` fails on an attached Iceberg catalog; the route names its namespace

- **Touches:** RFC 0078 §4.2 step 1 (`USE lake`), §4.1 ("There is no `query_schema`"), §5.1, and
  decision 12.
- **RFC said:** the framework issues `USE lake`, and a plan's `gold.orders` then resolves in the
  tenant's warehouse.
- **Found:** `USE lake` raises `CatalogException: SET schema: No catalog + schema named "lake"
  found`, on a plain session and on a hardened one. `USE lake.gold` works, and after it both
  `gold.orders` and a bare `orders` resolve in the warehouse.
- **Because:** `USE <catalog>` selects the catalog's default schema, and an attached Iceberg
  catalog has none.
- **Class:** `discovery`.
- **Consequence:** `DuckDbDynamicReadConfig` gains a required `namespace`, and `run_dynamic`
  issues `USE lake.<namespace>` with each part quoted. Plans stay tenant-neutral. The namespace
  must exist in every tenant's warehouse. Where it does not, every statement on the route is
  `dynamic_read_statement_invalid`, and under D-026 the miss sticks until `evict_tenant`.
- **Proposed row (RFC 0078):** `ASSUMED` — a dynamic route names one namespace, issued as
  `USE lake.<namespace>`. This overrides §4.1's field list and §4.2 step 1, which stay as written.

## D-035 — Step 8 attests `disabled_filesystems` by effect and leaves `memory_limit` out

- **Touches:** RFC 0078 §3.4 step 8 ("`duckdb_settings()` has every value above") and battery 14.
- **RFC said:** read every value back from `duckdb_settings()`.
- **Found:** two of the values cannot be read back as set.
  - `disabled_filesystems` reads back empty from every connection while it is enforced.
  - `memory_limit` reads back reformatted: `1GB` comes back as `953.6 MiB`.

  Two smaller facts surfaced in battery 14.
  - `enable_http_metadata_cache` is off by default, so removing its step changes nothing the
    attestation can see.
  - With `allow_persistent_secrets` left on, `CREATE SECRET` reads `~/.duckdb/stored_secrets`,
    which the allowlist refuses. The session fails before it can attest anything
    (`PermissionException`, so `dynamic_read_permission_denied`).
- **Because:** DuckDB keeps the disabled set on the filesystem, not in the setting, and formats
  memory sizes for display.
- **Class:** `discovery`.
- **Consequence:** the attestation reads `read_text('/forze-attest')` and
  `read_text('http://forze-attest.invalid/')`, and requires the "… has been disabled"
  `PermissionException` for each. Neither does any I/O. `memory_limit` is not attested: it bounds
  resources, not reach. Battery 14 turns the metadata cache on rather than removing its step, and
  pins the persistent-secrets case separately. The attestation checks state, not steps.
- **Proposed row (RFC 0078):** `ASSUMED` — step 8 probes the disabled filesystems by effect and
  skips `memory_limit`. This overrides step 8's wording, which stays as written.

## D-036 — Fields and checks beyond §2.1's sketch

- **Touches:** RFC 0078 §2.1 (the `LakeTenantCatalog` and `RoutedDuckDbClient` sketches), §3.4
  (`extension_directory`, the `ATTACH` options), and D-030's "at construction".
- **RFC said:** the field lists in §2.1, an `ATTACH … URI …`, and (D-030) the empty `client_id`
  refused at construction.
- **Found:** the sketch omits what other rows require, and two spellings do not fit.
  - `LakeTenantCatalog.storage_prefix` is new and required. D-027's allowlist needs the
    tenant's prefix, and the prefix must be `s3://…/`: without the trailing slash it would also
    admit siblings that share the spelling (`duckdb_lake_catalog_invalid`).
  - `RoutedDuckDbClient.extension_directory` is new. §3.4 connects with one, and §2.1 has no
    field for it. `None` keeps DuckDB's default.
  - The `iceberg` extension's option is `ENDPOINT`, not `URI`.
  - The client is built long before any secret is read, so the empty `client_id` is refused when
    the tenant's secret is first resolved, as `duckdb_lake_client_id_empty` (configuration).
  - `http_timeout` must be a whole number of seconds, at least 1, because DuckDB takes seconds.
  - The catalog fields are captured at build alongside the secret, so they belong in the
    instance's fingerprint, as the routed base's rotation contract says. The structured base
    had no hook for facets outside the secret, so it gains `access_fingerprint_extra_parts`,
    the DSN base's hook under the same name, empty by default. A remapped catalog then
    rebuilds the instance after `evict_tenant` or a `fingerprint_ttl` refresh.
  - §4.2 puts `run_dynamic` on `DuckDbClientPort`. Adding a method to a public `Protocol`
    breaks every external implementation of it, so it lives on a sub-protocol,
    `DuckDbDynamicReadClientPort`, which the adapter requires.
- **Because:** the sketch predates D-024, D-027 and D-030, and was not checked against the
  extension's option names or the shipped base and port.
- **Class:** `spec-gap`.
- **Consequence:** none beyond the fields. `guarded` stays a field, as sketched, and `False` is
  refused with `duckdb_routed_client_unguarded`.
- **Proposed row (RFC 0078):** `ASSUMED` — the catalog carries `storage_prefix`, the client
  carries `extension_directory`, `ATTACH` uses `ENDPOINT`, the fingerprint covers the catalog,
  and `run_dynamic` is on a sub-protocol.

## D-037 — `duckdb_routed_client_static_credentials` has nothing to inspect

- **Touches:** RFC 0078 §2.1 ("A construction that smuggles them in through `engine` fails with
  `duckdb_routed_client_static_credentials`"), decision 6, and battery 2.
- **RFC said:** `engine` is a path static credentials could take, and construction refuses it.
- **Found:** `engine` is a `DuckDbConfig`: threads, memory limit, executor size, read-only. It
  carries no credential, statement or connect option. The connect options are an `initialize`
  argument that the routed client builds itself.
- **Because:** the sentence anticipated a config shape the shipped `DuckDbConfig` does not have.
- **Class:** `spec-gap`.
- **Consequence:** the code is not minted. The refusal is structural: passing `object_stores`,
  `sources`, `bootstrap_sql` or raw secret statements raises `TypeError`, and a unit test pins
  it. One path the RFC did not list stays open. `lock_configuration` does not cover secret DDL
  (D-025), so a statement could `CREATE SECRET` or `DROP SECRET lake_cat`. Only classification
  refuses those, and battery 6 pins both.
- **Proposed row (RFC 0078):** `ASSUMED` — the routed client has no static-credential surface,
  by construction, and no error code for one.

## D-038 — Taxonomy rows §4.5 and D-029 did not cover

- **Touches:** RFC 0078 §4.5, D-029's proposed row, and battery 16.
- **RFC said:** the class table in §4.5, plus D-029's message rows for the bare `duckdb.Error`.
- **Found:** the battery provokes refusals neither one maps.
  - `InvalidInputException` covers three different things. "attached in read-only mode" is a
    write, and the lock's "configuration has been locked" and "Displaying unredacted secrets is
    disabled" are confinement. A missing or excess `$parameter` is the caller's statement.
  - A revoked grant reaches the statement as `CatalogException` (`schema "gold" does not
    exist`), which D-029's table showed and its row did not map.
  - `ConversionException` quotes the value that failed, which can come out of a column.
  - The client's own timeout is an `infrastructure` error, which the adapter could not tell
    apart from any other.
  - A comment-only string holds zero statements.
  - A result with two columns of the same name (`SELECT a.id, b.id`) collapses into one key
    when a row becomes a mapping, and the first value is lost without a word.
  - A broken image fails while the instance is built. A missing `LOAD iceberg` or `LOAD httpfs`
    raises `InvalidInputException` or `CatalogException`, and a missing catalog secret raises
    the bare `Error`. Mapped like a statement's failure, the first two would blame the caller's
    statement for a deployment fault. The same is true of D-029's build-time refusals: an
    ungranted principal's `NotFound_404` and a wrong secret's `Unauthorized_401` arrive while
    §3.4 runs, before any statement.
- **Because:** these surfaced only once the adapter ran against the stack.
- **Class:** `spec-gap`.
- **Consequence:** the mapping follows these rules.
  - The read-only message becomes `dynamic_read_write_refused`. The two lock messages become
    `dynamic_read_permission_denied`. Any other `InvalidInputException` becomes
    `dynamic_read_statement_invalid`.
  - A revocation is `dynamic_read_statement_invalid`, indistinguishable from a typo. It is still
    never rows.
  - A conversion failure reports the class name only, as Postgres does for SQLSTATE class 22.
  - The client's timeout carries the code `duckdb_query_timeout`. That is a user-visible change
    to shipped code, recorded under `Changed`.
  - Zero statements is `dynamic_read_multi_statement`, by §4.2's "`len != 1`".
  - Duplicate column names are refused as `dynamic_read_statement_invalid`, naming them.
  - Anything a step of §3.4 raises before the attestation becomes
    `duckdb_session_setup_failed` (configuration), with the engine's message in the details,
    and the instance is closed. That covers the missing extensions and secret, and D-029's
    `NotFound_404` and `Unauthorized_401` at build time. It overrides D-029's row for
    refusals raised while an instance is built, which stays as written. The same refusals
    raised by a statement still map as D-029 says. Battery 14 and 16 pin it. Nothing is served
    either way.
  - Anything unmatched goes through `fallback_exception_mapper("DuckDB")` to `infrastructure`.
- **Proposed row (RFC 0078):** `ASSUMED` — §4.5 gains the rows above, and battery 16 pins them.

## D-039 — The process-wide semaphore is held by the worker, not by the caller's scope

- **Touches:** RFC 0078 §2.2 ("an `asyncio.Semaphore(max_concurrent_queries_total)` around every
  `client_scope` use") and decision 4.
- **RFC said:** the routed client wraps each scope in the semaphore.
- **Found:** under D-028 option (a) the caller returns at the statement timeout while the worker
  keeps running. A semaphore released with the caller's scope would let new statements start
  while hung workers still hold threads and memory.
- **Because:** the semaphore exists to bound the work, and the caller's lifetime no longer
  bounds the work.
- **Class:** `spec-gap`.
- **Consequence:** `DuckDbClient.initialize(admission=…)` takes a shared semaphore. A statement
  takes a slot before it is submitted with `executor.submit`, and the concurrent future's own
  done callback gives it back through `loop.call_soon_threadsafe`. So the slot is held until the
  thread returns, whether the caller waits, times out or is cancelled. A cancelled caller
  cancels only asyncio's wrapper, which settles before the thread does. A statement cancelled
  while still queued gives its slot back at once, since it never runs. The routed client passes
  one semaphore to every instance, so instance creation counts against the bound too.
- **Proposed row (RFC 0078):** `ASSUMED` — the bound is held by the statement's worker. This
  overrides §2.2's placement, which stays as written.

## D-040 — After the D-025 drop, a by-path read inside the tenant's prefix goes to public AWS

- **Touches:** RFC 0078 §3.3, §3.5's "no cross-tenant read by object path" row, D-025 and D-027.
- **RFC said:** nothing about own-prefix paths once the vended secrets are gone.
- **Found:** with the secrets dropped after each statement, a statement that names an `s3://`
  path inside the tenant's own table matches no secret. The allowlist admits the prefix. With
  global S3 configuration off the endpoint is ignored, so the read goes unsigned to
  `https://lake.s3.amazonaws.com/…`, which answered 403.
- **Because:** D-027's mechanism, reached from the tenant's own prefix instead of another's.
- **Class:** `discovery`.
- **Consequence:** this is not a cross-tenant read, and a trusted compiler never emits paths.
  But the bucket name and the tenant's own path names reach the public internet, and only the
  network egress policy stops that. The DuckDB docs page lists it under the deployment's
  controls. No code changed.
- **Proposed row (RFC 0078):** `ASSUMED` — the deployment contract (§3.1) requires an egress
  policy that admits only the catalog, the IdP and the store.

## D-041 — Battery items adapted to the P0 findings

- **Touches:** RFC 0078 §7 items 4, 5, 7, 10 and 12.
- **RFC said:** the items as written.
- **Found:** five items could not run as written on this stack.
  - Item 4 says A "names B's namespace". Both warehouses hold `gold`, so a namespace name
    cannot point at B. The test names a catalog and a namespace that do not resolve, and
    attaches B's warehouse with classification stubbed (`NotFound_404`).
  - Item 5's listener variant becomes DuckDB's own HTTP log: zero requests. The allowlist
    refuses B's paths before any I/O, so no request could be signed with the ambient key.
  - Item 7's second leg removes the allowlist pair as well as step 5, since D-027 made the
    allowlist a second refusal. The process control stands in as a directory this process
    cannot write. The test skips as root.
  - Item 10's stubbed leg meets the `READ_ONLY` attach first, which P0 adopted. A third leg
    removes `READ_ONLY` too. The store refuses `INSERT`, `UPDATE` and `DELETE`, the catalog
    refuses `CREATE TABLE`, and CTAS stops at the disabled local filesystem.
  - Item 12's timeout bound holds for a CPU-bound statement. A hung request is D-028's case.
- **Because:** D-027, D-028 and P0's `READ_ONLY` finding changed what each layer refuses first.
- **Class:** `spec-gap`.
- **Consequence:** each item still removes one layer and asserts the next one refuses.
- **Proposed row (RFC 0078):** `ASSUMED` — §7 items 4, 5, 7, 10 and 12 read as adapted above.

## D-042 — `DuckDbClient` returns the timeout without waiting for the worker

- **Touches:** RFC 0078 §4.3's timeout cell ("the executor thread is held until DuckDB unwinds"),
  D-028 option (a), and battery 12.
- **RFC said:** the caller learns of the timeout when the worker returns, about 120 s late for a
  hung request (D-028).
- **Found:** nothing new. This is the change D-028's consequence asked about. On timeout,
  `__submit` interrupts the cursor and raises at once. The worker keeps its executor thread and
  its admission slot (D-039) until DuckDB unwinds, and the late result is consumed so asyncio
  does not report it.
- **Because:** working assumption (a). The author has not chosen.
- **Class:** `spec-gap`.
- **Consequence:** the caller sees `dynamic_read_timeout` at the statement timeout. The test,
  `test_duckdb_client_hung_request.py`, stands a local socket that never answers in for a hung
  catalog: a plain `http://` read the interrupt cannot cut short. It sits with the integration
  tests because it needs `httpfs`, not Docker. `DuckDbClient.close()` still waits for the
  worker; the routed client runs that close in the background (D-043). This is the branch's last
  commit, with its own test and changelog line, so it can be dropped alone.
- **Proposed row (RFC 0078):** `OPEN` — keep the early return if D-028 resolves to (a), and
  drop the commit otherwise.

## D-043 — An evicted tenant instance closes in the background

- **Touches:** RFC 0078 §2.4 ("closes after its last in-flight statement. Close is the existing
  `DuckDbClient.close`") and decision 4.
- **RFC said:** the guarded drain closes the instance with `DuckDbClient.close`.
- **Found:** the pool awaits that close in the caller's path. The last scope to leave a
  draining instance awaits it, and so does an overflow eviction inside another tenant's first
  `client_scope`. `DuckDbClient.close` waits for every worker, and a worker stuck on a hung
  request takes about 120 s (D-028). So a timed-out caller, or a cancelled one, waits after all
  when it leaves the scope, and an unrelated tenant's first call stalls behind the eviction.
- **Because:** the shared pool treats dispose as cheap, and DuckDB's close is not cheap while a
  worker is stuck.
- **Class:** `spec-gap`.
- **Consequence:** `DuckDbClient.close` keeps waiting, which is the analytics contract. The
  routed base gains a `_dispose_client` hook whose default is the old close, so every other
  integration is unchanged. `RoutedDuckDbClient` overrides it to start the close as a tracked
  background task, and `RoutedDuckDbClient.close` waits for every outstanding teardown. A close
  that fails is logged. Each hung teardown parks one thread of the loop's default executor,
  because `DuckDbClient.close` shuts its executor down through `asyncio.to_thread`. A burst of
  evictions of hung instances can therefore exhaust that executor.
- **Proposed row (RFC 0078):** `ASSUMED` — the routed client disposes an instance in the
  background and drains the teardowns on close.

## D-044 — Vended secrets are dropped only when no statement is running on the instance

- **Touches:** D-025's proposed row ("after every statement, `run_dynamic` drops the vended
  `__internal_ic_%` secrets") and §2.2's "max_concurrent_queries" per instance.
- **RFC said:** D-025 drops after every statement.
- **Found:** an instance runs up to `engine.max_concurrent_queries` statements at once. Dropping
  every `__internal_ic_` secret when one statement ends can drop the secret another statement
  was just vended, while that statement is still reading with it.
- **Because:** the drop matches by prefix, not by the statement that was vended the secret, and
  DuckDB exposes no link between the two.
- **Class:** `spec-gap`.
- **Consequence:** each instance counts its running dynamic statements under a lock. A statement
  increments the count before it opens its cursor. It decrements the count when it ends, and
  drops the secrets if the count reaches zero, still under the lock, so no statement can start
  between the two. Under sustained concurrent load the instance may never be idle. The by-path
  revocation window of D-025 then extends to the next idle moment, still bounded by the vended
  credential's STS validity.
- **Proposed row (RFC 0078):** `ASSUMED` — vended secrets are dropped when the instance's last
  running statement ends. This refines D-025's row, which stays as written.

## D-045 — One deadline covers the wait for a slot and the statement

- **Touches:** RFC 0078 §2.2 (the process-wide semaphore), §4.3's timeout cell, and decision 10.
- **RFC said:** the timeout is the statement's, enforced by interrupt.
- **Found:** a statement waited for its admission slot before its timeout started. With every
  slot held by hung workers (D-039, D-028), a caller could wait without bound for a slot.
- **Because:** the semaphore was added in front of an executor whose timeout covered only the
  work.
- **Class:** `spec-gap`.
- **Consequence:** `__submit` computes one deadline from the statement's timeout. The wait for a
  slot runs under `asyncio.timeout_at` of that deadline, and running out raises the same
  `duckdb_query_timeout`, so the adapter reports `dynamic_read_timeout`. The statement gets
  whatever is left. A statement without a timeout still waits as long as it takes.
- **Proposed row (RFC 0078):** `ASSUMED` — a statement's timeout covers its wait for admission.

## D-046 — A timed-out statement still queued was never stopped, and one interrupt is not enough

- **Touches:** RFC 0078 §4.3's timeout cell ("client-side `cursor.interrupt()`, cooperative") and
  battery 12.
- **RFC said:** the client interrupts the statement's cursor when the timeout fires.
- **Found:** two gaps.
  - A statement still queued in the instance's executor has no cursor when its timeout fires.
    It was not interrupted, and it ran later without a bound while holding a slot.
  - DuckDB drops an interrupt that arrives before its statement starts. A probe interrupted a
    fresh cursor and then ran a 10 s query on it, and the query ran to the end. So a worker that
    has published its cursor but not yet begun executing misses a single interrupt.
- **Because:** the first is how `run_in_executor` works. The second is DuckDB's: each query
  clears the interrupt flag when it starts.
- **Class:** `discovery`.
- **Consequence:** on timeout the client cancels the executor future, which dequeues a statement
  that never started. It sets an `expired` flag before reading the cursor, and each worker
  checks that flag right after publishing its cursor, raising before it executes. Under the GIL
  either the caller sees the cursor or the worker sees the flag. The caller then interrupts
  again every 100 ms until the worker returns, which closes the window between the worker's
  check and the start of its query. A test holds the worker in that window for 300 ms and
  checks that the statement still stops.
- **Proposed row (RFC 0078):** `ASSUMED` — a timeout dequeues a queued statement and repeats
  the interrupt until the worker returns.

## D-047 — An analytics read kept its vended secret, and once expired it broke the table

- **Touches:** D-025's proposed row ("after every statement, `run_dynamic` drops the vended
  `__internal_ic_%` secrets"), RFC 0078 §2.5's "Named-query analytics routes keep working on the
  routed client", §3.2, and battery 11b, which P2 runs (Unit 7).
- **RFC said:** every statement starts from a fresh `LoadTable`, so an expired credential is
  never reused. D-025 added the drop to the dynamic path only.
- **Found:** P2's first real run of battery 11b, before this branch was published, found it.
  An analytics read on the routed client (`run_query`) kept the secret it was vended,
  as D-025 describes for any committed statement. After that credential expired, the next
  analytics read of the same table on the same instance loaded the table again and was vended a
  fresh secret, then signed its store requests with the old one. It failed with
  `HTTPException` "HTTP 400 Bad Request … InvalidRequest: check key failed", and so did every
  read after it. Only the old secret was left afterwards: the failed statement rolled back and
  took the fresh one with it. A dynamic read on the same instance succeeded, because its drop
  had left no old secret to choose. A plain session that kept its secret did not fail with the
  external file cache on, probably because its read never reached the store. With the cache
  off, it fails the same way (11b's kept-secret leg, Unit 7).
- **Because:** the engine picks between two secrets of the same scope without regard to
  expiry, and here it picked the older one. duckdb-iceberg#1352 reports the same symptom.
- **Class:** `discovery`. It shows only after a credential expires, 900 s at the store's floor.
- **Consequence:** with the routed client as first built, an instance's analytics reads of a
  table stopped working one credential lifetime after the first of them, until `evict_tenant`.
  The fix is the drop D-025 proposed, on every read: `DuckDbClient.run_query` now drops the
  vended secrets after each statement, as `run_dynamic` does. Only an instance initialized with
  `drop_vended_secrets=True` drops them, and the routed client is what sets it. A plain
  `DuckDbClient` reads exactly as before: no extra cursor, no `duckdb_secrets()` scan, no lock.
  `run_command` is not a read path and is left alone. Pinned here by
  `test_an_analytics_read_keeps_no_vended_secret_either` in the battery, and by
  `test_an_analytics_read_drops_the_vended_secrets_too` and `test_a_plain_client_never_drops` in
  `test_duckdb_run_dynamic.py`. The expiry itself takes 15 minutes to show, so it is pinned in
  P2's `lake_slow` module.
- **Proposed row (RFC 0078):** `ASSUMED` — every read statement on a routed instance, dynamic
  or analytics, drops the vended secrets when it ends. This widens D-025's row, which stays as
  written.

## D-048 — A running statement's vended secret is invisible to another cursor; D-044's rule is withdrawn

- **Touches:** D-044 ("Dropping every `__internal_ic_` secret when one statement ends can drop
  the secret another statement was just vended") and its proposed row.
- **RFC said:** nothing. D-044 reasoned the race from the drop matching by prefix, and drops only
  when no dynamic statement is running.
- **Found:** a secret vended inside a statement is part of that statement's transaction. Until the
  statement ends, `duckdb_secrets()` on any other cursor does not list it, so a drop issued
  elsewhere cannot see it. Probed with a real `LoadTable` (another cursor counts no new
  `__internal_ic_` secret while the stream is open, and one after it is exhausted) and with a
  plain `BEGIN; CREATE TEMPORARY SECRET` (the drop leaves it, and it is there after `COMMIT`).
  Thirty-one dynamic statements dropping secrets while an analytics scan of 24 files ran left
  the scan intact. Two drops of the same secret from two connections do conflict: the second
  raises `TransactionException: Catalog write-write conflict`.
- **Because:** DuckDB's secret catalog is transactional, like the rest of its catalog.
- **Class:** `discovery`. It corrects D-044's premise, which was not probed.
- **Consequence:** the race D-044 guards against cannot happen. Its rule has a cost D-047
  exposes. Under sustained concurrent load the instance is never idle, so committed secrets
  pile up, and once the oldest expires the next statement on that table fails as in D-047. The
  counter is gone. Each read drops the committed vended secrets when it ends, and a per-instance
  lock serializes the drops, since two would conflict.

  The design now rests on two engine assumptions, and each has a tripwire.
  - A statement's vended secret stays uncommitted until the statement ends. The drop's
    docstring states it. P0's `test_a_running_statements_secret_is_invisible_to_another_cursor`
    fails first on an engine that lists a running statement's secret to other cursors.
  - Two drops of one secret cannot both succeed. This is why the lock exists. The `lock_removed`
    leg of `test_concurrent_drops_conflict_unless_serialized` fails first on an engine where
    they can, and the lock would then be unnecessary rather than wrong.

  Four more tests pin the rest:
  - `test_a_scan_keeps_reading_while_another_statements_drop_runs`, in the P0 module. A paused
    scan of an 8-file table keeps reading after the drop has taken another statement's
    leftover, and its own secret appears once it ends.
  - `test_duckdb_hides_a_secret_created_under_begin_until_commit`, the same visibility rule on
    a plain transaction.
  - `test_a_committed_leftover_is_dropped_while_another_statement_runs`. The drop does not wait
    for idle. Its running statement reads no table, so the scan test above is what shows a
    reader is not disturbed.
  - `test_concurrent_drops_conflict_unless_serialized`. Two reads drop the same leftover, forced
    to interleave by a barrier. With the lock, one waits out the barrier's timeout alone and the
    other finds nothing left to drop. Without it, one drop fails. That is its sabotage leg.
- **Proposed row (RFC 0078):** `ASSUMED` — the drop runs after every read statement, serialized
  per instance. This withdraws D-044's row, which stays as written.

## D-049 — A failed vended-secret drop retires the instance and never masks the read

- **Touches:** D-047 and D-048 (the drop after every read), §2.4 (lifecycle and eviction), and
  §4.5 (taxonomy).
- **RFC said:** nothing. The drop is not in the RFC. D-025 proposed it, and D-047 and D-048 put
  it on every read without saying what a failure does.
- **Found:** a review of D-047's fix found the drop in the read's `finally`. A drop that raised
  replaced the read's outcome. A failed read surfaced as the drop's error, and a good read
  lost its rows. Either way the leftover stayed, and under D-047 it fails every later read of
  its table once it expires.
- **Because:** the drop was written as cleanup, but its failure leaves state behind that breaks
  the instance later.
- **Class:** `spec-gap`.
- **Consequence:** a failed drop never raises over the read. The read's rows or its error come
  back, the failure is logged at warning, and the client is retired. A read that failed retires
  the client as well, because the leftover stays either way.

  A retired client refuses, with `duckdb_instance_retired` (infrastructure), every statement
  submitted after the retire, including one that was waiting for an admission slot: the check
  runs again once the slot is held, and the slot goes back. A statement already running
  finishes. The health check reports the refusal.

  `RoutedDuckDbClient.client_scope` evicts a retired instance at two points, each time only
  while the pool still holds that instance, so a scope that held it late spares its healthy
  successor.
  - When the scope that saw the retire ends.
  - When a scope starts. A timed-out read leaves its scope while its worker still runs (D-042),
    and the worker's failed drop can retire the instance after that scope has gone. The next
    scope for the tenant evicts it and builds a fresh one before yielding, so `health()` too
    rebuilds and then probes.

  Until that next call, or an LRU eviction, a retired instance lingers in the pool. It costs
  memory and nothing else. A retire that lands between the entry check and a statement is
  refused by that statement's own check, and evicted when the scope ends. Pinned by
  `test_a_failed_drop_after_a_good_read_returns_the_rows_and_retires_the_client`,
  `test_a_failed_drop_after_a_failed_read_raises_the_reads_error`,
  `test_a_statement_waiting_for_a_slot_is_refused_once_the_client_retires`,
  `test_a_retired_instance_is_evicted_and_its_successor_spared`, and
  `test_a_timed_out_read_still_drops` in the unit suite. The battery adds
  `test_a_failed_drop_retires_the_instance_and_the_next_call_rebuilds`, a real `DuckDbClient`
  under the routed client. With D-042, the battery's
  `test_a_late_worker_retire_is_evicted_by_the_next_call` pins the entry check. That test
  travels in D-042's commit, because only D-042 lets a worker outlive its scope. Each test was
  checked against its fix removed.
- **Proposed row (RFC 0078):** `ASSUMED` — a failed drop is logged, never raised over the read,
  and retires the instance, which the routed client evicts and rebuilds.

## Review findings — 2026-10-27

PR #685, after it was published. Five findings from CodeRabbit and CodeAnt, all fixed, plus
the Codecov patch check. None changes a proposed row. R-5 amends what D-034's consequence says
about quoting, and that entry stays as written.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | Medium | `provenance` is a `Literal`, which attrs does not enforce, and the module refused only the exact `"untrusted"`. A misspelt tier wired as trusted (CodeRabbit, CodeAnt) | Fixed — the config refuses a value outside the closed set (`duckdb_dynamic_read_provenance_invalid`), and the module admits only the exact `"trusted"` |
| R-2 | Medium | A caller cancelled mid-statement, by a disconnect or an outer `asyncio.timeout`, did not interrupt the statement or retrieve its late result. The worker ran past `statement_timeout` on its thread and a process-wide admission slot (CodeRabbit). The released `DuckDbClient` had the same gap, without the slot | Fixed — a cancelled caller takes D-042's path: flag, cancel, interrupt until the worker returns, retrieve its result. The cancellation is re-raised unchanged, and the slot still goes back from the worker |
| R-3 | Medium | `duckdb_session_setup_failed` carried the engine's text and chained its error. Setup writes the client secret into `CREATE SECRET`, and a DuckDB parser error quotes a window of the statement, cut at either end. The secret could reach the details and the server-error log, whole or in part (CodeRabbit) | Fixed — the text loses DuckDB's quote of the statement and both spellings of the secret, and the error is not chained |
| R-4 | Low | A retired client stayed retired through `close()` and `initialize()`, and refused every statement (CodeAnt). `initialize` reopens a closed client, though the routed client never does | Fixed — `initialize` clears the flag when it opens a connection. `close` has drained the old connection's workers by then |
| R-5 | Medium | `run_dynamic` split `use` on every dot, so `namespace="gold.sub"` issued `USE "lake"."gold"."sub"`, a parser error (CodeAnt) | Fixed — the catalog ends at the first dot and the rest is one schema. On the stack, DuckDB lists the nested Iceberg namespace `gold.sub` as one schema of that name. `USE "lake"."gold.sub"` resolves a bare table name; a two-part `sub.deep` does not |
| R-6 | — | Patch coverage: 14 lines and 2 branches of `routed_client.py`, 7 lines and 2 branches of `client.py`, with the unit and integration legs combined (Codecov) | Unit tests over fakes now cover them, except one branch: a submit that fails with no admission semaphore, which needs the executor shut down under a call |

Each fix was checked against its own removal. R-5 has a battery test as well,
`test_a_nested_namespace_is_one_schema_to_use`, which brings the battery to 76 tests.

---

# Unit 7 · RFC 0078 P2 — tightening

Branch `feat/rfc-0078-p2-tightening`, after P1 (#685).
RFC 0078 §8 P2 names three things. Same engine as Units 5 and 6 (`duckdb==1.5.6`, iceberg
`890b78a9c`), same stack, which gains two knobs: the tenant principals' token lifespan and the
warehouses' `sts-token-validity-seconds`.

- **The allowlist profile.** Nothing to build. P1 adopted it (D-027, D-036): the routed client
  sets `allowed_directories` to the tenant's prefix and then `enable_external_access=false`,
  both before `CREATE SECRET`, and the attestation reads both back. Re-checked here, in place.
- **Battery 11b and 11c, scheduled.** Built, run for real, and scheduled (D-051, D-052). The
  first real run found a defect in P1's analytics path and a false premise in D-044. P1 was
  not yet published, so the fix and both entries went there: D-047 and D-048, at the end of
  Unit 6, with D-049 from a review of that fix. This unit starts at D-050.
- **The DuckDB 2.0 re-probe.** Run against `duckdb==2.0.0` without touching the lock or
  `pyproject.toml` (D-053 to D-060). 2.0 is not adopted. Adopting it is a separate change, and
  as things stand it cannot happen by a lock bump alone (D-054, D-055).

P3 (compiled read) stays out of scope: it waits on RFC 0038, which is parked.

**Working assumptions.** The author has not answered Units 5 and 6. Every `ASSUMED` row there is
taken as the design, as Unit 6 did, D-047 to D-049 included. D-028 stays `OPEN` and is worked
under option (a), and D-042's `OPEN` row is kept as it stands. D-054 is decided: the extra is
capped below 2, on a branch of its own that goes to `main` first.

**Drift count: 0.** Eleven entries, D-050 to D-060: nine `discovery`, two `spec-gap`. None
changes product code. Units 5 and 6 are not edited here.

**Where each item lives.**

- 11b and 11c:
  [`test_lake_credential_expiry.py`](../tests/integration/test_forze_duckdb/test_lake_credential_expiry.py),
  marked `lake_slow`. `just test` deselects it, `just lake-slow` runs it, and
  [`lake-expiry.yml`](../.github/workflows/lake-expiry.yml) runs that recipe daily at
  05:30 UTC. One stack, one scenario per item, each test asserting one outcome.
- D-047's regression on the real leg: the 11b test
  `test_routed_reads_after_expiry_load_afresh_on_the_same_instance`, and its mechanism in
  `test_a_kept_secret_outranks_the_fresh_one_after_expiry`.
- The 2.0 re-probe: one-off scripts and the existing suites run on an overlay environment
  (`uv run --with duckdb==2.0.0`), recorded here only. Nothing pins 2.0 behaviour, because the
  repository does not run 2.0.

**11b and 11c, measured.** One run on 1.5.6 for the record: 6 passed in 1018.7 s, about 17
minutes. The 11c scenario took 101.6 s, which includes the stack start and seeding the 24-file
table. The 11b scenario took 914.2 s, almost all of it the wait for the 900 s credential. Every
leg came out as pinned. Before the D-047 fix, the same scenario as a scratch probe had the
analytics read after expiry fail. On 2.0, through the shim, every leg comes out the same except
the kept-secret one, which cannot arise because nothing is kept (D-053).

**Confirmed, so no entry.**

- The store enforces the expiry of a vended credential. The credential vended at t0, used
  directly with boto3, reads at t0 and gets `400 InvalidRequest: check key failed` from RustFS
  after expiry. Without this control, every read after expiry would prove nothing.
- Lakekeeper asked the store for a fresh credential on each `LoadTable`. Two loads two seconds
  apart returned different access keys, each with its full 900 s, so no cache handed out an
  older one.
- With the routed client's drop, a read after expiry starts from a fresh `LoadTable` and
  succeeds on the same instance, for dynamic and analytics reads alike (11b).
- The catalog token refreshes without a rebuild (11c). With 1 s tokens and a 75 s wait, a token
  fetched at the start is refused (`401`), and the instance built at the start reads again.
- §3.2 and the RFC's docs line hold on 1.5.6: the engine does not re-vend mid-statement. A
  statement held open across expiry makes one more store request, gets HTTP 400, and fails. It
  sends no new `LoadTable` (D-051).

## D-050 — The shortest catalog-token life the chain enforces is about 61 s

- **Touches:** RFC 0078 §7 item 11c ("Catalog token expiry → auto-refresh") and §3.2's catalog
  token bullet.
- **RFC said:** nothing about how short a token can be made, or when it stops working.
- **Found:** Keycloak issues a 1 s access token when a client's `access.token.lifespan` is 1, and
  takes the value without complaint. Lakekeeper keeps accepting that token for 60 s past its
  `exp`. Probed at 5 s steps: `200` up to 59.9 s past `exp`, `401` at 64.9 s, logged as
  "Token rejected: token has expired".
- **Because:** Lakekeeper validates the token with a 60 s leeway, the default of common JWT
  libraries.
- **Class:** `discovery`.
- **Consequence:** 11c waits the lifespan plus 60 s plus a margin, 75 s in all, and checks that a
  token fetched at the start is refused before it reads again. A principal disabled at the IdP
  keeps catalog access for its token lifespan plus 60 s. Revoking its grant is what cuts it at
  the next statement, within the authorizer's cache TTL (D-032).
- **Proposed row (RFC 0078):** `ASSUMED` — the deployment contract (§3.1) states the 60 s leeway
  next to the token lifespan, and points at grant revocation for an immediate cut.

## D-051 — What battery 11b exercises once every read drops its secrets

- **Touches:** RFC 0078 §7 item 11b, §3.2's mid-statement bullet, decision 14, D-025 and D-047.
- **RFC said:** "A statement after expiry succeeds via a fresh `LoadTable`. A statement spanning
  expiry either succeeds or fails loud, and the result is pinned as documented behavior."
- **Found:** two things changed what each half can mean.
  - With the drop, a routed read never finds an old secret, so "after expiry succeeds" holds by
    construction. What the leg still proves is that the store enforces expiry, that the next
    `LoadTable` vends a working credential, and that the instance needs no rebuild. It also pins
    D-047's regression: the analytics read runs last at t0 and first after expiry.
  - No forze path can hold a statement across expiry. `run_dynamic` fetches its probe and closes
    the cursor, `run_query_streamed` materializes the result before it yields, and decision 14
    sizes the credential at ten times the longest timeout. So the spanning half runs on a plain
    session (§3.4 steps 1 to 4) with a streaming scan of a 24-file table, paused after one row.
    On 1.5.6 the next fetch makes one store request, gets HTTP 400, and fails with
    `InvalidInputException` naming the HTTP error. No `LoadTable` goes out. That is "fails loud".
  - A third leg, also on a plain session with the file cache off, keeps its first secret and
    reads after expiry. It fails with `HTTPException` 400: D-047's mechanism, pinned so the
    reason for the drop is visible.
- **Because:** D-025 and D-047 removed the case the RFC wrote the item for, and decision 14
  already forbids the statement the second half describes.
- **Class:** `spec-gap`.
- **Consequence:** the module asserts six outcomes: two for 11c and four for 11b. The spanning
  and kept-secret legs pin engine behaviour, so a bump that adds a mid-statement re-vend
  (duckdb-iceberg#1352) fails them first. 2.0 does not add one, on this stack (D-053). RustFS
  answers an expired key with `400 InvalidRequest: check key failed`. AWS answers with
  `400 ExpiredToken`, and whether the extension's re-vend keys on that code was not tested.
- **Proposed row (RFC 0078):** `ASSUMED` — §7 item 11b reads as built above. This overrides the
  item's wording, which stays as written.

## D-052 — 11b and 11c run in their own scheduled workflow, under a marker of their own

- **Touches:** RFC 0078 §7 item 11 ("(b) and (c) are slow and run nightly") and §8's P2 bullet
  ("Battery 11b/c in the nightly matrix").
- **RFC said:** in the nightly matrix.
- **Built:** a `lake_slow` marker, registered in `pyproject.toml` and excluded by `just test`
  next to `perf` and `fuzz`. A `just lake-slow` recipe runs it. A new
  [`lake-expiry.yml`](../.github/workflows/lake-expiry.yml), daily at 05:30 UTC and on
  `workflow_dispatch`, runs that recipe with a 40-minute limit and the pinned actions of
  `ci.yml`.
- **Because:** `nightly.yml` is the DST matrix. A red nightly means "the simulator found an
  interleaving", and D-006 already kept a different failure out of it for that reason. The CI
  shards run `just test`, so excluding the marker there keeps the shards' 30-minute budget.
- **Class:** `spec-gap`.
- **Consequence:** one more workflow file, and about 20 runner-minutes a day. A cold runner also
  pulls the stack's images, about 0.56 GB compressed (D-031). The module sets its own 30-minute
  test timeout over `pyproject.toml`'s 300 s.
- **Proposed row (RFC 0078):** `ASSUMED` — battery 11b and 11c run as `lake_slow` in
  `lake-expiry.yml`. This overrides §8's "in the nightly matrix", which stays as written.

## D-053 — The DuckDB 2.0.0 re-probe: method, builds, and the outcome per row

- **Touches:** RFC 0078's fact-tag note ("every probe re-runs on a bump"), §6's last non-goal,
  §8's P2 bullet, and every Unit 5 and 6 entry that names the engine.
- **RFC said:** re-probe on DuckDB 2.0.
- **Found:** `duckdb==2.0.0` (`v2.0.0`, `d54cbbe3df`) ran through an overlay environment,
  `uv run --with duckdb==2.0.0`, which shadows the locked 1.5.6 and leaves the lock alone. The
  core repository serves its extensions, and `INSTALL` then `LOAD` work with autoinstall off:
  iceberg `4c0c0874c4`, httpfs `77392a7207`, and avro `345c3f4e82`, which iceberg pulls in.
  The P0 module, the P1 battery and the duckdb unit suite ran unmodified first. They ran again
  with a throwaway shim that prototypes adoption (D-054, D-055), so that the battery could get
  past session setup.

  | Row | On 2.0 |
  |---|---|
  | D-025 vended secrets outlive their statement | **changed**: discarded when it ends (D-056) |
  | D-026 a missing namespace sticks | unchanged |
  | D-027 an unmatched `s3://` path goes unsigned to public AWS | unchanged. Without step 2 the ambient key still signs; DuckDB's HTTP log no longer shows it (D-060) |
  | D-027 the allowlist profile, §9 q1 | **changed**: the catalog's requests pass the external-access check (D-055) |
  | D-028 nothing bounds a hung request | **changed**: `http_timeout` bounds it. Interrupt still does not (D-057) |
  | D-029 catalog refusals as the bare `duckdb.Error` | partly changed: a `CREATE TABLE` 403 is now an `HTTPException` (D-060). `ATTACH` 404 and 401, describe-only and revocation are unchanged |
  | D-030 neither credential fix is in the build | **changed**: both are (D-059) |
  | D-033 a `SET` on a cursor stays on it | unchanged |
  | D-034 `USE lake` fails | unchanged. `USE lake.gold` works |
  | D-035 `disabled_filesystems` reads back empty, `memory_limit` reformatted | unchanged |
  | D-046 an interrupt before `execute` is dropped | unchanged |
  | §3.2 one `LoadTable` per statement | unchanged |
  | §4.2 classification | partly changed: `PRAGMA version` and `PIVOT` (D-060) |
  | §4.3 `fetchmany` streams | unchanged |
  | 11b and 11c | unchanged: routed reads after expiry succeed, and a statement held open across expiry still fails with HTTP 400 and no new `LoadTable` (D-051) |
  | D-047 a kept secret outranks a fresh one | not reachable: nothing is kept (D-056) |
  | D-048 a running statement's secret is invisible elsewhere | not reachable, for the same reason |
  | D-049 a failed drop retires the instance | not probed: on 2.0 the drop finds nothing, so it has little to fail on |

  Two things are new on 2.0 and match no row: the client fails every query (D-054), and a name
  with an unknown first part resolves inside the tenant's own warehouse (D-058). D-032
  (OpenFGA's cache) belongs to the catalog and was not re-run. The idle footprint (§9 q4) was
  not re-measured.
- **Because:** 2.0 is a major release, and the fact tags say every probe re-runs on a bump.
- **Class:** `discovery`.
- **Consequence:** 2.0 fixes three things the design works around (D-025, D-028, D-030) and
  breaks two it relies on (D-054, D-055). Adopting it would take one change carrying all of
  these: the client fix (D-054); the allowlist naming the catalog and the IdP, with
  `HTTPFileSystem` left on (D-055); the attestation reading the `s3_*` options and
  `extension_directory` with `current_setting()` (D-060); the taxonomy matching a 403 inside an
  `HTTPException` (D-060); re-pinning the P0 module, whose version pin fails on the bump as
  designed; and a decision on nested namespaces (D-058). With the shim, fifteen tests of the
  battery and the unit suite still fail. Each traces to an entry here or to a shortcut of the
  shim itself.
- **Proposed row (RFC 0078):** `ASSUMED` — P2 does not adopt 2.0. The lock stays at 1.5.6, and
  adoption is its own change, carrying the list above.

## D-054 — On 2.0 `DuckDbClient` fails every query, and the dependency floor admitted 2.0

- **Touches:** `DuckDbClient.run_query`, every analytics adapter on top of it, and
  `pyproject.toml`'s `duckdb>=1.1.0`. RFC 0078 treats 2.0 as a bump forze chooses.
- **RFC said:** "DuckDB 2.0.0 shipped on 2026-10-21 and is **not** the pinned engine."
- **Found:** 2.0's Python API has no `fetch_arrow_table` on a connection or a cursor. The error
  suggests `to_arrow_table`, which 1.5.6 also has. `DuckDbClient.run_query` calls
  `fetch_arrow_table`, so every query raises `AttributeError` on 2.0. This is not about the
  lake: 39 tests fail for it in the unit suite alone, across the client, the analytics adapter
  and the sources. The floor has no upper bound. A fresh `pip install 'forze[duckdb]'` without this
  repository's lock resolves 2.0 today, and gets a client that fails every query.
- **Because:** 2.0 removed the method, and forze's floor was written before 2.0 existed.
- **Class:** `discovery`.
- **Consequence:** users meet this now, not at adoption. The author chose the cap: branch
  `fix/duckdb-below-2` sets the extra to `duckdb>=1.1.0,<2` with a docs note, and goes to `main`
  ahead of this branch. P2 changes neither the client nor the extra. Its docs page carries the
  same note, word for word. Switching to `to_arrow_table` belongs to the adoption change.
  Whether `to_arrow_table` exists at the 1.1.0 floor was not checked.
- **Proposed row (RFC 0078):** `ASSUMED` — capped below 2 until the adoption change (D-053).

## D-055 — On 2.0 the catalog's own requests pass the external-access check, so the P1 profile never builds

- **Touches:** D-027's proposed row (the allowlist profile, adopted in P1), Unit 5's confirmation
  of §9 q1 ("the REST catalog's traffic does not go through the filesystem layer"), §3.4 steps
  5 and 8, and battery 8.
- **RFC said:** §9 q1 asked whether catalog traffic survives the hardening. On 1.5.6 it does,
  with neither the catalog nor the IdP allow-listed.
- **Found:** on 2.0 the iceberg extension's OAuth2 token request and its REST requests go through
  the same access check as a file read. Probed on a plain session, one profile at a time:

  | Profile | On 2.0 |
  |---|---|
  | `disabled_filesystems='LocalFileSystem,HTTPFileSystem'`, external access on, before or after `ATTACH` | reads |
  | allowlist of the prefix only, external access off, before `CREATE SECRET` (P1's profile) | `CREATE SECRET` fails: "Could not get token … file system operations are disabled by configuration" |
  | the same, set after `ATTACH` | the next `LoadTable` fails the same way |
  | allowlist of the prefix, the catalog origin and the IdP origin | reads |
  | that, plus `HTTPFileSystem` disabled | "File system HTTPFileSystem has been disabled" |
  | `allowed_paths` naming the token endpoint only | the token passes; the catalog's `/v1/config` is refused |

  So `RoutedDuckDbClient` cannot build a session on 2.0. Every tenant fails with
  `duckdb_session_setup_failed`.
- **Because:** on 2.0 the extension's HTTP requests pass the filesystem layer's access check. On
  1.5.6 they did not.
- **Class:** `discovery`.
- **Consequence:** on 2.0 the session must give up one layer. Either the allowlist names the
  catalog and IdP origins and `HTTPFileSystem` stays enabled, or external access stays on and
  the allowlist goes. The first keeps more. A statement can then open `http://` URLs on those two
  origins and nothing else: an `https://example.com` read is still refused, by the allowlist
  rather than by the disabled filesystem. The second loses D-027's in-engine refusal. The shim
  took the first. The battery's HTTP-egress test then fails only on the wording of the refusal.
- **Proposed row (RFC 0078):** `OPEN` — for 2.0, choose between an allowlist that names the
  catalog and IdP origins with `HTTPFileSystem` enabled, and external access on with no
  allowlist. Either way, step 8 attests the HTTP refusal by effect.

## D-056 — On 2.0 a vended secret ends with its statement

- **Touches:** D-025 and its proposed row, D-044, D-047 and D-048.
- **RFC said:** §3.2 relayed the maintainer: vended secrets are "scoped to the transaction and
  discarded after it". D-025 found that false on 1.5.6.
- **Found:** on 2.0 it is true. After three autocommit statements, a session lists no
  `__internal_ic_` secret, and none is visible while a statement runs. A by-path read inside the
  table after the statement gets HTTP 403, "No credentials are provided". P0's three pins of the
  1.5.6 behaviour fail.
- **Because:** the 2.0 iceberg build discards the secret when the transaction ends.
- **Class:** `discovery`.
- **Consequence:** on 2.0 the routed client's drop finds nothing, and the engine closes D-025's
  by-path window itself. The drop still costs one query on `duckdb_secrets()` per statement.
- **Proposed row (RFC 0078):** `ASSUMED` — the drop stays on 2.0 as a belt. The adoption change
  re-pins P0's three tests to the 2.0 behaviour.

## D-057 — On 2.0 `http_timeout` bounds a hung catalog or store request

- **Touches:** D-028 and its `OPEN` row, D-042's `OPEN` row, the §4.3 timeout cell, and the docs
  page's "about 120 s".
- **RFC said:** a blocking HTTP read is bounded by `http_timeout`. D-028 found it is not, on
  1.5.6.
- **Found:** on 2.0 it is. The probe set `SET GLOBAL http_timeout=2` and `http_retries=0` after
  `ATTACH`, with the external file cache off. Pausing the catalog container made the next
  statement fail after 2.0 s, with `IOException: … Timeout was reached` on the `LoadTable`
  request. Pausing the store did the same on the manifest-list GET. An interrupt sent at 1 s
  still did not cut the request short: the statement ended at 2.0 s, by the timeout, not with
  `InterruptException`.
- **Because:** the 2.0 iceberg build takes the session's HTTP settings.
- **Class:** `discovery`.
- **Consequence:** on 2.0, option (b) of D-028 is delivered upstream. A hung request lasts
  `http_timeout × (1 + http_retries)`, not about 120 s, and the §4.3 cell becomes true as
  written. D-042's early return still matters, since interrupt still does not cut a request.
- **Proposed row (RFC 0078):** `ASSUMED` — on adoption, D-028 resolves to (b) for 2.0, and the
  docs restate the hung-request bound as `http_timeout × (1 + http_retries)`. Option (a) stays
  the answer for 1.5.

## D-058 — On 2.0 a name with an unknown first part resolves inside the tenant's own warehouse

- **Touches:** battery 4 ("tenant A names B's namespace, or `ATTACH`es B's warehouse → refusal,
  **never rows**"), D-041's adaptation of it, §5.1 and decision 12.
- **RFC said:** a name that does not resolve fails at the catalog.
- **Found:** on 1.5.6, `wh_tenant_b.gold.orders` fails at bind with `Catalog "wh_tenant_b" does
  not exist!`. On 2.0 an unknown first part is read as one level of a nested namespace. A plain
  DuckDB database then reports that schema `wh_tenant_b.gold` does not exist. The iceberg
  extension instead sends only the last level to the catalog. With `USE lake.gold`,
  `wh_tenant_b.gold.orders` and `zzz.gold.orders` both send `LoadTable` for
  `namespaces/gold/tables/orders`, and both return tenant A's own rows. Without `USE`,
  `lake.zzz.gold.orders` does the same.
- **Because:** 2.0 resolves a multi-part name as a nested namespace, and the 2.0 iceberg build
  drops all but the last level when it builds the request.
- **Class:** `discovery`.
- **Consequence:** confinement holds. The principal and the warehouse are the tenant's, so the
  rows are A's own, never B's. Battery 4's first leg would see rows where it expects a refusal.
  The real defect is inside one tenant. In a warehouse with nested namespaces, `a.gold.orders`
  and `b.gold.orders` would both read `gold.orders`, so a statement can read a namespace it did
  not name. This should be reported to duckdb-iceberg.
- **Proposed row (RFC 0078):** `ASSUMED` — on adoption, battery 4 asserts "never B's rows" for
  this leg instead of a refusal. The deployment contract forbids nested namespaces in a tenant
  warehouse until the extension resolves them in full.

## D-059 — On 2.0 both credential fixes are in, and battery 17 still cannot run against Lakekeeper

- **Touches:** D-030 and its proposed row, and battery 17.
- **RFC said:** whether the pinned build has #1378's fix and #1536 is 🔎, and "Battery item 17
  pins it either way". D-030 deferred item 17 to the 2.0 re-probe.
- **Found:** the 2.0 iceberg build, `4c0c0874c4` (2026-10-19), has both. GitHub's compare puts
  it 688 commits ahead of #1379's merge (`28e62cb`, the fix for #1378) and 171 ahead of #1536's
  (`6570418`), and behind neither. duckdb-iceberg#1390 is closed. Neither fix was exercised
  live. The empty-`CLIENT_ID` case needs an IdP that issues a token without a client id, and
  Keycloak does not.
- **Because:** both fixes landed on `main` before the 2.0 line was cut.
- **Class:** `discovery`.
- **Consequence:** on 2.0 the build no longer needs a non-empty `client_id`, and refusing an
  empty one stays harmless. Battery 17 still needs a catalog that vends two or more credentials
  with nested prefixes for one table. Lakekeeper vends exactly one, on 2.0 as on 1.5.6. 2.0
  puts the fix in the build. It does not make the case reachable.
- **Proposed row (RFC 0078):** `ASSUMED` — battery 17 stays deferred until a catalog in the
  fixture vends nested-prefix credentials. The empty-`client_id` refusal stays.

## D-060 — Smaller 2.0 deltas the adoption change must carry

- **Touches:** §4.2 (classification), §4.5, D-029 and D-038, §3.4 step 8, batteries 5 and 9, and
  P0's battery 5 pin.
- **RFC said:** the 1.5.6 behaviour, as Units 5 and 6 pin it.
- **Found:**
  - A catalog 403 on `CREATE TABLE` arrives as an `HTTPException` carrying `Forbidden_403`, not
    as the bare `Error`. The adapter's HTTP pattern looks for `HTTP 403`, so this maps to
    `infrastructure`. A store 403 still reads "(HTTP 403 Forbidden)" and still maps to
    `dynamic_read_permission_denied`, because the adapter's pattern takes both its old wording
    ("HTTP code 403") and its new one. Only a statement past classification can reach the
    `CREATE TABLE` case.
  - `PRAGMA version` classifies as `PRAGMA`, not `SELECT`, so it is refused. `PIVOT` comes back
    as one statement of type `MULTI`, refused as `dynamic_read_write_refused` rather than as
    `dynamic_read_multi_statement`. Both are still refused, and the compiler emits neither.
  - `duckdb_settings()` no longer lists the `s3_*` options or `extension_directory`.
    `current_setting()` still reads them, so step 8 must read them that way, or it attests
    nothing for them.
  - `lock_configuration` now refuses `CALL enable_logging(…)`. Battery 5 turns logging on after
    the lock, so on 2.0 it must turn logging on first.
  - DuckDB's HTTP log no longer carries the SigV4 `Credential=` field, which P0's ambient-key pin
    reads. On 2.0 the read without step 2 still succeeds, and the store refuses unsigned reads,
    so the ambient key signed it. The pin just cannot see that.
  - The lock refusal quotes the option name twice (`""disabled_filesystems""`). The adapter
    matches on "the configuration has been locked", which still holds.
- **Because:** 2.0 changed each of these.
- **Class:** `discovery`.
- **Consequence:** none on 1.5.6.
- **Proposed row (RFC 0078):** `ASSUMED` — the adoption change carries each item. A 403 inside an
  `HTTPException` maps to `dynamic_read_permission_denied`. Step 8 reads the `s3_*` options and
  `extension_directory` with `current_setting()`. Battery 5 turns logging on before the lock.
  P0's ambient-key pin reads the store's request log. The classification tests follow the new
  types.

## Review findings — 2026-10-28

PR #690, after it was published. Two findings from CodeRabbit and CodeAnt, both fixed. Neither
changes a proposed row.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | Low | D-054 describes the dependency as it stood when written: no upper bound, and the cap on its own branch. #679 has since merged, so `main` caps the extra at `duckdb>=1.1.0,<2` and the lock records 1.5.6; D-054 stays as written (CodeRabbit). | Recorded here |
| R-2 | Medium | `just test -m integration` replaced the recipe's `not lake_slow`, since pytest keeps the last `-m`, so the expiry battery ran (CodeAnt). | Fixed: the duckdb integration conftest skips a `lake_slow` test unless the marker expression names it |

---

# Unit 8 · RFC 0078 — DuckDB 2.0 adoption

Branch `feat/duckdb-2`, on `main` after P1 (#685) and P2 (#690). Scope: make `duckdb==2.0.0` (released 2026-10-21) the locked engine, carry
D-053's list, and keep DuckDB 1.5 supported. Same stack as Units 5 to 7. Two builds:
`duckdb==2.0.0` with iceberg `4c0c0874c4`, the lock's, and `duckdb==1.5.6` with iceberg
`890b78a9c`, run as an overlay (`uv run --with duckdb==1.5.6`).

**Working assumptions.** The author has not answered Units 5 to 7. Every `ASSUMED` row there is
taken as the design. D-055 was `OPEN`; this unit answers it under the maintainer's standing
instruction, "the most reasonable choice for a production framework" (D-062, D-063). D-054's cap
(`<2`) went to `main` as #679. This unit sets that `pyproject.toml` line to `>=1.5.6,<3`, and
rewords #679's changelog entry, which no release has shipped, to the fix as it now stands. D-042's early
return stays, and D-028 stays `OPEN` for 1.5 (D-068).

**Drift count: 0.** Sixteen entries, D-061 to D-076: ten `discovery`, six `spec-gap`. Product
code changes in four places: the plain client's result fetch (D-061), the routed session
profile and its attestation (D-062, D-063, D-065, D-075), the dynamic-read classification
(D-066, D-072, D-076), and the taxonomy (D-067). D-072 to D-076 came out of a review of the
branch before it was published, each probed on both builds against the stack. D-072 was a
defect: D-066's check walked table references only, and a table function went round it. Units
5 to 7 are not edited here. D-054's cap is superseded by D-061.

**Where each item lives.**

- The plain client: `DuckDbClient.run_query` and `_ignore_arrow_copy_warning`; pinned by
  `test_run_query_does_not_warn_about_the_arrow_copy`.
- The session profile: `_allowlist`, `_session_statements`, `_expected_settings`, `_setting`
  and `_attest` in `routed_client.py`, and the new `RoutedDuckDbClient.ca_cert_file`. Battery 8
  (`TestHttpEgress`) and 14 (`TestAttestation`) pin it on the stack. The unit suite pins the
  statements (`test_the_session_allow_lists_the_prefix_the_catalog_the_idp_and_the_ca_bundle`).
- HTTPS, which the stack does not serve: the P0 module's
  `test_on_2_https_needs_the_ca_bundle_on_the_allowlist`, against a local TLS server with a
  throwaway CA.
- The classification: `_refuse_indirect_reads` in `client.py`, which refuses unattached
  catalogs (D-066), table functions and paths (D-072) and `PRAGMA` (D-076). The unit suite,
  batteries 4, 5 and 8, and the P0 pin `test_an_unknown_first_part_is_a_nested_namespace_on_2`.
- What the engine's allowlist does not do: the P0 pins `test_the_allowlist_resolves_no_dot_segments`
  and `test_a_redirect_is_followed_past_the_allowlist` (D-073, D-074).
- Version-specific facts: the P0 module and the battery branch on the major. The P0 version
  pin admits exactly the two probed builds (`_PROBED`).
- 1.5 in CI: `just test-duckdb-1x`, run by `ci.yml`'s duckdb shard after `just test` (D-070).

**The battery on the adopted profile.** Every item, run against the stack on both builds.

| Item | 2.0.0 | 1.5.6 |
|---|---|---|
| 1 fixture self-test | pass | pass |
| 3 disjoint tenants | pass | pass |
| 4 cross-tenant by name | refused, including `zzz.gold.orders`, `lake.zzz.gold.orders` (D-066), `query(…)` and `query_table(…)` (D-072). Stubbed: A's own rows (D-058, D-072) | refused. Stubbed: `BinderException` |
| 5 cross-tenant by path | refused by classification (D-072). Stubbed: refused by the allowlist, no request logged | the same |
| 6 classification | pass | pass |
| 7 local filesystem | refused by the allowlist ("file system operations are disabled by configuration"); the process control refuses with the session's layer gone | the same |
| 8 HTTP egress | every URL refused by classification (D-072). Stubbed: `https://example.com` and the catalog's and IdP's siblings refused; the catalog tree answers 401 (D-064) | the same |
| 9 lock | pass, with the doubled quotes of D-060 | pass |
| 10 read-only | pass. Without `READ_ONLY`, a CTAS is refused by the catalog (403) before it stages (D-067) | pass. A CTAS is refused by the allowlist where it stages |
| 12 shell | pass | pass |
| 13 pool | pass | pass |
| 14 attestation | every step removed once is refused; an allowlist entry removed fails setup (the catalog or IdP is refused); the catalog's origin allow-listed fails by effect (D-075) | the same, except an allowlist entry removed builds and fails attestation |
| 16 taxonomy | pass | pass |
| 11b, 11c | 6 passed in 1014.8 s (`just lake-slow`), and in 1020.4 s after the review's changes. The kept-secret leg reads (D-056), and the held-open leg logs no status (D-067) | 6 passed in 1019.1 s through the overlay, after the review's changes (D-070) |
| 17 | deferred (D-059) | deferred |

**Counts.** The DuckDB unit and integration suites, `lake_slow` excluded: 389 passed on 2.0.0
(`just test`) and 389 on 1.5.6 (`just test-duckdb-1x`), in about 80 s each with the stacks; 270
of them are the unit suite. Before the review's changes it was 361 on each. On a 1.5.0 overlay the routed battery passed 75 of 79
(D-061).

**Confirmed, so no entry.**

- The vended-secret drop runs on 2.0 and finds nothing, under `lock_configuration` too (D-056).
  P0's three D-025 pins now pin each build.
- `http_timeout` reads back in whole seconds, and `lock_configuration` refuses every `SET` of
  the profile on the parent connection and on cursors, on both builds.
- `ca_cert_file` set to a CA a client trusts works on both builds; 1.5.6 reads it outside
  DuckDB's filesystem, so neither the allowlist nor `LocalFileSystem` matters there.
- A path entry matches exactly, on both builds: the token endpoint with `/x`, `?q` or `#f`
  added is refused, and so is a file beside the CA bundle or the bundle's path with `x` added.
  A directory entry is a directory: `tenant-ab` and `catalogx` are refused, and an entry
  written without its slash reads back with one, so its siblings are refused too.
- `READ_ONLY` refuses `CREATE SCHEMA lake.…` on both builds before any request. `CREATE TABLE`
  looks the table up in the catalog first.

## D-061 — The plain client fetches with `to_arrow_table`, which exists from 1.5.0 and is all 2.0 keeps

- **Touches:** D-054 and its row, which this entry supersedes ("Switching to `to_arrow_table`
  belongs to the adoption change"; "Whether `to_arrow_table` exists at the 1.1.0 floor was not
  checked"), `DuckDbClient.run_query`, and the extra's range.
- **RFC said:** nothing. D-054 capped the extra below 2, until the adoption change.
- **Found:**
  - A cursor's `to_arrow_table` is absent on 1.2.0, 1.3.0, 1.4.0 and 1.4.4, and present from
    1.5.0. On 1.5.6, `fetch_arrow_table` raises a `DeprecationWarning` naming it. On 2.0.0 it is
    the only Arrow fetch left on a cursor. Rows and types match on both builds (integer,
    string, decimal, `NULL`, an empty result).
  - On 2.0.0 every Arrow fetch from an executed cursor warns: "Arrow output on a DB-API result
    makes an extra copy of the rows, use con.sql(query).to_arrow_table() instead". The
    relation path it names exists from 1.2.0 and does not warn, but binds a `SELECT` twice.
    Against a local HTTP server, `sql(q).to_arrow_table()` made three requests where
    `execute(q)` made two, on both builds. On a lake that is a second `LoadTable` per statement.
  - 1.1.0 has no CPython 3.13 wheel, and its sdist fails to build. The floor never installed on
    this project's Python.
  - The routed battery on 1.5.0 (extensions installed by hand, D-071) passes 75 of 79. All
    confinement items pass. Four outcomes differ from 1.5.6: a catalog 401 maps to
    `infrastructure`, a describe-only read times out instead of being refused, a CTAS reaches
    the catalog first, and a persistent-secrets fault fails attestation instead of setup.
- **Because:** 1.5 added `to_arrow_table` to the cursor, and 2.0 removed the rest.
- **Class:** `discovery`.
- **Consequence:** one call on both majors. The 2.0 warning is filtered for the client module
  only, by `initialize`. A filter set at import does not survive pytest, which restores the
  filters after collection, and an application suite run with `filterwarnings = error` would
  then fail every query on 2.0. The extra becomes `duckdb>=1.5.6,<3`: the plain client needs
  1.5.0, and 1.5.6 is the one 1.x build the routed client is verified on, so every admitted
  1.x build is a verified one. Users on 1.1 to 1.5.5 must upgrade, which the changelog marks.
  D-054 is superseded: its cap below 2 was the stopgap until this change.
- **Proposed row (RFC 0078):** `ASSUMED` — the extra is `duckdb>=1.5.6,<3`, and both clients run
  on both majors with one code path. This supersedes D-054's row.

## D-062 — D-055 answered: one allowlist of the prefix, the catalog tree and the token endpoint, and no filesystem disabled

- **Touches:** D-055 (`OPEN`), D-027's proposed row, §3.4 steps 5 and 8, §3.5's rows "No local-disk
  read/write" and "No arbitrary HTTP egress", and batteries 7, 8 and 14.
- **RFC said:** §3.4 step 5 sets `disabled_filesystems='LocalFileSystem,HTTPFileSystem'`. D-027
  adds an allowlist of the prefix with external access off. D-055 leaves two options open.
- **Found:** D-055's first option, narrowed. `allowed_directories` names the prefix and the
  catalog's base URI as a directory, `allowed_paths` names the token endpoint as one exact
  path, and external access goes off before `CREATE SECRET`. D-055 probed whole origins; this
  is the narrowest set that builds. It builds, attests and reads on both builds. Under it, on
  both builds, these are all `PermissionException` "Cannot access file … file system
  operations are disabled by configuration": local reads and writes (`/etc/passwd`,
  `/proc/self/environ`, `COPY TO`, a local `ATTACH`, a `glob`, `INSTALL`), any other URL, and
  B's prefix, before any request. With `HTTPFileSystem` also disabled, 2.0 refuses the catalog
  (D-055). With `LocalFileSystem` disabled, 2.0 cannot make an HTTPS request (D-063).
- **Because:** on 2.0 the allowlist is the one check that admits the extension's own requests
  and refuses a statement's. The disabled filesystems refuse both.
- **Class:** `discovery`.
- **Consequence:** one layer inside the engine, for local and HTTP alike, where 1.5 had two for
  local paths. The outer controls, the process filesystem and the egress policy, stay primary
  (§3.5). Battery 7's second leg still proves the process control. 1.5.6 runs the same profile,
  so the battery tests the production profile on both builds. Step 8 attests the refusals by
  effect, a local path and an unlisted URL, as D-055's row asked.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.4 step 5 is the allowlist above, set before
  `CREATE SECRET`, and `disabled_filesystems` is not set. §3.5's two "promise" rows name the
  allowlist. This overrides §3.4 step 5 and D-027's row, which stay as written.

## D-063 — On 2.0 every HTTPS request reads the CA bundle through the allowlist, so `LocalFileSystem` stays on and the bundle is configured

- **Touches:** D-062, §3.4's connect options and step 5, §3.1 (the deployment contract), and the
  stack, which serves plain HTTP only.
- **RFC said:** nothing about TLS.
- **Found:**
  - With external access off and `https://example.com/` allow-listed, 2.0.0 refuses the read:
    "Cannot read a CA certificate bundle: file system access is restricted by configuration.
    Add one of /etc/ssl/certs/ca-certificates.crt, … to allowed_paths, point ca_cert_file at an
    allowed file, or disable server certificate verification". With the bundle in
    `allowed_paths` it reads. With `LocalFileSystem` disabled as well, it is refused again. 1.5.6
    reads in all four cases.
  - Once `ca_cert_file` is set, 2.0 hands the file to curl for every request, plain HTTP too.
    An empty file fails the token request: "Failed to set curl option 40309".
  - A by-path read inside the prefix with no vended secret behind it heads for the public AWS
    endpoint (D-040). On 2.0 under the allowlist it now stops at the bundle, before any request.
  - DuckDB reports a symlink in `allowed_paths` as its target.
- **Because:** 2.0's `httpfs` reads the bundle through DuckDB's filesystem, which honours
  `enable_external_access` and the disabled filesystems.
- **Class:** `discovery`.
- **Consequence:** `RoutedDuckDbClient` gains `ca_cert_file`, resolved to its real path at
  construction. When it is set, the session sets `ca_cert_file` and allow-lists the path, and
  step 8 reads it back. Without it, a 2.0 session with an HTTPS endpoint fails setup with
  DuckDB's own message, so this fails closed. A statement can read the bundle, which holds
  public certificates only. `allowed_paths` also admits a write to it: classification refuses
  `COPY`, and the deployment's read-only filesystem is the outer control.
- **Proposed row (RFC 0078):** `ASSUMED` — the deployment contract (§3.1) names the CA bundle as
  the routed client's `ca_cert_file` whenever the catalog, the IdP or the store is HTTPS.

## D-064 — What the allowlist opens over HTTP: the catalog's tree and the token endpoint, with no token

- **Touches:** §3.5's "No arbitrary HTTP egress" row, battery 8, and D-055's consequence ("A
  statement can then open `http://` URLs on those two origins").
- **RFC said:** `HTTPFileSystem` disabled refuses every `http(s)://` read.
- **Found:** under the profile, on both builds, `read_text('<catalog uri>/v1/config')` goes out
  and Lakekeeper answers 401. A GET on the token endpoint gets 405. The catalog origin's
  `/health` and the IdP's `/certs` are refused by the allowlist. The requests carry no bearer
  token: the catalog secret is an `iceberg` secret, not an HTTP one.
- **Because:** a directory entry matches by prefix, a path entry exactly.
- **Class:** `discovery`.
- **Consequence:** a statement can send unauthenticated GETs to two endpoints the deployment
  already trusts, which is less than D-055 expected. The catalog's tree is shared by every
  tenant. Since D-072 a dynamic read cannot name a URL at all, so only analytics SQL, which is
  code, can do this. Sending data out by URL needs control of the catalog or the IdP. Battery 8
  pins both halves, with classification stubbed. `LakeTenantCatalog` now refuses a `uri`
  or `oauth2_server_uri` that is not an http(s) URL, since each widens the allowlist.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.5's HTTP row reads "egress policy; inside the
  engine, the allowlist bounds HTTP to the catalog's tree and the token endpoint".

## D-065 — Step 8 reads each setting with `current_setting()`, one query each

- **Touches:** §3.4 step 8, D-035, and D-060's attestation item.
- **RFC said:** step 8 reads `duckdb_settings()`.
- **Found:** 2.0's `duckdb_settings()` leaves out the `s3_*` options and `extension_directory`
  (D-060). `current_setting()` reads them on both builds, as typed values. But on a fresh
  cursor, `current_setting()` of an extension option the session never set raises: "Setting
  with name "s3_access_key_id" is not in the catalog, but it exists in the httpfs extension".
  On the parent connection it returns `NULL`. Battery 14's fault that skips the `s3_*` clear
  showed it: the error escaped step 8 and reached the caller as the statement's
  `dynamic_read_statement_invalid`. Lists read back sorted.
- **Because:** an extension option is not in a new connection's catalog until it is set.
- **Class:** `discovery`.
- **Consequence:** step 8 reads one setting per query, and one it cannot read counts as a
  problem. It now runs inside the setup guard, so any other error there is
  `duckdb_session_setup_failed`, never a statement's code. That adds about twenty queries per
  instance build. Battery 14 pins every step's removal on both builds.
- **Proposed row (RFC 0078):** `ASSUMED` — step 8 reads each setting with `current_setting()` on a
  fresh cursor, and treats one it cannot read as unattested.

## D-066 — Classification refuses a table named through a catalog that is not attached, or nested past three parts

- **Touches:** D-058 and its row, §4.2, battery 4, and decision 12.
- **RFC said:** a name that does not resolve fails at the catalog. D-058 proposed asserting
  "never B's rows" and forbidding nested namespaces in the deployment contract.
- **Found:** D-058 re-confirmed on 2.0.0. `zzz.gold.orders`, `wh_tenant_b.gold.orders` and
  `lake.zzz.gold.orders` all return tenant A's rows. `json_serialize_sql` parses without
  binding and gives each table reference's catalog, schema and table on both builds. On 2.0 it
  also gives the full dotted path (`qualified_name.path`), the levels the extension drops. 1.5.6
  cannot parse a four-part name ("NameListToString NOT IMPLEMENTED"). `get_table_names` is no
  use: it binds table functions, and an `iceberg_scan` in it raised the allowlist's refusal, so
  it does I/O.
- **Because:** D-058 left the safer option open, and refusing is it.
- **Class:** `spec-gap`.
- **Consequence:** after the `SELECT` check, the client serializes the classified statement. It
  refuses any table whose catalog is not attached (compared case-insensitively) or whose path
  has more than three parts, with `dynamic_read_statement_invalid`. That is one parse-only
  query per dynamic read, with no I/O. Both builds give the same answer, so battery 4 keeps
  asserting a refusal. D-058's "never B's rows" becomes the stubbed leg's assertion on 2.0.
  Analytics reads are not checked, since their SQL is code. A view's body is resolved by the
  engine, not by this check. As built here the check walked table references only, and a table
  function taking a name went round it: D-072 closes that.
- **Proposed row (RFC 0078):** `ASSUMED` — this overrides D-058's row: classification refuses such
  names. The deployment rule against nested namespaces stays, as a second line.

## D-067 — D-060's deltas, carried; on 2.0 a CTAS reaches the catalog first, and a failed request logs no status

- **Touches:** §4.5, D-029, D-038, §4.2, batteries 5, 9 and 10, and D-060's row.
- **RFC said:** the 1.5.6 behaviour, as D-060 lists it.
- **Found:**
  - With `READ_ONLY` taken away and classification stubbed, a CTAS on 2.0 is refused by the
    catalog (403, `NamespaceActionForbidden`) before it writes anything. On 1.5.6 it stages a
    local file first, and the allowlist refuses that.
  - DuckDB 2's HTTP log records a failed GET with `'response': NULL`, where 1.5.6 records its
    status. Battery 11b's held-open leg reads the store request's status from that log. On 2.0
    it reads empty, while the error still names HTTP 400.
- **Because:** 2.0's iceberg build creates the table in the catalog first, and 2.0's logger
  writes no response for a request that failed.
- **Class:** `discovery`.
- **Consequence:** the rest of D-060 is carried as proposed, with one change.
  - The taxonomy maps `Unauthorized_401` and `Forbidden_403` inside an `HTTPException` to
    `dynamic_read_permission_denied`. Both CTAS refusals map there.
  - On 2.0, `PRAGMA` is refused as `PRAGMA` and `PIVOT` as `MULTI`. The tests pin each build.
  - Battery 9 matches the lock refusal with either quoting, and tries
    `SET allowed_directories=['/']` instead of `SET disabled_filesystems=''`, which no longer
    guards anything. Battery 5 turns logging on before the lock, through the sequence seam.
  - The change: P0's ambient-key pin checks the key on 1.5 only. On 2.0 it rests on the read
    succeeding against a store that refuses unsigned reads (battery 1). Reading the store's
    request log, as D-060 proposed, was not built.
  - Battery 11b's held-open leg asserts one store request on both builds, with its status
    `BadRequest_400` on 1.5 and empty on 2.0.
- **Proposed row (RFC 0078):** `ASSUMED` — D-060's row stands, except that P0's ambient-key pin on
  2.0 infers the signature from the read rather than from the store's log.

## D-068 — On adoption: the drop stays and finds nothing, and a hung request lasts `http_timeout` × (1 + `http_retries`) on 2.0

- **Touches:** D-056, D-057, D-028 (`OPEN`), D-042 (`OPEN`), and the docs' "about 120 s".
- **RFC said:** a blocking HTTP read is bounded by `http_timeout`.
- **Found:** P0's D-025 pins, re-pinned per build. On 2.0 no `__internal_ic_` secret exists after
  a statement, while one runs, or after a rollback. The routed client's drop runs and finds
  nothing, under the lock too. D-057 was not re-measured: Unit 7's 2 s probe stands.
  `http_retries` defaults to 3 on both builds, and the routed client does not set it.
- **Because:** 2.0 ends the secret with its statement and honours `http_timeout`.
- **Class:** `spec-gap`.
- **Consequence:** the drop stays as a belt, at one `duckdb_secrets()` query per read. D-042's
  early return stays, since an interrupt still does not cut a request short. The docs and
  docstrings state the hung-request bound per build.
- **Proposed row (RFC 0078):** `ASSUMED` — D-028 resolves to option (b) on 2.0, with the bound
  `http_timeout` × (1 + `http_retries`). On 1.5 it stays `OPEN`, worked under option (a).

## D-069 — The lock moves to 2.0.0 with a newer uv, and one unrelated wheel drops with it

- **Touches:** `uv.lock`.
- **RFC said:** nothing.
- **Found:** the local uv 0.10.10 rewrites the lock as revision 3, and so does uv 0.11.6. uv
  0.13.6 keeps revision 5. `uv lock --upgrade-package duckdb` under 0.13.6 moves `duckdb` from
  1.5.6 to 2.0.0 (abi3 wheels) and updates its specifier. It also drops one wheel line,
  zensical 0.0.71's armv7l. 0.11.6 drops the same line, so it comes from the index, not the
  tool. PyPI confirms it: `https://pypi.org/pypi/zensical/0.0.71/json` lists nine files and no
  armv7l wheel (a first query timed out; the second answered). uv 0.10.10 then accepts the
  lock: `uv lock --check` passes and `uv sync` leaves it alone.
- **Because:** revision 5 postdates uv 0.11, and the index no longer lists that wheel.
- **Class:** `discovery`.
- **Consequence:** the lock's diff is `duckdb`, its specifier and that one wheel line. CI's
  `setup-uv` installs a current uv. A relock with a uv older than 0.12 would downgrade the
  format.
- **Proposed row (RFC 0078):** `ASSUMED` — the lock pins the newest admitted DuckDB, 2.0.0, and
  1.5.6 runs as an overlay (D-070).

## D-070 — 1.5 stays tested: the duckdb shard runs the DuckDB suites again on 1.5.6

- **Touches:** CI and D-061.
- **RFC said:** nothing. The fact tags say every probe re-runs on a bump.
- **Built:** `just test-duckdb-1x`, which runs the unit and integration DuckDB suites under
  `uv run --with duckdb==1.5.6` with `just test`'s markers. It first asserts the overlay really
  gives a 1.5 build, so a resolver change cannot run the suites on the lock's 2.0 unnoticed.
  `ci.yml`'s duckdb shard runs it after `just test`. `lake_slow` ran once on 1.5.6 through the
  overlay: 6 passed in 1019.1 s. The scheduled `lake-expiry` workflow covers the lock's 2.0 only.
- **Because:** the range admits 1.5.6, and the lock no longer exercises it.
- **Class:** `spec-gap`.
- **Consequence:** the duckdb shard runs about twice as long, with no coverage upload from the
  second run. A 1.5.x release after 1.5.6 is admitted but untested until the overlay pin and
  `_PROBED` move together.
- **Proposed row (RFC 0078):** `ASSUMED` — 1.5 support is CI-tested at 1.5.6 only.

## D-071 — The image needs `avro` too: `iceberg` loads it, and fails without it

- **Touches:** §3.4's connect options and step 1 ("pre-installed; INSTALL never runs at
  runtime"), the docs' `extension_directory` line, and the stack's extension install.
- **RFC said:** the image holds `httpfs` and `iceberg`.
- **Found:** from an extension directory holding only `httpfs` and `iceberg`, with autoinstall
  off, `LOAD iceberg` fails on both builds: "The iceberg extension requires the avro extension".
  On 1.5.0, `INSTALL iceberg` does not fetch `avro`, so the stack's session setup failed there.
  Battery 14's extension-directory test copies the whole installed directory, `avro` included,
  so it never saw this.
- **Because:** the iceberg build loads `avro` at its own initialization.
- **Class:** `spec-gap`.
- **Consequence:** the stack installs `avro` with the other two, and the docs and the
  `extension_directory` docstring name all three. An image built from the old docs fails setup
  on the first tenant (`duckdb_session_setup_failed`), so it fails closed.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.4 step 1's pre-installed set is `httpfs`, `iceberg`
  and `avro`.

## D-072 — A table function or a path went round D-066's check; classification now allows three table functions and no path

- **Touches:** D-066, D-058, §4.2 ("Classification is a belt"), decision 9, and batteries 4, 5
  and 8.
- **RFC said:** §4.2 classifies by statement type, and notes that a `SELECT` can still reach a
  side-effecting table function. D-066 added a check over table references.
- **Found:** the check walked `BASE_TABLE` nodes only. On 2.0.0, through the shipped session,
  `SELECT … FROM query('FROM zzz.gold.orders')`, `query_table('zzz.gold.orders')` and
  `query_table('wh_tenant_b.gold.orders')` all return tenant A's rows. That is D-058 again,
  through a table function. 1.5.6 refuses them at bind. `json_serialize_sql` shows every table
  function as a `TABLE_FUNCTION` node with its name, on both builds. It shows a quoted path in
  `FROM` (`FROM 's3://…'`, which DuckDB reads as a file) as a `BASE_TABLE` with no catalog.
  `read_parquet`, `iceberg_scan`, `read_text`, `glob` and `duckdb_secrets()` passed the check
  too, leaving only the allowlist and classification by type in their way.
- **Because:** D-066 checked what a statement names as a table, not what else can open one.
- **Class:** `spec-gap`.
- **Consequence:** classification refuses, with `dynamic_read_statement_invalid`, any table
  function but `range`, `generate_series` and `unnest`, and any table name that looks like a
  path or a URL (it holds `/`, `:` or `.`). That covers nested positions too: subqueries, CTEs,
  joins and `LATERAL`. A dynamic read can then open nothing but tables of an attached catalog,
  by name. Batteries 5 and 8 assert the refusal, then stub classification to keep proving the
  allowlist underneath. Widening the list is a code change, and a reviewed one.
- **Proposed row (RFC 0078):** `ASSUMED` — §4.2's classification also refuses every table
  function outside `{range, generate_series, unnest}` and every path-like table name. This
  extends D-066's row.

## D-073 — The engine's allowlist resolves no dot-segments

- **Touches:** D-062, §3.5's path and HTTP rows, battery 5, and the attestation (D-075).
- **RFC said:** nothing about path normalization.
- **Found:** probed on both builds through the shipped session, with logging on:
  - `<catalog>/../management/v1/info`, `%2e%2e` and `%2E%2E` pass the allowlist as inside the
    catalog's tree. The request goes out, and Lakekeeper answers 401. `<catalog>//../management`
    goes out too (404). `<origin>//catalog/…` is refused.
  - `s3://lake/tenant-a/../tenant-b/<file>` passes as inside A's prefix, with `read_parquet`
    and `iceberg_scan` alike. With no secret matching it, it goes unsigned to the public AWS
    endpoint, which refuses it (403) or times out. `%2e%2e` is sent double-encoded, as a
    literal key.
  - `s3://lake/tenant-a/<A's table>/../../tenant-b/<file>` matches A's vended secret on 1.5.6
    (in a raw session that keeps it) and is signed with it. The store refuses it (403). Used
    directly, A's vended credential gets 403 on that key and on B's own.
- **Because:** the allowlist compares spellings, and neither DuckDB nor its HTTP clients
  resolve `..` before the check.
- **Class:** `discovery`.
- **Consequence:** inside the engine, the allowlist is not a bound against a crafted path.
  Nothing crosses tenants: the store refuses, because the vended credential is scoped to the
  table. For dynamic reads, D-072 means no statement can pass a path at all. Analytics SQL is
  code. Step 8 cannot probe a `..` path, because it would go out. The P0 pin
  `test_the_allowlist_resolves_no_dot_segments` shows a bump that starts resolving them.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.5 states that the in-engine allowlist matches
  spellings, and that confinement by path rests on the store and the vended credential's scope.

## D-074 — A redirect from an allow-listed origin is followed without being checked again

- **Touches:** D-062, D-064, and §3.5's HTTP row.
- **RFC said:** nothing about redirects.
- **Found:** with a stub on an allow-listed path answering 302, both builds follow it: to an
  off-list host, and to a path outside the allowed directory on the same host. Each target got
  `HEAD` and `GET`, and the read returned its rows. No setting governs redirects: nothing in
  `duckdb_settings()` matches `%redirect%` or `%follow%`, on either build.
- **Because:** `httpfs` follows redirects in its HTTP client, below the allowlist check.
- **Class:** `discovery`.
- **Consequence:** there is no session setting to turn redirects off, so none is attested. A
  dynamic read cannot name a URL (D-072). The engine's own requests go to the catalog, the IdP
  and the store, and follow their redirects: those servers and the egress policy are the
  controls. The P0 pin `test_a_redirect_is_followed_past_the_allowlist` shows a bump that
  starts re-checking them.
- **Proposed row (RFC 0078):** `ASSUMED` — §3.5's HTTP row adds that the engine follows a
  redirect from an allow-listed origin unchecked, and that the egress policy bounds it.

## D-075 — Step 8 checks more refusals by effect, the class decides, and the bundle must be PEM

- **Touches:** §3.4 step 8, D-062, D-063, and battery 14.
- **RFC said:** step 8 reads the settings and checks the filesystems by effect.
- **Found:** on both builds, through the shipped session:
  - a path entry matches exactly: the token endpoint with `/x`, `?q` or `#f` added is refused;
  - a directory entry refuses its siblings (`tenant-ab`, `catalogx`). Written without its
    slash, it reads back with one;
  - only the CA bundle's exact path is readable, not a file beside it;
  - `READ_ONLY` refuses `CREATE SCHEMA lake.…` with no request.

  The message check on a refusal matched one English phrase, and 2.0 had already reworded
  other messages (D-060).
- **Because:** these are the edges of an allowlist and of a read-only attach, and none was
  attested.
- **Class:** `spec-gap`.
- **Consequence:**
  - Step 8 now also checks that these are refused: the token endpoint with a path or a query
    added, a sibling of the prefix and of the catalog's tree, a file beside the CA bundle, and
    `CREATE SCHEMA` on `lake`.
  - The exception class decides each probe. A message without the expected phrase is logged.
  - The path probes run only once external access reads back off, since on they would go out.
    The write probe runs only once `lake` reads back read-only, so it cannot reach the catalog.
  - A comment beside the allowlist records the exfiltration boundary: secrets are unreadable
    (redacted `duckdb_secrets()`, cleared `s3_*`, none persistent, `lock_configuration`), and
    each of those is attested.
  - `ca_cert_file` must hold a PEM certificate at construction, and its docs say every
    statement on the instance can read it.
  - Battery 14 adds a widened allowlist (the catalog's whole origin), which fails by effect.
- **Proposed row (RFC 0078):** `ASSUMED` — step 8 attests the allowlist's edges and the
  read-only attach by effect, by exception class.

## D-076 — `PRAGMA` and `PIVOT` get one answer on every build

- **Touches:** D-060, D-067, §4.2, and the classification tests.
- **RFC said:** `PRAGMA version` classifies as `SELECT` and passes (§4.2, on 1.5.6).
- **Found:** 1.5.6 rewrites `PRAGMA version`, typing it `SELECT` with the text
  `SELECT * FROM pragma_version();`. 2.0 keeps it as `PRAGMA`. A `PIVOT` is two statements on
  1.5.6 and one `MULTI` statement on 2.0.
- **Because:** the two builds' parsers differ.
- **Class:** `discovery`.
- **Consequence:** a `pragma_*` table function is refused as `PRAGMA` (`dynamic_read_write_refused`
  with `statement_type: PRAGMA`), so `PRAGMA version` gets that answer on both. A `MULTI`
  statement is refused as `dynamic_read_multi_statement`, so `PIVOT` gets that answer on both.
- **Proposed row (RFC 0078):** `ASSUMED` — §4.2's `PRAGMA version` is refused, on every build.
  This overrides that line of §4.2, which stays as written.

## Review findings — 2026-10-28

PR #700, after it was published. Four findings from CodeAnt, and one CodeQL alert. Two are
fixed, one is refuted, and one is kept by design. None changes a proposed row.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | Medium | `ca_cert_file` passed when it held only the PEM begin marker, so a truncated bundle built the client and then failed every request (CodeAnt, twice) | Fixed — OpenSSL loads the file at construction (`ssl.create_default_context(cafile=…)`), and a file it loads no certificate from is refused |
| R-2 | — | The P0 probe's local TLS server allowed TLS 1.0 and 1.1 (CodeQL `py/insecure-protocol`) | Fixed — its context sets TLS 1.2 as the minimum |
| R-3 | — | Battery 9's lock messages use `"+`, read as a literal by mistake (CodeAnt) | Refuted — the `+` quantifies the quote, which DuckDB 2 doubles (D-060); the battery passes on 1.5.6 and 2.0.0 |
| R-4 | Low | A quoted table name holding a dot, legal in Iceberg, is refused as a path (CodeAnt) | Kept — parsing binds nothing, so `"orders.v2"` cannot be told from a replacement scan of `"orders.csv"`. Refusing every `/`, `:` and `.` is D-072's fail-closed choice; the docs now say such a table cannot be read dynamically |

---

# Unit 9 · RFC 0077 P3 — surfaces onto ingress

Branch `refactor/ingress-fastapi-socketio-ws`. RFC 0077 §12 P3's bullet "moving FastAPI,
Socket.IO and the WS route onto `ingress`", against §5.7 and the P1 core in
[`execution/ingress/`](../src/forze/application/execution/ingress/). Each surface's copy of a
boundary step was set beside the `ingress` function, and a step moved only where the two
agree on every input the surface can send.

**Drift count: 1.** Eleven entries: one `drift` (D-083), six `spec-gap`, four
`discovery`. Two steps moved with no behaviour change (D-077, D-078). Three behaviour
changes landed, each in a commit of its own:
- the WS route's bind-and-run moved whole and gained metadata (D-083);
- Socket.IO's events mint metadata while its bind-and-run stays (D-086), under the same
  changelog entry as D-083;
- Socket.IO's events bind their connection's tenant (D-087), with an entry of its own.

Six steps stayed, each for the reason its entry gives.

**Socket.IO moved one step.** Its events mint their metadata through
`mint_invocation_metadata` (D-086), and they now bind the tenant the WS route already
bound (D-087). Every other step is either shared with the WS route below the execution
layer (D-084, D-085) or runs through a port that has no registry (D-086).

| Id | Surface | Step | Decision |
|---|---|---|---|
| D-077 | FastAPI | invocation-metadata minting | moved, no change |
| D-078 | FastAPI | API-key split and verification | moved, no change |
| D-079 | FastAPI | principal → tenant resolution | stays: the HTTP ladder differs |
| D-080 | FastAPI | route input validation | stays: the 422 body would change |
| D-081 | FastAPI | exposure and the sensitive refusal | stays: same rule, other wording |
| D-082 | FastAPI | bind-and-run | stays: the envelope is bound by middleware |
| D-083 | WS route | bind-and-run | moved, **behaviour change** |
| D-084 | WS route, Socket.IO | payload validation | stays: route-typed, not descriptor-typed |
| D-085 | WS route, Socket.IO | identity and tenant ladder | stays: below the execution layer |
| D-086 | Socket.IO | metadata, bind-and-run | metadata moved, **behaviour change**; run stays |
| D-087 | Socket.IO | tenant binding per command | fixed, **behaviour change** |

## D-077 — FastAPI's metadata minting is `mint_invocation_metadata`; only parsing differs

- **Touches:** RFC 0077 §5.7, the `mint_invocation_metadata` row ("the fresh-`uuid7` blocks
  in MCP and FastAPI").
- **RFC said:** the function replaces FastAPI's block.
- **Found:** it does, exactly.
  [`InvocationMetadataMiddleware._decode_metadata`](../src/forze_fastapi/middlewares/invocation.py)
  minted a `uuid7` execution id, took `X-Correlation-ID` or a fresh `uuid7`, and took
  `X-Causation-ID` or nothing. Even the minting order matches, execution id first. What
  differs is parsing, which the function leaves to its caller: HTTP ignores a malformed
  header and falls back, while `forze ops` refuses a malformed `--correlation-id` (§5.4).
- **Because:** a header is advisory client input, and a script's argument is a bug when it
  is wrong. Both readings are the surface's, not the core's.
- **Class:** `discovery`. The parsing split only shows once the second consumer exists.
- **Consequence:** `_decode_metadata` keeps its name and its header parsing, and calls
  `mint_invocation_metadata`. No behaviour change.

## D-078 — FastAPI's API-key path keeps its blank check, then calls `authenticate_api_key`

- **Touches:** RFC 0077 §5.7, the `authenticate_api_key` row ("FastAPI's copies later"), and
  "As built (P1)", which made `api_key_credentials` public for this move.
- **RFC said:** the function replaces FastAPI's copy.
- **Found:** the split agrees: `_split_authorization(raw, sep=":")` and
  `api_key_credentials` strip, then split on the first colon, and take a key with no colon
  whole. The order differs. `authenticate_api_key` resolves the authn port and then refuses a
  blank key. FastAPI checks for a blank header first, and returns no identity when the
  ingress is not `required`, an outcome the core function does not have.
- **Because:** an optional header is an HTTP notion. The CLI and MCP always hold a key.
- **Class:** `spec-gap`. Knowable from
  [`resolvers.py`](../src/forze_fastapi/security/resolvers.py).
- **Consequence:** `_resolve_header_api_key_authn` keeps its blank and `required` check and
  then calls `authenticate_api_key`. A key past that check is never blank, so the two orders
  cannot be told apart. `_split_authorization` lost its `sep` parameter, since only the
  bearer split is left. Its colon cases left `test_authorization_split.py`, and
  `TestApiKeyCredentials` in `test_ingress.py` covers them. No behaviour change.

## D-079 — FastAPI's tenant ladder stays: it drops a tenant the ingress refuses

- **Touches:** RFC 0077 §5.7, the `resolve_principal_tenant` row ("FastAPI's copies later"),
  and "As built (P1)" (`tenant_request_unverifiable`, `tenant_mismatch`).
- **RFC said:** FastAPI's `resolve_tenant_identity` moves later.
- **Found:** the two agree only on the resolver call. Past it,
  [`resolve_tenant_identity`](../src/forze_fastapi/security/resolvers.py) does four things
  the ingress function does not:
  - it runs for an anonymous request, which has no principal;
  - it honours `trust_tenant_header`, the gateway fallback;
  - it drops an `X-Tenant-Id` that nothing vouches for, and the request runs with no tenant.
    The ingress function refuses the same request, with `tenant_request_unverifiable` when
    there is no resolver and `tenant_mismatch` when the resolver finds no tenant;
  - it ignores a malformed header, where the ingress takes a parsed `UUID`.
- **Because:** the ingress refusal was written for a caller who names a tenant on purpose,
  the CLI's `--tenant`. An HTTP client may send the header out of habit, and today that
  request succeeds.
- **Class:** `spec-gap`. §5.7 listed FastAPI's copy as a later move without comparing it.
- **Consequence:** left as is. Adopting the ingress function would turn requests that run
  with no tenant today into 401s and 400s, and close no gap: a dropped header binds nothing.
  Issue #666, where an ambiguous multi-membership principal resolves to no tenant, changes
  what the resolver returns and reaches both ladders unchanged. Nothing here touches
  `forze_identity/tenancy/`.
- **Proposed row (RFC 0077):** `ASSUMED` — HTTP keeps its own tenant ladder. The ingress
  refusal of an unverifiable requested tenant applies where the caller names the tenant
  explicitly, and an advisory `X-Tenant-Id` stays dropped, not refused.

## D-080 — FastAPI's route validation stays: moving it changes every 422 body

- **Touches:** RFC 0077 §2's first table row. It lists the MCP, Socket.IO, WS and agent-tool
  copies, but not FastAPI's
  [`validate_payload`](../src/forze_fastapi/routes/_attach.py).
- **RFC said:** nothing about FastAPI's copy.
- **Found:** `validate_payload(dto_type, data, op)` validates the DTOs that path, query and
  multipart endpoints assemble by hand; FastAPI validates a request body itself. It differs
  from `validate_operation_input` in three ways:
  - it takes a DTO type, because an endpoint builder holds no descriptor;
  - its rejection is `Invalid input for operation '<op>'`, with the default code
    `core.validation`;
  - it asks pydantic for no `url`, `input` or `ctx` before sanitizing. The ingress keeps
    each row's `url`.

  On unknown names they agree: FastAPI leaves them to the DTO's `extra` policy, which is
  `forbid_unknown=False`.
- **Because:** the copy predates the core and was shaped by the endpoint builders.
- **Class:** `discovery`. The `url` difference only shows when the two are compared.
- **Consequence:** left as is. Moving it would add a `url` to every error row of these 422s
  and close no gap, since both drop the input and the echoing messages.

## D-081 — FastAPI's sensitive refusal stays: the same rule, worded for routes

- **Touches:** RFC 0077 §2's second table row (`_attach.py:926`) and §5.2 ("now in one
  place").
- **RFC said:** a sensitive operation is refused on every generated surface, and the rule
  lives in one place.
- **Found:** `attach_operation_routes` already applies the same predicate as
  `refuse_sensitive` (`descriptor is not None and descriptor.sensitive`), at attach time.
  It runs inside the per-binding loop, after the path checks, and names the namespace.
  `refuse_sensitive` words its message `Refusing to register <surface>s`. Exposure itself does
  not map at all: routes are chosen by binding tables and an `include` set, not by
  `include_writes`.
- **Because:** HTTP has its own projection, so `select_operations` has nothing to select.
- **Class:** `spec-gap`.
- **Consequence:** left as is. Calling `refuse_sensitive` per binding would change only the
  refusal's text and drop the namespace from it. P1 made the same call for MCP's schema and
  resource-template refusals (`forze_mcp/schemas.py`, `resource_templates.py`), which keep
  their own wording.

## D-082 — FastAPI's bind-and-run does not map onto `invoke_at_ingress`

- **Touches:** RFC 0077 §2's third table row ("FastAPI middleware plus `_operation_runner`").
- **RFC said:** the route's bind-and-run is a copy of `invoke_at_ingress`.
- **Found:** HTTP binds the envelope in two middlewares, around the whole ASGI app. The
  invocation middleware binds metadata, idempotency and deadline, and echoes the ids in the
  response headers. The security middleware binds identity. `_operation_runner` only calls
  `run_operation`. `invoke_at_ingress` binds everything around one call, and it binds a
  `None` idempotency key on purpose, which would mask the key the middleware bound.
- **Because:** the envelope belongs to the whole request, not to one call: the ids go back in
  the response headers and sit in the log fields of everything the request runs.
- **Class:** `spec-gap`.
- **Consequence:** left as is.

## D-083 — The WS route ran commands with no invocation metadata; it now uses `invoke_at_ingress`

- **Touches:** RFC 0077 §2's third table row, "Mint metadata, bind identity, run", which
  lists the WS route (`forze_fastapi/realtime/ws.py:780`) among its copies.
- **RFC said:** the WS route is a copy of that sequence, so it mints metadata.
- **Found:** a `cmd` frame bound identity, the connection's admission, the idempotency key
  and the deadline, then called `run_operation`, but bound no `InvocationMetadata`. The
  middlewares pass the socket's scope through, so nothing ambient had one. Its logs and
  spans carried no execution or correlation id, and its integration events had none
  (`execution/outbox/enrichment.py`). That contradicts two docstrings: the module's ("the
  same frozen-registry discipline as HTTP") and the invocation middleware's, whose
  `allowed_websocket_paths` lists routes "which bind their own per-frame envelope".
- **Because:** the route was written without the metadata step, and nothing failed without
  it, because the readers treat missing metadata as `None`.
- **Class:** `drift`. §2 counted the route as a copy that mints metadata, and it was built
  otherwise. The route predates the RFC, so the RFC's account was wrong as well as the code.
- **Consequence:** adopted, as a **behaviour change** in its own commit, with a `Fixed`
  entry and a test that failed first (`test_realtime_ws_metadata.py`). The frame calls
  `invoke_at_ingress` with `mint_invocation_metadata()`, the connection's identity, its
  idempotency key and its deadline. The admission stays bound around the call, because the
  ingress has no parameter for it. It is not a security fix: no check depended on the
  metadata. A frame-supplied correlation id was not added.
- **Proposed row (RFC 0077):** `ASSUMED` — every governed invocation runs with its own
  `InvocationMetadata`, a duplex command frame included. A connection is not an invocation.

## D-084 — Realtime payload validation is typed by the route, not by the descriptor

- **Touches:** RFC 0077 §2's first table row (Socket.IO `_parse_payload`, the realtime WS
  route).
- **RFC said:** both are copies of `validate_operation_input`.
- **Found:** both call `RealtimeCommandRoute.parse_payload`, which validates against the
  route's own `payload_type` through a `TypeAdapter`. Any JSON value can arrive there, not
  only an object. The ingress function takes a descriptor and a mapping. Sanitizing is
  already shared: both use `sanitize_pydantic_errors` and keep their own codes
  (`realtime_invalid_payload`, `socketio.invalid_payload`).
- **Because:** a realtime route declares its wire type separately from the operation's input
  type, so one shared declaration serves both duplex transports.
- **Class:** `spec-gap`.
- **Consequence:** left as is on both surfaces.

## D-085 — The realtime identity and tenant ladder sits below the execution layer

- **Touches:** RFC 0077 §5.7 ("Integrations sit below that layer and cannot import it").
- **RFC said:** `ingress` lives in the execution layer.
- **Found:** both duplex transports authenticate through `resolve_realtime_identity` and
  `_tenant` in
  [`integrations/realtime/auth.py`](../src/forze/application/integrations/realtime/auth.py).
  The import-linter contract "Layered `forze.application` module architecture" puts that
  module below `execution`, so it cannot import `ingress`. The ladder is token-only and never
  takes a requested tenant. With none requested, `_tenant` answers what
  `resolve_principal_tenant` would.
- **Because:** the ladder is shared by the Socket.IO and WS resolvers, which is the reason it
  lives in the kernel.
- **Class:** `discovery`.
- **Consequence:** left as is. The ladder's answers agree, so resolution owes no parity
  work. Whether each command then *binds* the resolved tenant is a separate question, and
  Socket.IO did not (D-087).

## D-086 — Socket.IO's events now mint metadata; the run cannot reach `invoke_at_ingress`

- **Touches:** RFC 0077 §2 ("Socket.IO's event path … calls the resolved operation
  directly, not through `run_operation`") and §12 P3.
- **RFC said:** Socket.IO moves onto `ingress` later.
- **Found:** `ForzeSocketIOAdapter` takes an `operation_resolver`, documented as
  `registry.resolve`, and calls the handler it returns. That handler runs the full plan:
  authz, tenancy, read-only enforcement and deadline. It skips `run_operation`'s trace
  records and binds no metadata. The adapter never holds a registry, and
  `invoke_at_ingress` needs one.
- **Because:** the adapter's public shape predates the registry entry point.
- **Class:** `discovery`.
- **Consequence:** split in two.
  - **The metadata is adopted**, as a **behaviour change** in its own commit, under the same
    `Fixed` entry as D-083, with a test that failed first (`test_routing_metadata.py`).
    `_dispatch_event` binds `mint_invocation_metadata()` with `bind_metadata` around the
    resolved handler's run, beside the session identity and the admission, so the two duplex
    transports agree on metadata (the tenant is D-087). The `operation_resolver` API is
    unchanged. The event's
    metadata replaces any bound around it for the event's duration; a handler that binds its
    own inside still wins.
  - **The bind-and-run move stays demand-gated.** It needs a public API decision, such as a
    `registry=` on the adapter, which is not a refactor.
- **Proposed row (RFC 0077):** `ASSUMED` — Socket.IO moves onto `invoke_at_ingress` only
  with a registry-backed adapter, on a named consumer, as the rest of P3 is. Until then it
  mints each event's metadata as the WS route does.

## D-087 — Socket.IO's command events bound no tenant; they now bind the connection's

- **Touches:** RFC 0077 §2 ("Socket.IO's event path … binds identity but no
  `InvocationMetadata`"). Unlisted: the tenant.
- **RFC said:** the event path binds identity, and nothing about the tenant.
- **Found:** `attach_realtime_connection` resolves the principal and the tenant at connect,
  and stores the connection on the session under `CONNECTION_SESSION_KEY`. `_dispatch_event`
  bound only `bind_identity(authn=...)`, which sets the tenant to `None`. So a tenant-aware
  operation on a tenanted connection failed `tenant_required`. Replay and ack on the same
  connection did bind the tenant (`_bind_connection`), and the WS route binds it per frame.
  The branch's review found this, not the inventory: D-085 compared how the tenant is
  resolved, not whether it is bound.
- **Because:** the routing module predates `attach_realtime_connection`, and its docstring
  left tenant resolution to the `context_factory`.
- **Class:** `spec-gap`.
- **Consequence:** fixed, as a **behaviour change** in its own commit, with its own `Fixed`
  entry and a test that failed first (`test_command_tenant.py`). The event binds the stored
  connection's tenant with the identity. `CONNECTION_SESSION_KEY` moved to `routing` to
  avoid a circular import, and the package still exports it. A connection authenticated by
  an `identity_resolver` stores no connection, so its events still bind no tenant. No
  security fix is claimed: tenant-aware stores and `TenantRequired` fail closed without a
  tenant.
- **Proposed row (RFC 0077):** `ASSUMED` — a duplex command binds the identity and the
  tenant its connection resolved, on every transport.

**Out of scope, noted.** Neither duplex transport checks its command table against the
catalog at attach time, so an unknown operation raises `internal` at the first call. Neither
refuses a sensitive operation. These are hand-declared routes, which the sensitive rule (§5.2,
generated surfaces) does not cover, and no copy of a boundary step is involved.

---

---

# Unit 10 · RFC 0077 P3 — CLI items

Branch `feat/ops-cli-p3`. RFC 0077 (operation CLI, ✅ Complete for P1 and P2) §12 P3, the
three `forze ops` items the maintainer named a demand for on 2026-10-27: typed flags for flat
scalar inputs, a JSONL batch in one runtime scope, and shell completion of operation names.
The FastAPI / Socket.IO / WS moves onto `ingress` (Unit 9), the boundary-wide
`commit_ambiguous` question (Unit 11) and the surface field (RFC 0079) are their own changes.

**Drift count: 0.** Seven entries, all `spec-gap`: §12 named the three items and §5.9 and §8
gave them one sentence each, so every rule below was knowable before code and none was
settled. One entry amends a graded row (decision 15); three are close-outs for the `LOCKED`
rows a batch touches (decisions 9, 10 and 12), folded into D-092 and D-093.

## D-088 — Decision 15 gives way to §12's demand gate for three items

- **Touches:** RFC 0077 decision 15 (`ASSUMED`): "No typed flags, JSONL batch, remote mode, or
  surface field in this RFC." And §12, whose P3 list names the same items as demand-gated.
- **RFC said:** both, and they read differently: decision 15 keeps the items out of the RFC,
  while §12 keeps them in it, gated on a named consumer.
- **Found:** the maintainer named the consumer for three of them. Building them as P3 of this
  RFC follows §12 and contradicts decision 15 as worded.
- **Because:** decision 15 was written for the first version, and its "in this RFC" says more
  than that.
- **Class:** `spec-gap`.
- **Consequence:** built here, recorded in §5.8 "As built (P3)". Remote mode and the surface
  field stay out.
- **Proposed row (RFC 0077):** amend decision 15, `ASSUMED` — no remote mode or surface field in
  this RFC. Typed flags for flat scalar inputs, the JSONL batch and shell completion shipped in
  P3 on a named demand.

## D-089 — Typed flags: flat scalars only, all or none, validated as strings

- **Touches:** RFC 0077 §5.9 and §8, which defer "the flat-scalar case … using the flat
  signature MCP already synthesises"; decision 13 (`ASSUMED`); decision 5 (`LOCKED`). The
  parse model, the scalar set and option collisions are unlisted.
- **RFC said:** typed flags for a flat-scalar input, from MCP's flat signature.
- **Found:** MCP's signature is one parameter per field of any annotation, and FastMCP derives
  the schema from it, so it maps a list or a model as readily as a string. It is also behind
  the `mcp` extra (§5.9). So
  [`flags.py`](../src/forze_cli/ops/flags.py) checks each field itself. With `Annotated` and
  `Optional` stripped, a field must be `str`, `int`, `float`, `Decimal`, `UUID`, `date` (so
  `datetime`), an `Enum` of strings, an `IntEnum`, or a `Literal` of strings; a `bool` is a
  `--name` / `--no-name` switch. Three shapes came back from probing pydantic: a non-string
  `Literal`, like a plain enum with non-string values, never validates from a string (`"2"` is
  not `Literal[2]`, `"1"` not `Code.ONE = 1`), while an `IntEnum` coerces as `int` does; and a
  union of two scalars keeps a string a string. The first two and the union are out.
- **Built:** Typer's options are static, and the flags depend on the operation, which Click
  has not read when it parses. `call` sets `allow_extra_args` and `ignore_unknown_options` and
  parses `ctx.args` against the field table. Click claims `call`'s own options first, so a
  `timeout` field could never be set by `--timeout`. Refusing the call option at run time would
  change P2 behaviour, and prefixing every flag would make every flag worse. Instead, an input
  takes no flags at all when a field is spelled like a call option (`CALL_OPTIONS`, which a
  test pins to the command) or two fields share a spelling (`no_x` beside a switch `x`). A
  call option then never changes meaning with the operation.
  - Values stay the strings typed. The raw object goes through
    `validate_operation_input(..., forbid_unknown=True, by_name=True)`; `by_name` because the
    flags spell field names, and an alias-only field would otherwise be refused as unknown.
  - Decision 13 is extended, not departed from: an input declaring a secret takes no flags,
    and flags given to it are refused as `cli_inline_secret`. Decision 5 concerns the
    identity's key and still holds: `call`'s own options carry none
    ([`test_ops_call.py`](../tests/unit/test_forze_cli/test_ops_call.py),
    `test_no_option_takes_a_key_on_argv`).
- **Because:** a partial mapping makes the contract depend on each input's shape, the reason
  §5.9 gave for deferring. All or none keeps the dependence to one question that `describe`
  answers.
- **Class:** `spec-gap`.
- **Consequence:** `describe` lists the flags or says why there are none (`typed_flags` in
  `--json`). A value that starts with `--` needs `--field=VALUE`; a bare one is refused,
  since a forgotten value would swallow the next flag, a misspelt one included. A float flag accepts `nan` exactly as `--input` accepts the
  string `"nan"`: flags match `--input`'s string form, and only the JSON literal `NaN` is
  refused there. Values on argv are visible to `ps` and shell history; the docs send any
  sensitive value that is not a declared secret to `--input @PATH`.
- **Proposed row (RFC 0077):** `ASSUMED` — an input takes typed flags only when every field is a
  scalar or an optional one, it declares no secret, and no flag shares a spelling with a call
  option or another flag. Otherwise it takes none. The flags build the raw object for the
  shared validation, read by field name.

## D-090 — An unknown option after `OP` is an error object, not Click's usage text

- **Touches:** RFC 0077 §5.5, whose first table row makes argument parsing Click's usage text
  on stderr with nothing on stdout; decision 8 (`ASSUMED`).
- **RFC said:** a parse error exits 2 with Click's usage text.
- **Found:** under D-089's parse model, Click hands every option it does not know to the
  command, a misspelt call option included. Only the loaded declaration can tell a typo from a
  typed flag, and once it has loaded, decision 8 owes an error object.
- **Class:** `spec-gap`. It follows from the parse model, which was a design choice.
- **Consequence:** still exit 2, now as `cli_malformed_flags`, `cli_flags_unsupported` or
  `cli_input_not_accepted`. The last two name the first flag ("`--timout` is no call option
  either"), so a typo is recognisable. A malformed value of a known option is still Click's
  usage text. A script matching exit 2 sees no change; stdout is no longer empty there.
- **Proposed row (RFC 0077):** `ASSUMED` — an option `call` does not have is read as a typed
  flag, and its refusal is an error object with exit 2.

## D-091 — `--batch` writes one JSON line per call

- **Touches:** decision 8 (`ASSUMED`): exactly one JSON document on stdout. §8 named a batch
  mode ("many inputs, one scope") and nothing else.
- **RFC said:** one document per invocation.
- **Built:** with `--batch`, stdout is JSONL, one line per input line that ran, in order, each
  flushed as its call finishes. A line is `{"line": N, "result": …, "execution_id": …,
  "correlation_id": …}` or the error object with `"line": N`.
  - The result is nested under `result` because the exit code no longer says whether a given
    line succeeded.
  - `line` is the physical input line, blank lines skipped, so a reader can resume from it.
  - Every line has its own `execution_id`. All share the batch's `correlation_id`, the one
    given or one minted, and the `--causation-id` if given.
  - A refusal of the whole batch is one error object without `line`: a conflicting option, an
    unreadable file, a runtime that fails to build or start, or a signal during startup.
- **Class:** `spec-gap`.
- **Consequence:** decision 8 holds unchanged without `--batch`.
- **Proposed row (RFC 0077):** `ASSUMED` — with `--batch`, stdout carries one JSON line per call
  that ran, numbered by its input line, its value under `result` or `error`. A refusal of the
  whole batch is one error object without a line number.

## D-092 — A batch is validated whole, stops at its first failure, and takes no key

- **Touches:** decisions 9 and 12 (`LOCKED`); §5.6 step 3 ("None of this builds a runtime").
  Unlisted: when lines are validated, stop or continue, the batch's exit code, the key.
- **Built:**
  - Lines end at `\n` only, with one trailing `\r` dropped: `str.splitlines` also splits at
    U+2028 and U+0085, which JSON allows raw inside a string.
  - Every line is read and validated before the runtime is built, as one call's input is. A
    malformed or invalid line is the only line written, and nothing runs, so a typo on line
    900 cannot leave 899 lines applied. The whole input is held in memory to allow that; a
    `shortcut:` comment in `_batch_calls` names the limit.
  - The first failure stops the batch, and its exit code, computed as for one call, is the
    batch's. There is no continue-on-error flag: nobody asked for one, and stopping is the safe
    default when later lines may depend on earlier ones.
  - `--idempotency-key` is refused as `cli_option_conflict`. One key would replay the first
    line's result for every identical line and refuse every other line as a conflict. A
    per-line key needs an envelope around each input, so it is left for a named demand.
    `--pretty` is refused too: an indented document is not a line.
- **Because:** the RFC named the mode and none of its rules.
- **Class:** `spec-gap`.
- **Consequence:** without a key, a batch is repeatable only when its operation is a query. A
  retryable failure exits 75 from a query batch and 69 from a command batch. Close-out for
  decision 9 (`LOCKED`): 75 still means safe to repeat as is, since re-running a query batch
  repeats only queries, and a command batch can never exit 75. Close-out for decision 12
  (`LOCKED`): compliant; under `--batch` the key is refused outright.
- **Proposed row (RFC 0077):** `ASSUMED` — a batch validates every line before building the
  runtime and stops at the first failure with that line's exit code. It refuses
  `--idempotency-key` and `--pretty`.

## D-093 — A signal stops a batch after the line in flight

- **Touches:** decision 10 (`LOCKED`) and §5.6's two-signal shape, written for one call.
- **Built:** the shape applies to the line in flight. The first signal arms the grace timer;
  the line finishes with its own document, or a second signal or the expired grace cancels it
  into `interrupted` or `commit_ambiguous`, exactly as one call. No line starts after a
  signal. With lines left unrun, the batch exits 128 plus the signal number. When the signal
  came during the last line and that line finished, the batch exits by its own outcome, as one
  call does. Each line resets the commit flag on entry, so an earlier line's commit never reads
  as the current one's. Each line also enters and leaves its own metadata, identity, tenant,
  idempotency and deadline bindings, and verifies its key again: a key revoked by line 1 refuses
  line 2, and nothing stays bound after the batch (`test_ops_batch.py`).
- **Class:** `spec-gap`.
- **Consequence:** close-out for decision 10 (`LOCKED`): a line cancelled after its commit
  point still reports `commit_ambiguous`, and no line is reported safe when its commit may
  have landed. The exit code alone says whether lines went unrun; stdout says which ran.
- **Proposed row (RFC 0077):** `ASSUMED` — a signal stops a batch after the line in flight,
  which gets one call's grace and cancellation. With lines left unrun, the batch exits 128
  plus the signal number.

## D-094 — Completion loads the declaration quietly and never builds a runtime

- **Touches:** decision 11 (`ASSUMED`): `list` and `describe` never build a runtime. Completion
  itself is unlisted.
- **Built:** `OP` completes after `describe` and `call` through Typer's `autocompletion`, from
  `--app` (when it comes before `OP`), `FORZE_OPS_APP`, or the declaration `build_ops_cli`
  bound. Loading runs with `sys.stdout`, `sys.stderr` and descriptors 1 and 2 discarded, and
  any failure, `SystemExit` included, completes nothing: the shell reads stdout as candidates
  and prints stderr under the prompt. The root app already
  offered Typer's `--install-completion` and `--show-completion`, and keeps them.
- **Class:** `spec-gap`.
- **Consequence:** every TAB imports the application module, as `list` does, so its import side
  effects run at completion time too (§9, "Import side effects").
- **Proposed row (RFC 0077):** `ASSUMED` — completion loads the declaration as `list` does,
  builds no runtime, and offers nothing, silently, when the declaration cannot load.

## Review findings — 2026-10-28

PR #692. Three findings from CodeRabbit and CodeAnt, all fixed. R-1 amends D-089's
consequence in place; no proposed row changes.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | Medium | A flag that needs a value took the next token unless it was one of the input's flags, so `--title --tilte` ran the operation with the title `--tilte` (CodeRabbit) | Fixed — any next token that starts with `--` is refused; `--title=--tilte` still spells that value |
| R-2 | Medium | A batch of blank lines built the runtime and ran its startup and shutdown with nothing to call (CodeAnt) | Fixed — an empty batch exits 0 after the key and the runtime profile are checked, and never enters the runtime's scope |
| R-3 | Low | The app-author skill said every flat-scalar input takes typed flags, though a secret field or a colliding spelling disables them (CodeAnt) | Fixed — it says "usually" and points at `describe` |

Each fix was checked against its own removal.

---

# Unit 11 · RFC 0077 P3 — commit ambiguity at every boundary

Branch `feat/commit-ambiguous-boundaries`. RFC 0077 §12 P3, the last code item: whether a
cancellation after the commit point should report `commit_ambiguous` at every boundary, not
only the CLI (§5.6). The decision is recorded in §5.6 "As built (P3)".

**Drift count: 0.** Three entries: two `discovery`, one `spec-gap`. The RFC left the question
open on purpose, so the decision itself is an entry rather than a departure.

**Already true, so no entry.** A cancel that lands inside a transaction's commit, or inside
its shielded post-commit drain, was already converted to `commit_ambiguous` by the
transaction scope on every surface (§5.8 P2 note). The open window was only a cancel after
the scope had exited: in an outer hook, a dispatched operation, or after a nested operation's
root transaction committed.

## D-095 — Every boundary records a late cancel; only the CLI and the saga executors convert it

- **Touches:** RFC 0077 §5.6 (last paragraph), §12 P3, decision 10.
- **RFC said:** reading the flag at the CLI boundary "keeps the CLI correct without changing
  how cancellation behaves everywhere else", and whether other boundaries need the same check
  is left to §12.
- **Found:** each other boundary has a reason to keep the raw cancel.
  - HTTP routes, MCP, Socket.IO and WS commands are cancelled by a client that left or by a
    server shutdown. No one is left to read a converted error, and uvicorn's shutdown and the
    receivers' task groups need the cancel to propagate.
  - The durable runner, Temporal and Inngest read the raw cancel to name its cause.
  - The saga executors already convert, at their own boundary, because they own the journal.

  So the only safe change is to make the outcome visible. `ResolvedOperation.__call__`, the
  top-level boundary every surface reaches, now logs a `commit_ambiguous` warning and adds a
  `forze.commit_ambiguous` span event when a cancel arrives with the flag set. It then
  re-raises the cancel unchanged. Socket.IO and Inngest call the resolved operation without
  `run_operation`, so the record could not live in `run_operation` itself.
- **Because:** the CLI can convert because it owns both its process and its result channel.
  No other boundary owns both.
- **Class:** `discovery`. The answer depended on each boundary's cancel semantics, which the
  RFC deliberately did not survey.
- **Consequence:** the P2 contract is unchanged: the flag survives the cancel, every reader
  resets it on entry, and the CLI keeps converting. A nested operation records nothing, and
  its top-level caller records once.
- **Proposed row (RFC 0077):** `LOCKED` — a cancel after the commit point is converted to
  `commit_ambiguous` only by a boundary that owns its process and result channel (the CLI)
  or its journal (the saga executors). Every other boundary records it at the top-level
  operation boundary, with a warning log and a span event, and re-raises the cancel.

## D-096 — The durable runner cannot read the commit flag; the record has to be emitted inside the body

- **Touches:** RFC 0077 §5.6. Unlisted: boundaries that run the operation on another task.
- **RFC said:** nothing. The CLI pattern assumes the boundary reads `commit_started()` on the
  task the operation ran on.
- **Found:** the self-hosted durable runner runs the body with `asyncio.ensure_future`, so the
  body gets a copy of the context. A flag the body's transaction sets never reaches the
  runner's frame. The runner names a cancel's cause by which watcher fired: a lease reclaim,
  an operator cancel, or `max_run_duration`. Any other cancel, a drain's included, propagates
  raw and writes no terminal state. The row stays `RUNNING`, and recovery re-claims it. That
  re-run can apply a committed operation twice, so ambiguity matters more here than on any
  request surface.
- **Because:** `ContextVar` writes on a child task stay on the child.
- **Class:** `discovery`.
- **Consequence:** a reader at each surface, as the CLI does it, cannot work here. This is why
  the record lives at the operation boundary, which runs on the body's own task. Making the
  re-run safe stays the job of an idempotency key, an inbox, or a saga journal.
- **Proposed row (RFC 0077):** none. D-095's row covers it.

## D-097 — A read-only root transaction marks the commit point too

- **Touches:** RFC 0077 §5.6 and decision 10. Also `TransactionContext.scope` and the
  deadline classification in `ResolvedOperation._run`.
- **RFC said:** "a cancel after the commit point reports `commit_ambiguous`", on the reading
  that a commit may have landed.
- **Found:** the scope called `mark_commit_started()` for every root transaction, read-only
  ones included (`src/forze/application/execution/context/transaction.py`, at the end of the
  root's body). Probed: after `tx.scope(route, read_only=True)` exits, `commit_started()` was
  `True`. So a query that is cancelled after its read reported `commit_ambiguous` on the CLI
  (exit 1, not 130 or 143). A query whose deadline fires after its read got the same report,
  and so did the new record. That error is not retryable, so a caller refused to retry a
  read.

  A plain gate on `read_only` would not be safe, because a `read_only` root does not always
  stop a write. Each manager that reports `TxCapabilities`, with its evidence:
  - **Postgres** enforces it only for a root `BEGIN … READ ONLY`
    (`src/forze_postgres/adapters/txmanager.py:115-126`). Integration tests pin the refusal
    (`test_pg_txmanager.py::test_transaction_per_call_read_only_rejects_writes`,
    `test_pg_transaction_options_leak.py::test_read_only_tx_still_rejects_writes`).
    Inside a client transaction opened outside forze, the client turns the root into a
    savepoint. A savepoint cannot be `READ ONLY`, and a write there lands
    (`test_pg_txmanager.py::test_read_only_is_enforced_only_by_a_read_only_begin`).
  - **`MockStrictTxManagerAdapter`** and **`MockJournalTxManagerAdapter`**, the mock's
    default, enforce it for a root they open. Each sets the read-only flag only at its own
    root (`tx.py:231`, `tx.py:350`), and every participating store's write checks it through
    `ensure_mock_tx_writable` (`tx.py:78`), raising `read_only_tx`. A scope nested in an outer
    read-write transaction inherits that transaction's flag, so a write there lands.
  - **`MockTxManagerAdapter`** (`transactions="none"`) is a no-op and lets the write through.
  - **Mongo** accepts the flag and does not enforce it
    (`src/forze_mongo/adapters/txmanager.py:73-76`).
  - **Firestore** does the same (`src/forze_firestore/adapters/txmanager.py:63-65`).
  - **Neo4j** does the same (`src/forze_neo4j/adapters/txmanager.py:75-77`).

  No other manager in `src/` reports `TxCapabilities`.

  The forze-level guard does not close the gap. `_refuse_if_read_only`
  (`src/forze/application/contracts/deps/keys.py:41-45`) checks whether the *operation* is
  read-only (`QUERY`), not the transaction. Its own docstring (`keys.py:64-66`) says
  `forze.testing.unguarded`, direct resolution and `backend_of` all bypass it. Two writes
  therefore commit unrefused on Mongo, Firestore or Neo4j, and inside a savepoint root:
  - a command that opens `ctx.tx_ctx.scope(route, read_only=True)` and writes through its
    command port, which nothing guards outside a `QUERY`;
  - a `QUERY` that reaches a write port through one of those bypasses.

  If the mark skipped those roots, a deadline or a cancel after either write would come back
  as `deadline_exceeded` or `interrupted`, although the write landed. That is the duplicate
  decision 10 forbids. In a real read-only `BEGIN`, or a mock root opened read-only, the
  store refuses the write whatever path reached it.
- **Because:** the mark predates any reader that cared which transactions can write.
  `read_only` is a hint on four of the seven managers that report capabilities, and on the
  other three it is enforced only for a root they open themselves.
- **Class:** `spec-gap`. Both the scope and the managers were readable when §5.6 was written.
- **Consequence:** implemented, fail-closed, as a question about the transaction actually
  opened. A new opt-in protocol, `ReadOnlyAware`, has one method, `read_only_enforced()`
  (`src/forze/application/contracts/transaction/ports.py:217`). The scope asks it just
  before the commit and skips the mark only when four things hold (`transaction.py:463`):
  - the root is read-only;
  - it is the only route a finished nested scope entered;
  - no after-commit callback is queued;
  - the answer is exactly `True`.

  Skipping the mark also skips the commit-time `commit_ambiguous` conversion. The answers:
  - **Postgres** answers from a per-task record in the kernel client
    (`kernel/client/read_only_root.py`). The manager opens the record only for a root
    `BEGIN … READ ONLY` (`txmanager.py:138`), and a nested savepoint shares it.
  - **The strict and journal mocks** answer from the flag `ensure_mock_tx_writable` refuses
    on (`tx.py:193`, `tx.py:298`).
  - **Mongo, Firestore, Neo4j and the no-op mock** do not implement the protocol, so they
    mark as before.

  On Postgres and the default mock, a query whose read-only root is the only transaction
  it entered now reports `deadline_exceeded` (CLI exit 6) or `interrupted` (130 or 143).
  Every other case still reports `commit_ambiguous`.

  How the design got here, all before publish:
  - **Manager-wide flag.** The first cut put `TxCapabilities.read_only_enforced` on the
    manager. Review found that it claimed enforcement for a savepoint root, so it became
    the per-transaction question.
  - **Nested root on another route.** A fresh-eyes review found two more ways a write could
    commit while the answer stayed yes. Here a nested scope on a second route with the same
    scope key (two Postgres clients under `"postgres"`) calls that manager's
    `transaction()`. That client is not in a transaction, so it opens an independent
    `BEGIN` and commits, and nested scopes never mark. The fix: the root records every
    route a nested scope enters (`transaction.py:380`, `:422`), and skips only while it
    holds the root's own route alone.
  - **`detached()` statements.** `PostgresClient.detached()` resets the client's depth, so
    its statements autocommit beside the root. The fix: `detached()` clears the record on
    entry (`client.py:693`). So does any write-capable root opened by any Postgres client
    (`client.py:899`), which also covers the nested-route case for Postgres.
  - **Autocommit on another client.** A statement on another client outside any
    transaction autocommits beside the root. The fix: both of the client's
    out-of-transaction connection paths clear the record before they check out a pooled
    connection.
    - `_statement_conn` serves every single-statement method in autocommit
      (`client.py:631`).
    - `__acquire_conn` serves `fetch_all_batched`, which wraps an out-of-transaction scan
      in a short-lived transaction of its own and commits it, and `health`
      (`client.py:589`).

    A query that also runs such a read loses only the skip.
  - **Shared record.** The record is one per task and shared by every client, so a bypass
    on any client, in a nested scope or a child task, clears the root's own claim.

  Pinned by:
  - a mock test that ties each mock's answer to whether a write in its read-only root is
    actually refused, at the root and nested in an outer transaction;
  - Postgres unit tests, with a fake client, for a root `BEGIN`, a savepoint, a read-write
    root, a `detached()` scope (at the root and from a nested scope), and an autocommit
    statement on a second client;
  - Postgres integration tests: a savepoint root lets a write land, and a `detached()`
    insert, a second client's transaction, or its autocommit insert clears the claim;
  - a core test where a nested scope on another route commits an independent transaction,
    and a cancel in the read-only root's commit window reports `commit_ambiguous`;
  - core and CLI tests that assert the reported code (`deadline_exceeded`, `interrupted`,
    `commit_ambiguous`) on the deadline path and under a signal.

  Removing the skip fails eight of those tests. Removing only the route check fails the
  nested-route test.

  Still outside the claim, as for any transaction:
  - a write to another store (Redis, a queue, a blob);
  - `dblink` or `postgres_fdw`.

  Neither ever joined the transaction the commit mark covers.
- **Proposed row (RFC 0077):** `ASSUMED` — a read-only root marks no commit point only
  when three things hold:
  - its manager implements `ReadOnlyAware` and answers exactly `True` for the transaction
    it actually opened;
  - no nested scope on another route finished its body;
  - no after-commit callback is queued;
  - for Postgres, no `detached()` statement, autocommit statement, `bound_connection()` or
    other write-capable root ran beside it.

  Every other root marks one. A manager may answer `True` only if its own store refuses
  every write to that transaction on every path. Follow-up, not built: make the Mongo,
  Firestore and Neo4j write adapters refuse writes inside a read-only root, as the mocks
  do, so they can implement the protocol too.

## Review findings — 2026-10-28

PR #693. Three findings from CodeRabbit and CodeAnt, all fixed, plus the Codecov patch check,
then four more from CodeRabbit's second pass (R-5 to R-8), all fixed. R-1 and R-3 add
conditions to D-097's summary and proposed row, both amended in place.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | Medium | A read-only root skipped the mark while an after-commit callback was queued. The callback runs after the root exits and may write, so a cancel in the drain came back retryable after that write (CodeRabbit, CodeAnt) | Fixed — a queued callback keeps the mark |
| R-2 | Medium | `bound_connection()` on another client never cleared the read-only record, though the pool commits the connection's statements when it goes back (CodeRabbit) | Fixed — it clears the record before checking out the connection |
| R-3 | Low | A nested scope on another route was recorded on entry, so one whose body failed and rolled back still cost the root its skip when the root's body caught the error (CodeAnt) | Fixed — the route is recorded once the nested body succeeds, before its commit, so a torn commit still counts |
| R-4 | — | Patch coverage: the record's fallback for a generator finalized in another context, and a `None` branch for the route set that a nested scope never takes (Codecov) | A unit test covers the fallback; the branch is gone, since the root sets the route set with the depth |
| R-5 | Medium | A read-only root opened directly on a second Postgres manager, inside the first, replaced the first's record. A `detached()` write inside it cleared only the inner record, so the outer root still answered `True` (CodeRabbit) | Fixed — the record holds one box per open root, and a bypass clears all of them |
| R-6 | Low | Before `configure_logging`, the fallback renderer did not merge the bound context, so the `commit_ambiguous` warning, and every other, lost the invocation's ids (CodeRabbit) | Fixed — the fallback chain merges it after the level filter, as the configured chain does |
| R-7 | Low | A logger that raised skipped the span event too (CodeRabbit, unverified) | Fixed — the two records are attempted separately |
| R-8 | Low | D-095's heading named only the CLI as converting, though the saga executors convert too; D-097's summary listed three conditions where the row has four (CodeRabbit) | Fixed in place |

Each fix was checked against its own removal.

---

# Unit 12 · RFC 0078 — a DuckLake per tenant (issue #800)

Branch `feat/duckdb-ducklake-catalog`. Issue #800 asks for a DuckLake variant of the tenant
catalog: a Postgres metadata schema read with the tenant's own role, and a local or `s3://`
data path, built by a fixed sequence and attested like the Iceberg one, on DuckDB 1.5. The
issue's own probe (DuckDB 1.5.6, `ducklake` ac7595b0, `postgres_scanner` 318dabb) is the
design this unit is measured against, beside RFC 0078 §3.4 and §3.5. Every probe below ran on
both builds, `duckdb==2.0.0` and `duckdb==1.5.6`, against Postgres 18 and RustFS, except
where a function exists on one build only, as D-101 says.

**Drift count: 0.** Seven entries, D-098 to D-104, all `discovery`. Product code changes in
`routed_client.py`, which gains `DuckLakeTenantCatalog`, `DuckLakeCatalogCredentials`, the
DuckLake sequence and its attestation, and picks a tenant's secret shape from its catalog;
the two package `__init__` files export the new types.
The Iceberg sequence is unchanged.

**Where each item lives.** The sequence and its checks: `_ducklake_session_statements`,
`_ducklake_expected_settings`, `_ducklake_outside` and `_ducklake_problems`. The battery:
`tests/integration/test_forze_duckdb/test_ducklake_routed_reads.py`, over
`tests/support/ducklake.py`. Its `TestEngineFacts` pins D-098 and D-101 to D-103.

**Review before publishing.** A fresh-eyes review of the branch raised eleven findings; the
table at the end of the unit records each. Two reported holes were probed and refuted, and
D-104 is the one fact that probing them found.

## D-098 — Persistent secrets stay off only because the password is already in memory

- **Touches:** §3.4 (`allow_persistent_secrets: false` at connect) and the issue's sequence,
  which does not set it.
- **RFC said:** persistent secrets are off at connect, so none loads from `secret_directory`.
- **Found:** with them off, an attach whose password is in the connection string fails on both
  builds: `Unknown secret storage found: 'local_file'`. The `postgres` extension finds no
  secret in memory and asks the persistent storage, which is not there. With an unnamed
  `postgres` secret created first, the same attach succeeds, and keeps working through forced
  pool reconnects and later snapshots.
- **Because:** the extension's secret lookup falls through to `local_file` only when memory
  holds no match.
- **Class:** `discovery`.
- **Consequence:** the sequence creates the unnamed secret before `ATTACH` (D-099), and
  persistent secrets stay off and attested, as in the Iceberg profile. A test removes the
  secret and puts the password back in the string; the build fails with this message.
- **Proposed row (RFC 0078):** `ASSUMED` — a DuckLake session keeps persistent secrets off;
  the unnamed `postgres` secret, created before `ATTACH`, is what makes that possible.

## D-099 — The password goes in the unnamed `postgres` secret, and the attach string is bare

- **Touches:** the issue's first open question ("the metadata password is visible to
  statements").
- **Issue said:** with the password in the attach string, `duckdb_databases()` returns it,
  and no secret-based attach worked (`METADATA_PARAMETERS` with `SECRET`, `TYPE ducklake`).
- **Found:** `CREATE SECRET (TYPE postgres, HOST, PORT, DATABASE, USER, PASSWORD)` with no
  name, then `ATTACH 'ducklake:postgres:' …`, attaches on both builds. `duckdb_databases()`
  reads the path back as `postgres:`, and `duckdb_secrets()` shows `password=redacted`.
- **Because:** the extension uses its default secret for an attach that names none.
- **Class:** `discovery`.
- **Consequence:** the sequence attaches this way. The attestation checks that the lake's path
  is exactly `postgres:`, so a password, a host or anything else in the string fails it. A
  build error is redacted of the password and of the store key.
- **Proposed row (RFC 0078):** `ASSUMED` — the metadata password lives only in the unnamed
  `postgres` secret, and the attestation reads the attach string back as bare `postgres:`.

## D-100 — `ATTACH` appends four working-directory paths to `allowed_paths`

- **Touches:** §3.4's allowlist, which names everything a statement may open.
- **Found:** after the DuckLake `ATTACH`, `allowed_paths` also holds `<cwd>/postgres:` and the
  same with `.wal`, `.wal.checkpoint` and `.wal.recovery`, on both builds. A later
  `SET GLOBAL allowed_paths=…` leaves them in place. `COPY … TO 'postgres:'` then writes a
  file in the working directory. With the password in the string, each entry carried it.
- **Because:** the engine allow-lists an attached database's files, and reads the metadata path
  as a file relative to the working directory.
- **Class:** `discovery`.
- **Consequence:** the attestation expects exactly those four entries beside the CA bundle, so
  any other path fails it. The docs ask for a read-only working directory.
- **Proposed row (RFC 0078):** `ASSUMED` — the DuckLake allowlist attests the four paths
  `ATTACH` adds; a read-only working directory is the deployment's control over them.

## D-101 — On DuckDB 1.5, `postgres_scan` and `postgres_attach` connect past external access

- **Touches:** §3.5 "No arbitrary HTTP egress" and "No ambient credentials", and the issue's
  probe table, which lists neither function.
- **Found:** on 1.5.6, with `enable_external_access=false` and the configuration locked,
  `postgres_scan('<connection string>', schema, table)` and `CALL postgres_attach(…)` open
  connections. One read another tenant's metadata with that tenant's password, and one
  tried `127.0.0.1:1`. libpq also reads `PGPASSWORD`, `PGPASSFILE` and `~/.pgpass`. No
  setting turns the functions off. DuckDB 2's extension has neither function. The session's
  unnamed secret does not ride along: a connection string without a password fails with
  `fe_sendauth: no password supplied`, and an empty one goes to the default socket.
- **Because:** the 1.5 extension checks external access in `ATTACH` only.
- **Class:** `discovery`.
- **Consequence:** a dynamic read already refuses both, as table functions outside its list
  (D-072) and as a `CALL`. Analytics SQL is the application's. The docs say to run the worker
  with no `PGPASSWORD`, `PGPASSFILE` or `PGSERVICEFILE` set and no readable `~/.pgpass`, and
  to let egress reach only the metadata database and the store. A test pins each build's answer for both functions, and that the secret stays out.
- **Proposed row (RFC 0078):** `ASSUMED` — on DuckDB 1.5 the engine does not bound Postgres
  egress from a DuckLake session; the dynamic-read classification and the deployment's
  egress policy do.

## D-102 — A statement can query the metadata database as the tenant's role

- **Touches:** the issue's second open question (the role's grants as the bound) and §3.5.
- **Found:** on both builds, `postgres_query('__ducklake_metadata_lake', …)` runs any read-only
  SQL as the role, and `__ducklake_metadata_lake.<schema>.<table>` reads the metadata tables,
  though `duckdb_databases()` does not list that catalog. Both run in a read-only
  transaction: `postgres_execute` of a `CREATE` is refused. Another tenant's schema is
  refused by Postgres.
- **Because:** DuckLake attaches its metadata database as a catalog of its own.
- **Class:** `discovery`.
- **Consequence:** the attestation asks Postgres, through that function, whether the role can
  write any table, view, materialized view, foreign table or sequence in its schema, create in
  the schema, read any other schema that holds DuckLake metadata, read any other relation
  outside its schema that `PUBLIC` cannot, or is a superuser, or when its schema holds a view
  that reads outside it, directly, through another view or through a function, and refuses
  the session if so. Column grants count.
  What `PUBLIC` reads is left out: every login reads it, so it is the deployment's choice,
  and refusing it would refuse a database with `pg_stat_statements` installed. Another lake's metadata can hold its inlined rows, so reading
  it is reading that tenant's data. The battery's tenant `c` holds a stray `INSERT` grant on
  a table, and tenant `d` one on a view plus `SELECT` on tenant `a`'s metadata; both are
  refused. Tenant `d` can also read a table in an unrelated schema, and is refused for it,
  while a table granted to `PUBLIC` refuses no one.
- **Proposed row (RFC 0078):** `ASSUMED` — the metadata role's grants bound the metadata
  database, and the attestation reads them back.

## D-103 — The allowlist admits writes under the data path

- **Touches:** §3.5 "Read-only lake" and "No local-disk read/write".
- **Found:** on both builds, `COPY … TO '<data path>/x.parquet'` succeeds although `lake` is
  attached `READ_ONLY`. On 2.0, `ducklake_delete_orphaned_files('lake', cleanup_all => true)`
  deleted a file there; on 1.5 it was refused. The same holds for an `s3://` path whose key
  can write.
- **Because:** `allowed_directories` admits reads and writes alike, and removing an orphan
  writes no metadata, so the read-only attach never sees it.
- **Class:** `discovery`.
- **Consequence:** a dynamic read refuses `COPY` and the `ducklake_*` functions already. The
  docs name the outer control: a read-only store key, or a read-only mount, which a builder
  writing the same directory from the same process cannot have. A test pins the write.
- **Proposed row (RFC 0078):** `ASSUMED` — on a DuckLake, read-only rests on the store key or
  the mount; the `READ_ONLY` attach and the classification are belts.

## D-104 — A dot-segment escapes an `s3://` allowlist, not a local one

- **Touches:** §3.5 "No cross-tenant read by object path", and D-073's note that the engine
  resolves no dot-segments.
- **Found:** on both builds, with `allowed_directories` holding a local data path,
  `read_parquet('<data path>../<other tenant>/…')` and `read_text('<data path>../../…')` are
  refused before any I/O. With it holding `s3://lake/t1/`, `s3://lake/t1/../t2/…` passes the
  allowlist and goes to the store, which refused it with a 403 in the probe.
- **Because:** the local check sees the path the filesystem would open; the `s3://` check
  sees a key, which only the store interprets.
- **Class:** `discovery`.
- **Consequence:** the attestation probes `<data path>../forze-attest/x` on a local lake. On
  `s3://` the docs say the tenant's key must be scoped to its own prefix, as the Iceberg docs
  say of vended credentials. A dynamic read refuses every path anyway. A test pins that the
  engine sends the `s3://` form to the store; the store's answer is the store's, so the
  battery, whose key reaches the whole bucket, does not pin it.
- **Proposed row (RFC 0078):** `ASSUMED` — a local DuckLake attests the engine's refusal of a
  dot-segment; on `s3://` the store key's scope confines reads to the prefix.

## Review findings — 2026-11-01

Fresh-eyes review of the branch before it was published.

| # | Severity | Finding | Status |
|---|---|---|---|
| R-1 | High | On DuckDB 1.5 the session can open Postgres connections past external access, and attests clean; the changelog said it attests "like the Iceberg one" | Kept on 1.5, which the issue requires (SQLMesh pins `duckdb<2`). Dynamic reads refuse the functions (D-101); the changelog and docs now say the engine does not bound Postgres egress on 1.5 |
| R-2 | High | A local allowlist matches by spelling, so `<data path>../<other>/` might read another tenant | Refuted by probe: the engine refuses it on both builds. The attestation now probes it; the `s3://` case is D-104 |
| R-3 | Question | The unnamed `postgres` secret might be merged into a statement's own connection string, sending the password elsewhere | Refuted by probe (D-101): `fe_sendauth: no password supplied`. Pinned |
| R-4 | Medium | The grants check saw tables only, not views or sequences | Fixed: views, materialized views, foreign tables and sequences |
| R-5 | Medium | Nothing checked that the role cannot read another tenant's metadata | Fixed: the attestation refuses a role that can read another schema holding DuckLake metadata |
| R-6 | Medium | The CA bundle sits in a writable `allowed_paths` on a local lake, where nothing needs it | Fixed: a DuckLake sets and allow-lists it only for an `s3://` data path |
| R-7 | Medium | An empty role or password lets libpq fall back to the OS user or `PGPASSWORD` | Fixed: refused before any instance (`duckdb_lake_role_credentials_empty`) |
| R-8 | Low | The fingerprint and the build each call `catalog_for_tenant`, so a change between them leaves a stale fingerprint | Not changed: the Iceberg path has the same shape, and the next change heals it |
| R-9 | Low | `_ducklake_problems` and `_ducklake_outside` had no unit coverage | Fixed: unit tests over a fake engine, the nested quoting included |
| R-10 | Low | D-101 said both builds' answers are pinned, but only `postgres_scan` was | Fixed: `postgres_attach` is pinned too |
| R-11 | Question | The libpq environment fallbacks are not pinned | Not pinned: libpq's documented behaviour, not the engine's |

## Review findings — 2026-11-01 (PR #801)

| # | Severity | Finding | Status |
|---|---|---|---|
| R-12 | High | The grants check refused only other schemas holding DuckLake metadata, so a role granted `SELECT` on application tables could read them through `postgres_query` (cubic) | Fixed: any relation outside the schema that the role reads and `PUBLIC` does not is refused (D-102) |
| R-13 | Medium | D-104's `s3://` dot-segment path had no regression test (cubic) | Fixed: a test pins that the engine sends it to the store; the store's own answer is not pinned (D-104) |
| R-14 | Medium | D-101's mitigation did not name the libpq credential sources to remove (cubic) | Fixed: the docs and D-101 name `PGPASSWORD`, `PGPASSFILE`, `PGSERVICEFILE` and `~/.pgpass` |
| R-15 | — | A test held a literal wrong password, which GitGuardian scans in every commit of the PR (GitGuardian) | Fixed in code: generated with `token_hex`. An earlier commit of the PR still holds the literal, a dummy that was never a credential; clearing the check needs the incident resolved or the history rewritten, which is the maintainer's call |
| R-16 | High | The grants check read table-level privileges only, so a column-level `UPDATE` on metadata, or a column-level `SELECT` elsewhere, passed (cubic) | Fixed: `has_any_column_privilege` for inserts, updates and reads. Tenant `d` holds one of each |
| R-17 | High | A local data path that is a symlink passes the lexical allowlist, and the filesystem follows it to another tenant's data (cubic) | Fixed in part: the build refuses a local data path that is not its own real path. A symlink deeper under it is not walked; the docs say to keep none there |
| R-18 | Low | An Iceberg principal and a DuckLake one with the same id and secret could share a fingerprint (cubic) | Fixed: the DuckLake fingerprint and access parts carry a kind marker |
| R-19 | Low | The unit's header claimed every probe ran on both builds, and product code in one file (cubic) | Fixed in place |
| R-20 | Low | The docs wrote the optional CA entry as a nested list (CodeAnt) | Fixed |
| R-21 | High | A view in the role's own schema runs with its owner's rights, so it can expose another lake's metadata past every grant check (CodeAnt) | Fixed: the attestation refuses a view or materialized view in the schema that reads outside it and that the role can read. Tenant `d` holds one over tenant `b`'s metadata. `SECURITY DEFINER` functions are left to the deployment, and the docs say so |
| R-22 | High | R-21's view check counted only views the role can read with a direct outside dependency, so a readable view over an inner view, or over a function, passed (cubic) | Fixed: any view or materialized view in the schema that depends on a relation outside it or on a function outside `pg_catalog` is refused, readable or not. A chain leaving the schema must cross such a view. Tenant `d` holds a view over a view over tenant `b`'s metadata, and a view calling a function |
