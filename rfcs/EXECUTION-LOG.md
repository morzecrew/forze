# Execution log

Where building something disagreed with the design for it, written down at the moment it
happened. Nothing here is revised afterwards to agree with what was later settled, and
nothing here has been folded back into an RFC's own text.

The decision rows below are put forward for the author to accept or refuse. Execution
does not write them into a decision table itself.

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
