# Contributing to Forze

Thank you for your interest in contributing to **Forze**. This document describes the development workflow, coding conventions, and contribution guidelines.

## Conventions and where they live

This file is authoritative for how to *work in this repository* — what to run, where tests and docs go, what a release needs. It deliberately does **not** restate formats that are maintained elsewhere: a second copy of a format drifts, and then the two copies contradict each other.

| Convention | Owner | What this file adds |
|---|---|---|
| Commit message and PR title format | [`gitmoji-conventional`](.claude/skills/gitmoji-conventional/SKILL.md) + [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) | the shape, this repo's scopes, how to check a subject |
| `CHANGELOG.md` format | [`keep-a-changelog`](.claude/skills/keep-a-changelog/SKILL.md) + [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) | what counts as user-facing here, the house concision rule |
| Documentation page structure | [`altitude-docs`](.claude/skills/altitude-docs/SKILL.md) | where pages live, how to build and serve them |
| Python docstring style | **this file** — see [Documentation](#documentation) | no skill owns it; the rule below is the rule |
| Commands, extras, layering contracts | `justfile`, `pyproject.toml` | pointers only |

Where this file and a skill disagree about a **format**, the skill wins — fix this file, or open an issue, rather than following the stale copy. Where they disagree about a **repository fact** — a command, a path, a marker, a directory layout — this file wins.

Skills live in `.claude/skills/` and are mirrored byte-for-byte to `.agent/skills/` and `.agents/skills/` for other tools; `AGENTS.md` routes agents to them.

## Reporting bugs

If you encounter a bug, please report it using the GitHub issue tracker:

https://github.com/morzecrew/forze/issues

When reporting a bug, please include:

- steps to reproduce the issue
- expected behavior
- actual behavior
- relevant logs or stack traces
- environment information (Python version, OS, etc.)

## Feature requests

Feature requests can also be submitted using the GitHub issue tracker.

Please describe the use case and why the feature would be useful.

## Development Setup

Prerequisites:

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- [d2](https://d2lang.com/) (optional, for regenerating diagrams in docs)

Clone the repository and install all dependencies, including development tools, documentation dependencies, and optional integrations:

```bash
git clone https://github.com/morzecrew/forze
cd forze
uv sync --all-groups --all-extras
```

### Running Tests

Run the full test suite:

```bash
just test
```

Run only unit tests:

```bash
just test tests/unit
```

Run integration tests (require running external services or testcontainers):

```bash
just test tests/integration
```

### The nightly DST matrix

Every build runs the flagship simulation scenarios over a small seed band as a merge guard — 8 dlock seeds and 12 HLC seeds, sized for a human waiting on a PR. `just fuzz` widens that to 64 and 128, but it is `fuzz`-marked and excluded from `just test`, so it runs only when asked. Neither is a size at which a rare interleaving turns up. The nightly workflow runs the same scenarios and the same invariants over 65,536 seeds per cell, across four fault profiles:

```bash
just dst-nightly-cells          # which cells exist
just dst-nightly dlock-storm    # one cell at the nightly band
just dst-nightly-all 1024       # the whole matrix at a band you will wait for
```

A cell is a scenario in one environment. The environments are `FaultProfile` declarations in `tests/support/dst_flagship.py`, and the cell list is **derived** from them — add a profile and the nightly gains a cell that the verdict then requires, with no list to remember to edit.

Each profile declares the reachability targets it must drive, because what is reachable depends on the environment: `contention` cuts no link and injects no error, so it can never reach `write-retried`, and a profile that declared no targets at all is refused outright. The verdict fails on a missing cell, a band that ran zero seeds, any violating seed, a declared target the band never drove, or a result for a cell nobody declared.

When a night finds a violating seed, append it to the scenario's `*_REGRESSION_SEEDS` tuple so the merge guard re-checks it forever.

### Emulators and engine matrices

A test that stands in for a managed cloud service is admissible in exactly three forms:

| Form | Why it's honest | Used here |
|---|---|---|
| **Independent-reimplementation emulator** | The emulator implements the wire protocol itself, so behavior differences are findings | floci (SQS, KMS, S3 — see `tests/support/floci.py`), fake-gcs-server, the Firestore/BigQuery emulators, fake-cloud-kms |
| **Engine matrix** | One suite over two independent implementations of one protocol flushes out accidental engine-specific behavior | Kafka suite over Apache Kafka + Redpanda; S3 suite over MinIO + floci-S3 |
| **Env-gated real cloud** | When no emulator exists, run the real service, skipped without credentials | Yandex KMS (`yc_kms` marker), VK ID live (`FORZE_LIVE_IDP_TESTS`) |

An emulator that merely proxies the same OSS engine a suite already runs (e.g. a "Neptune" that is a Neo4j container behind a byte relay) proves nothing beyond plumbing and must not be added as a fidelity claim.

A divergence an engine matrix finds is a finding: fix the adapter or declare it in capabilities if contract-relevant, normalize it in the test if incidental (with a comment naming the engine behavior) — never special-case per engine in `src/`. Known emulator infidelities and the reasoning behind the floci pin live in `tests/support/floci.py`.

### Code Quality

Run all quality checks (types, imports, dead code, dependencies, security, secret scanning):

```bash
just quality
```

Strict mode (fail on any issue):

```bash
just quality -s
```

`just quality` runs [gitleaks](https://github.com/gitleaks/gitleaks) on the full tree (including `tests/`) via pre-commit. Do not commit real credentials or API keys anywhere in the repository; use synthetic fixtures in tests.

All checks must pass before submitting a pull request.

### Documentation

Documentation lives in `pages/docs/` and is built with [Zensical](https://zensical.org/). See `pages/zensical.toml` for navigation and structure. Page structure, altitude, and Diátaxis placement are owned by the [`altitude-docs`](.claude/skills/altitude-docs/SKILL.md) skill.

**Serving docs while editing**

Use `just serve-docs` to serve the documentation with live reload:

```bash
just serve-docs
```

**Diagrams**

Source diagrams live in `pages/diagrams/` as `.d2` files. They are built to SVG in `pages/docs/_diagrams/` (light and dark variants). If the [Run on Save](https://marketplace.visualstudio.com/items?itemName=emeraldwalk.runonsave) extension is installed (recommended in `.vscode/extensions.json`), diagrams are regenerated automatically when you save a `.d2` file. Otherwise, run:

```bash
just build-diagrams
```

**Consistency**

- Update documentation when behavior changes; keep docs aligned with code.
- Add or update pages under `pages/docs/` and adjust `pages/zensical.toml` navigation as needed.
- Follow markdownlint rules (see `.markdownlint.yaml`) for style consistency.

**Docstrings**

Python docstrings use **Sphinx/reST field lists and roles** — `:param x:`, `:returns:`, `:raises:`, and cross-references such as ``:class:`SomeType` ``. This is the repository's convention; there is no skill for it, so this section is the authority.

The installed `python-google-docstrings` skill **does not apply to this repository**. Google-style `Args:` / `Returns:` sections need Sphinx Napoleon, which this project does not use, and the skill's own guidance defers to a project that writes reST field lists. Do not convert existing docstrings.

### Integration dependency configs

Integration packages (`forze_postgres`, `forze_mongo`, `forze_redis`, etc.) declare **frozen `attrs` classes** for `*DepsModule` route maps—not `TypedDict` or plain dict literals. Shared conventions:

- `@attrs.define(slots=True, kw_only=True, frozen=True)`
- Inherit [`TenantAwareIntegrationConfig`](src/forze/application/contracts/tenancy/integration_config.py) when a route supports `tenant_aware`
- Nested member maps: use [`frozen_mapping`](src/forze/base/primitives/mapping.py) as an `attrs` field converter
- Validation on the type (`__attrs_post_init__`, `.validate()`, or `.validate_against_spec(spec)`); avoid exporting free-standing `validate_*` helpers from package `__all__`

App authors and tests construct configs explicitly, e.g. `MongoDocumentConfig(read=(...), write=(...), ...)`.

## Commit Messages

Commit subjects and PR titles use [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) with an [official gitmoji](https://gitmoji.dev/) prefix:

```text
<gitmoji> <type>[(scope)][!]: <description>
```

```text
✨ feat(search): add fuzzy match option
🐛 fix(postgres): correct ts_rank_cd signature
📝 docs: add S3 integration guide
💥 feat(tenancy)!: rename the statement origin floors
```

**This file does not define the emoji→type mapping.** The mapping is the official gitmoji set, and the copy this repository works from lives in the `gitmoji-conventional` skill:

- complete table — [`references/gitmoji-mapping.md`](.claude/skills/gitmoji-conventional/references/gitmoji-mapping.md)
- rules for scope, body, footers, breaking changes, and reverts — [`SKILL.md`](.claude/skills/gitmoji-conventional/SKILL.md)

Agent tooling is generally told to prefer a repository's own convention over a skill. Here the convention *is* the skill, so there is nothing to override — follow the skill.

### Checking a subject

The skill ships a validator. Run it on a message you are about to use, or on a whole branch:

```bash
CHECK=.claude/skills/gitmoji-conventional/scripts/check_commit_msg.py
python3 "$CHECK" --message "✨ feat(api): add OAuth login"
python3 "$CHECK" --range main..HEAD
```

It **fails** on an unofficial gitmoji, an emoji that disagrees with the type, breaking-change signals that disagree with each other, a malformed `BREAKING CHANGE:` footer, a past-tense description, and a body over the hard cap. Subject length and body line width are **warnings**. Variation-selector differences (`🗃` vs `🗃️`) are normalized and never fail.

The validator is not wired into a git hook, so it is advisory unless you run it. Commits predating the current convention do not all pass — expect noise from `--range` over old history.

### Most used in this repository

An excerpt of the full table, for convenience. It is a strict subset with identical types — **not** a competing mapping. If you need an emoji that is not here, take it from the full table, not from intuition.

`just quality` enforces that: a row here that the skill's mapping does not have, or one whose type disagrees with it, fails the build. Add the row upstream first.

| Gitmoji | Type | Use for |
|---|---|---|
| ✨ | feat | new features |
| 🛂 | feat | authorization, roles, permissions |
| 🦺 | feat | validation |
| 🧵 | feat | multithreading / concurrency |
| 🗃️ | feat | database-related changes |
| 🚩 | feat | feature flags |
| 📈 | feat | analytics / tracking |
| 🩺 | feat | healthchecks |
| 🐛 | fix | bug fix |
| 🚑️ | fix | critical hotfix |
| 🩹 | fix | simple, non-critical fix |
| 🥅 | fix | catch errors |
| 🔒️ | fix | security or privacy fix |
| 🚨 | fix | fix linter / compiler warnings |
| ♻️ | refactor | refactor code |
| 🔥 | refactor | remove code or files |
| ⚰️ | refactor | remove dead code |
| 🚚 | refactor | move or rename resources |
| 🏗️ | refactor | architectural changes |
| 🎨 | style | code structure / formatting |
| ⚡️ | perf | performance improvements |
| 📝 | docs | documentation |
| 💡 | docs | code comments |
| ✏️ | docs | fix typos |
| ✅ | test | add, update, or pass tests |
| 🧪 | test | add a *failing* test |
| 🤡 | test | mocks |
| ⬆️ | build | upgrade dependencies |
| ➕ ➖ | build | add / remove a dependency |
| 🧱 | build | infrastructure |
| 👷 | ci | CI configuration |
| 💚 | ci | fix CI build |
| 🔧 | chore | configuration files |
| 🔨 | chore | development scripts |
| 🧐 | chore | data exploration / inspection |
| 🧑‍💻 | chore | developer experience |
| 🔖 | chore | release / version tags |
| 💥 | *underlying type* + `!` | breaking change — see below |
| ⏪️ | revert | revert a commit |

### Breaking changes

Three signals must agree, or the validator rejects the subject:

1. `💥` **replaces** the type's usual emoji — it is not a type of its own.
2. `!` immediately before the colon.
3. A `BREAKING CHANGE:` footer whenever the break needs more detail than the subject holds.

The type underneath stays `feat`, `fix`, or `refactor`, so release tooling still reads the SemVer signal:

```text
💥 feat(tenancy)!: name the rung between built statements and raw ones

BREAKING CHANGE: `StatementOrigin.RAW` is removed; declare the floor
 explicitly on the spec instead.
```

A breaking commit also needs a `CHANGELOG.md` entry that names the break — see [Changelog](#changelog).

### Security fixes

`security` is **not** a Conventional Commits type. A security fix is `🔒️ fix(...)`, and its changelog entry goes under `Security`. Follow `SECURITY.md` for disclosure and keep public detail minimal.

### Scope

Optional, and a noun naming the affected area. Prefer the package or plane the change lives in: `core`, `execution`, `document`, `search`, `postgres`, `redis`, `mongo`, `fastapi`, `identity`, `authn`, `tenancy`, `kits`, `dst`, `realtime`, `deps`, `docs`, `skills`. Omit it when the change is genuinely cross-cutting. Never invent a scope to fill the slot.

### Body

Optional, separated from the subject by a blank line, for context the subject cannot carry — motivation, a mechanism, a rejected alternative, a consequence a reader would not predict. The full rules and the enforced caps are in the skill; the short version:

- imperative mood, no trailing period on the subject
- at most 4 bullets, `-` only
- hard cap of 20 non-blank body lines (fences and footers excluded)
- no session narrative, no evidence dumps, no restating the subject

```text
✨ feat(search): add fuzzy match option

- implement trigram-based matching
- add configuration flag for fuzzy mode
```

### Dependabot

Dependabot subjects are prefixed by [`.github/dependabot.yml`](.github/dependabot.yml) — `🔧 chore` for `uv`, `👷 ci` for GitHub Actions, `⬆️ build` for the devcontainer image. An ecosystem with no `commit-message.prefix` produces a subject with no gitmoji, which fails the convention; fix the config rather than rewriting bot commits.

## Pull Requests

PR titles use the same format as commit subjects, with tighter constraints — the title has to drop into GitHub unedited:

- exactly one line: no body, bullets, or footers
- no issue references in the title unless you are asked for them
- a mixed PR gets one primary type, not an enumeration
- a breaking PR carries `!`; migration notes go in the description, never the title

Fill in [`.github/pull_request_template.md`](.github/pull_request_template.md) — it is the checklist for architecture boundaries, public-API impact, and downstream `skills/` updates.

Guidelines:

- Submit **one logical change per pull request**
- `just test` and `just quality -s` green locally before you open it
- Rebase or squash commits before merging if needed
- Update documentation when behavior changes

## Testing Guidelines

Test layout:

```text
tests/
  unit/
  integration/
  perf/           # benchmarks; run with `just perf` (excluded from `just test` / CI)
```

Mirror the `src` structure when possible:

```text
src/pkg/foo/bar.py -> tests/unit/test_pkg/foo/test_bar.py
```

If there are duplicates in test filenames, use prefixes to distinguish them, for example:

```text
src/pkg/foo/bar.py -> tests/unit/test_pkg/foo/test_bar.py
src/pkg/baz/bar.py -> tests/unit/test_pkg/baz/test_baz_bar.py
```

Conventions:

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

**Unit Tests**

Avoid external i/o. Use mocks when necessary. Prefer `MagicMock(spec=RealClass)`. One `TestX` class per tested type.

**Integration Tests**

Use fixtures from `tests/integration/conftest.py` (shared Docker check via `tests/support/docker.py`). One scenario per test. Ensure test data isolation. See [tests/README.md](tests/README.md) for tiers (L0–L3) and the per-package smoke matrix.

**Markers**

- `integration` — applied to all tests under `tests/integration/` via root conftest
- `unit` — use `pytestmark = pytest.mark.unit` on focused unit modules (authn/authz pattern)
- `perf` — performance benchmarks under `tests/perf/`; **excluded from default `just test` (CI)**.
  Run with `just perf`. Perf measures overall throughput/latency; many perf tests use Docker
  testcontainers, but not all (e.g. codec micro-benchmarks are in-process only).

**CI vs perf:** `just test` runs unit + integration (`-m "not perf"`). `just perf` runs
`@pytest.mark.perf` with pytest-benchmark (`--benchmark-only`).

Run subsets: `just test -m integration`, `just test tests/unit`, `just perf tests/perf/...`.

New pytest markers must be registered in `pyproject.toml` before use.

## Changelog

User-facing changes go in `CHANGELOG.md` under `## [Unreleased]`, in [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) format. The six categories are `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, and `Security`; the [`keep-a-changelog`](.claude/skills/keep-a-changelog/SKILL.md) skill carries the full rules — where a breaking change goes, how a version section is cut, how a revert is recorded.

What this repository adds on top of the spec:

**Leave internal changes out.** CI updates, test-only changes, and trivial refactors do not belong here.

**Keep entries concise.** One bullet = a headline, the key public API/migration, and any
breaking note — not an essay. Leave out the *why*, the implementation mechanics, and
"verified by …" (those live in the PR and commits). Prefer one tight bullet over several
overlapping ones; group a multi-PR arc (e.g. a hardening initiative) under a bold sub-heading
rather than repeating context in each line. Always preserve **breaking** markers, new public
symbol names, and any **Migration:** SQL. When the `[Unreleased]` section grows large or several
bullets describe one feature, compact it: consolidate the overlap into grouped, single-line
entries (keep every breaking/migration/public-API fact).

**Edit only `[Unreleased]`.** Never rewrite an already-released version section.

## Release Process

Releases are tag-driven.

Creating a tag `vX.Y.Z` triggers GitHub Actions to:

1. Build the package
2. Publish it to PyPI
3. Create a GitHub release

Before tagging a release, move the relevant entries from the `[Unreleased]` section to the new version section in `CHANGELOG.md`.

## Performance regression gate

The in-process benchmark subset (marked `perf_gate`) is compared in CI against
your PR's merge-base **on the same runner**, **interleaved** across several rounds
(base, head, base, …), and fails on a >15% regression of the **median of each
side's per-run `min`**. Same-runner pairing cancels the between-runner lottery;
interleaving + median-of-mins cancels within-job drift (thermal throttle, a noisy
neighbour) and the unlucky-round flakiness a single `min` sample suffers on
sub-millisecond benchmarks. `min` is the per-run metric on purpose — micro-bench
noise is one-directional (interference only ever *slows* an iteration), so the
per-run `min` is the cleanest estimate of the code path; `mean`, dragged up by
every outlier, would flag *more* false positives, not fewer. The comparator is
[`tests/perf/gate_compare.py`](tests/perf/gate_compare.py); rounds per side are
tunable via `PERF_GATE_ROUNDS` (default 3 — higher is more robust but slower). Only
benchmarks ≥ `--min-floor-ms` (default 1 ms) can fail the gate — sub-millisecond ones
are below a shared runner's timing-noise floor and are reported for trend only, so a new
micro-benchmark won't make the gate flaky.

If it fires: fix the regression, or justify it in the PR and apply the
`skip-perf-gate` label. Comparisons match benchmarks by name — new benchmarks
pass trivially; renames silently drop out of comparison, so prefer keeping
names stable. Locally: `just perf-save` to snapshot a baseline, `just
perf-check` to compare your changes against it (10% threshold). Mark a new
benchmark with `perf_gate` only if it is in-process and deterministic (no
Docker).

## Questions

If you have questions about contributing or the codebase, please open an issue or start a discussion on GitHub.
