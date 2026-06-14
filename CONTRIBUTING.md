# Contributing to Forze

Thank you for your interest in contributing to **Forze**. This document describes the development workflow, coding conventions, and contribution guidelines.

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

Documentation lives in `pages/docs/` and is built with [Zensical](https://zensical.org/). See `pages/zensical.toml` for navigation and structure.

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
- Python docstrings use Sphinx/reST roles (see the `python-rest-docstrings` skill).

### Integration dependency configs

Integration packages (`forze_postgres`, `forze_mongo`, `forze_redis`, etc.) declare **frozen `attrs` classes** for `*DepsModule` route maps—not `TypedDict` or plain dict literals. Shared conventions:

- `@attrs.define(slots=True, kw_only=True, frozen=True)`
- Inherit [`TenantAwareIntegrationConfig`](src/forze/application/contracts/tenancy/integration_config.py) when a route supports `tenant_aware`
- Nested member maps: use [`frozen_mapping`](src/forze/base/primitives/mapping.py) as an `attrs` field converter
- Validation on the type (`__attrs_post_init__`, `.validate()`, or `.validate_against_spec(spec)`); avoid exporting free-standing `validate_*` helpers from package `__all__`

App authors and tests construct configs explicitly, e.g. `MongoDocumentConfig(read=(...), write=(...), ...)`.

## Commit Messages

Commits follow **Conventional Commits** with a **gitmoji** prefix:

```
<gitmoji> <type>[scope]: <description>
```

| Gitmoji | Type | Purpose |
|---------|------|---------|
| ✨ | feat | new features |
| 🚸 | feat | UX improvements |
| 📊 | feat | analytics / tracking |
| 💬 | feat | text / literals |
| 🌱 | feat | seed data |
| 🗃 | feat | database changes |
| 🧵 | feat | multithreading / concurrency |
| 🦺 | feat | validation |
| 🦖 | feat | backwards compatibility |
| 🛂 | feat | authorization / permissions |
| 🧭 | feat | feature flags |
| 🩺 | feat | healthchecks |
| 🥚 | feat | easter egg |
| 💥 | feat | breaking changes |
| 🐛 | fix | bug fix |
| 🚑 | fix | critical hotfix |
| 🩹 | fix | small fix |
| 🚨 | fix | fix linter / compiler warnings |
| 🎯 | fix | catch errors |
| ♻️ | refactor | refactor code |
| 🔥 | refactor | remove code/files |
| 💩 | refactor | bad code needing improvement |
| 🚚 | refactor | move/rename files |
| 🗑 | refactor | deprecate code |
| ⚰️ | refactor | remove dead code |
| 🏗 | refactor | architectural changes |
| 🎨 | style | code formatting / structure |
| ⚡️ | perf | performance improvements |
| 📝 | docs | documentation |
| 💡 | docs | code comments |
| ✏️ | docs | fix typos |
| 🧪 | test | tests |
| 🤡 | test | mocks |
| 📸 | test | snapshots |
| 📦 | build | packages / compiled files |
| ⬆️ | build | upgrade dependencies |
| ⬇️ | build | downgrade dependencies |
| 📌 | build | pin dependencies |
| ➕ | build | add dependency |
| ➖ | build | remove dependency |
| 🧱 | build | infrastructure |
| 👷 | ci | CI configuration |
| 💚 | ci | fix CI build |
| 🔧 | chore | maintenance |
| 🔨 | chore | dev scripts |
| 🙈 | chore | .gitignore |
| 🕵️ | chore | data exploration |
| 🧑‍💻 | chore | developer experience |
| 🔖 | chore | release / version tags |
| 🚀 | chore | deployment |
| 🚧 | chore | work in progress |
| 🔀 | chore | merge branches |
| 🔒 | security | security changes |
| ⏪ | revert | revert commit |

Examples:

```text
✨ feat(search): add fuzzy match option
🐛 fix(postgres): correct ts_rank_cd signature
📝 docs: add S3 integration guide
```

Commits may include an optional body after the subject line. The body should be separated from the subject by a blank line and may contain additional context, rationale, or a list of changes:

```text
✨ feat(search): add fuzzy match option

- implement trigram-based matching
- add configuration flag for fuzzy mode
- update search API documentation
```

Guidelines:

- Use **imperative mood** for the description
- Keep the subject line concise (≤72 chars)
- Do not end the subject line with a period
- If additional context is needed, add a body separated by a blank line
- Bullet lists are recommended for describing multiple changes

## Pull Requests

Pull request titles follow the same format as commit messages.

Guidelines:

- Submit **one logical change per pull request**
- Ensure tests and quality checks pass
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

Avoid external i/o. Use mocks when necessary. Prefer `MagicMoc(spec=RealClass)`. One `TestX` class per tested type.

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

User-facing changes must be recorded in `CHANGELOG.md` under the `[Unreleased]` section.

Categories:

- **Added** — new APIs, features, modules
- **Changed** — behavior changes, refactors affecting usage
- **Fixed** — bug fixes

Exclude internal changes such as CI updates, test-only changes, or trivial refactors.

**Keep entries concise.** One bullet = a headline, the key public API/migration, and any
breaking note — not an essay. Leave out the *why*, the implementation mechanics, and
"verified by …" (those live in the PR and commits). Prefer one tight bullet over several
overlapping ones; group a multi-PR arc (e.g. a hardening initiative) under a bold sub-heading
rather than repeating context in each line. Always preserve **breaking** markers, new public
symbol names, and any **Migration:** SQL. When the `[Unreleased]` section grows large or several
bullets describe one feature, compact it: consolidate the overlap into grouped, single-line
entries (keep every breaking/migration/public-API fact). Edit only `[Unreleased]` — never
rewrite an already-released version section.

## Release Process

Releases are tag-driven.

Creating a tag `vX.Y.Z` triggers GitHub Actions to:

1. Build the package
2. Publish it to PyPI
3. Create a GitHub release

Before tagging a release, move the relevant entries from the `[Unreleased]` section to the new version section in `CHANGELOG.md`.

## Questions

If you have questions about contributing or the codebase, please open an issue or start a discussion on GitHub.

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
tunable via `PERF_GATE_ROUNDS` (default 3 — higher is more robust but slower).

If it fires: fix the regression, or justify it in the PR and apply the
`skip-perf-gate` label. Comparisons match benchmarks by name — new benchmarks
pass trivially; renames silently drop out of comparison, so prefer keeping
names stable. Locally: `just perf-save` to snapshot a baseline, `just
perf-check` to compare your changes against it (10% threshold). Mark a new
benchmark with `perf_gate` only if it is in-process and deterministic (no
Docker).
