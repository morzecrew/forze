# Contributing to Forze

Thank you for your interest in contributing. This document outlines the development workflow, conventions, and how to submit changes.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or another PEP 517–compatible package manager

## Development Setup

```bash
uv sync --all-groups --all-extras
```

This installs core dependencies, dev tools, docs, and optional extras (fastapi, postgres, redis, etc.).

## Running Tests

```bash
just test
```

Unit tests only:

```bash
just test tests/unit
```

Integration tests (require running services or testcontainers):

```bash
just test tests/integration
```

## Quality Checks

Run all checks (types, imports, dead code, dependencies, security):

```bash
just quality
```

Strict mode (fail on any error):

```bash
just quality -s
```

## Commit Messages

Use **Conventional Commits** with a **gitmoji** prefix:

```
<gitmoji> <type>[scope]: <description>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `build`, `ci`, `revert`

**Gitmoji mapping:** feat → ✨, fix → 🐛, docs → 📝, style → 💄, refactor → ♻️, perf → ⚡️, test → ✅, chore → 🔧, build → 📦, ci → 👷, revert → ⏪

**Examples:**

- `✨ feat(search): add fuzzy match option`
- `🐛 fix(postgres): correct ts_rank_cd signature`
- `📝 docs: add S3 integration guide`

Subject line: imperative mood, concise (≤72 chars), no trailing period.

## Pull Requests

PR titles follow the same format as commit messages. One logical change per PR; squash or rebase as needed before merge.

## Tests

- **Layout:** `tests/unit/` and `tests/integration/`. Mirror `src` structure: `src/pkg/foo/bar.py` → `tests/unit/test_pkg/foo/test_bar.py`
- **Naming:** Files `test_*.py`, classes `Test*`, functions `test_*`
- **Unit:** No I/O. Use mocks; prefer `MagicMock(spec=RealClass)`. One `class TestX:` per tested type
- **Integration:** Use fixtures from `tests/integration/conftest.py`. One scenario per test; isolate data
- **Markers:** Register new markers in `pyproject.toml` before use

## Changelog

User-relevant changes go in `CHANGELOG.md` under `## [Unreleased]`:

- **Added** — New APIs, features, modules
- **Changed** — Behavior changes, refactors affecting usage
- **Fixed** — Bug fixes

Exclude: test-only changes, CI/CD, internal tooling, trivial refactors.

## Documentation

Docs live in `docs/` and are built with MkDocs:

```bash
uv sync --group docs
uv run mkdocs serve
```

Update `mkdocs.yml` when adding or renaming pages.

## Release Process

Releases are tag-driven. Pushing `vX.Y.Z` triggers GitHub Actions to publish the package and create a GitHub Release. Update `CHANGELOG.md` with the new version section before tagging.

## Questions

Open an issue or discussion on GitHub for questions about contributing or the codebase.
