# Contributing to Forze

Thank you for your interest in contributing to **Forze**. This document describes the development workflow, coding conventions, and contribution guidelines.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## Development Setup

Clone the repository and install all dependencies, including development tools, documentation dependencies, and optional integrations:

```bash
git clone https://github.com/morzecrew/forze
cd forze
uv sync --all-groups --all-extras
```

## Running Tests

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

## Code Quality

Run all quality checks (types, imports, dead code, dependencies, security):

```bash
just quality
```

Strict mode (fail on any issue):

```bash
just quality -s
```

All checks must pass before submitting a pull request.

## Commit Messages

Commits follow **Conventional Commits** with a **gitmoji** prefix:

```
<gitmoji> <type>[scope]: <description>
```

| Gitmoji | Type | Purpose |
|---------|---------|---------|
| ✨ | feat | new features |
| 🐛 | fix | bug fixes |
| 📝 | docs | documentation changes |
| 💄 | style | formatting or style changes |
| ♻️ | refactor | internal code restructuring |
| ⚡️ | perf | performance improvements |
| ✅ | test | test changes |
| 🔧 | chore | maintenance tasks |
| 📦 | build | build system changes |
| 👷 | ci | CI configuration changes |
| ⏪ | revert | revert a previous commit |

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
```

Mirror the `src` structure when possible:

```text
src/pkg/foo/bar.py -> tests/unit/test_pkg/foo/test_bar.py
```

Conventions:

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

**Unit Tests**

Avoid external i/o. Use mocks when necessary. Prefer `MagicMoc(spec=RealClass)`. One `TestX` class per tested type.

**Integration Tests**

Use fixtures defined in `tests/integration/conftest.py`. One scenario per test. Ensure test data isolation.

**Markers**

New pytest markers must be registered in `pyproject.toml` before use.

## Changelog

User-facing changes must be recorded in `CHANGELOG.md` under the `[Unreleased]` section.

Categories:

- **Added** — new APIs, features, modules
- **Changed** — behavior changes, refactors affecting usage
- **Fixed** — bug fixes

Exclude internal changes such as CI updates, test-only changes, or trivial refactors.

## Release Process

Releases are tag-driven.

Creating a tag `vX.Y.Z` triggers GitHub Actions to:

1. Build the package
2. Publish it to PyPI
3. Create a GitHub release

Before tagging a release, move the relevant entries from the `[Unreleased]` section to the new version section in `CHANGELOG.md`.

## Questions

If you have questions about contributing or the codebase, please open an issue or start a discussion on GitHub.
