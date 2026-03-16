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

Run all quality checks (types, imports, dead code, dependencies, security):

```bash
just quality
```

Strict mode (fail on any issue):

```bash
just quality -s
```

All checks must pass before submitting a pull request.

### Documentation

Documentation lives in `pages/docs/` and is built with MkDocs. See `pages/mkdocs.yml` for navigation and structure.

**Serving docs while editing**

Use `just pages serve` to serve the documentation with live reload:

```bash
just pages serve
```

**Diagrams**

Source diagrams live in `pages/diagrams/` as `.d2` files. They are built to SVG in `pages/docs/assets/diagrams/` (light and dark variants). If the [Run on Save](https://marketplace.visualstudio.com/items?itemName=emeraldwalk.runonsave) extension is installed (recommended in `.vscode/extensions.json`), diagrams are regenerated automatically when you save a `.d2` file. Otherwise, run:

```bash
just pages diagrams
```

**Consistency**

- Update documentation when behavior changes; keep docs aligned with code.
- Add or update pages under `pages/docs/` and adjust `pages/mkdocs.yml` navigation as needed.
- Follow markdownlint rules (see `.markdownlint.yaml`) for style consistency.
- API docs are generated from docstrings via mkdocstrings; use Sphinx/reST roles in Python docstrings.

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
