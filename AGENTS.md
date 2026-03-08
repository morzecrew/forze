# AGENTS.md

This file is a routing guide for AI agents working in this repository.

Do not treat this file as the primary source of truth for project rules.
Use it to find the correct file for each kind of task.

## Purpose

This repository uses specialized files for contribution rules, architecture constraints,
testing, documentation, security, and build workflows.

Agents should prefer the most specific source of truth instead of duplicating or inventing rules.

## Source of truth map

### General contribution workflow

Read:
- `CONTRIBUTING.md`

Use it for:
- commit and PR conventions
- testing expectations
- changelog rules
- contribution workflow

### Project metadata and architectural constraints

Read:
- `pyproject.toml`

Use it for:
- package metadata
- dependency groups and extras
- pytest configuration
- coverage settings
- import-linter contracts
- lint/tool configuration

### Common commands and quality checks

Read:
- `justfile`

Use it for:
- test commands
- quality commands
- security scan command
- repo-level developer workflows

### Documentation structure and build

Read:
- `pages/mkdocs.yml`
- `pages/justfile`

Use them for:
- documentation navigation
- docs page structure
- mkdocstrings behavior
- docs build workflow
- D2 diagram rendering

### Security process

Read:
- `SECURITY.md`

Use it for:
- vulnerability handling
- disclosure expectations
- security-sensitive changes

### Code and tests

Read:
- `src/`
- `tests/`

Use them for:
- public behavior
- architecture and package boundaries
- test synchronization
- examples and API usage

## Repository operating rules for agents

1. Prefer updating existing files over creating new top-level process documents.
2. Do not duplicate rules from `CONTRIBUTING.md`, `SECURITY.md`, `pyproject.toml`, or docs config.
3. When working on code, check `pyproject.toml` for architectural and tooling constraints first.
4. When working on tests, use `justfile` and pytest configuration from `pyproject.toml`.
5. When working on docs, use `pages/mkdocs.yml` and `pages/justfile` as the source of truth.
6. When working on security-sensitive changes, follow `SECURITY.md` and avoid public disclosure of vulnerabilities.
7. Keep changes small, scoped, and aligned with the repository’s existing structure.

## Agent-specific memory

Agent journals live under `.jules/`.

Suggested files:
- `.jules/atlas.md`
- `.jules/bolt.md`
- `.jules/verifier.md`
- `.jules/steward.md`

These are memory files, not the source of truth for repository rules.

## Cross-tool compatibility

If additional AI-tool-specific directories exist (for example `.agent/` or `.cursor/`),
they should point back to the same repository conventions rather than redefining them.

Preferred pattern:
- central rules and routing live in `AGENTS.md`
- specialized project policy stays in its canonical file
- tool-specific files may reference or mirror `AGENTS.md`