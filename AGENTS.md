# AGENTS.md

Routing guide for AI agents working in this repository.

This file is not the source of truth for project policy. It tells agents where
to look first for authoritative rules before editing code, tests, docs, or release assets.

## Project overview

Forze is a Python library for Domain-Driven Design and Hexagonal Architecture in backend services. It provides core framework layers (application, domain, utils, base) and optional integrations (FastAPI, Postgres, Redis, S3, Temporal, Mongo, HTTP outbound, RabbitMQ, Socket.IO, SQS). Development validation means running tests and quality checks—there is no runnable application or server to start.

## Agent workflow (quick checklist)

1. Identify task type (code, tests, docs, release, security, CI).
2. Open the canonical file(s) from the map below.
3. Apply the smallest scoped change in existing files when possible.
4. Run the relevant checks from `justfile`.
5. Keep tests/docs/changelog aligned when behavior changes.

## Source of truth map

### Contribution process and conventions

Read:
- `CONTRIBUTING.md`

Use it for:
- branch and contribution flow
- commit and PR title format (Conventional Commits + gitmoji)
- test expectations
- changelog and release preparation

### Architecture, packaging, and tool config

Read:
- `pyproject.toml`

Use it for:
- Python version and package metadata
- dependency groups and optional extras
- pytest and coverage configuration
- import-linter contracts and layering constraints
- lint/static-analysis tool configuration

### Commands and local quality gates

Read:
- `justfile`

Use it for:
- test entrypoints (`just test`, path-scoped tests)
- performance testing (`just perf`, performance benchmarks)
- quality checks (`just quality`, strict mode)
- security/dependency/dead-code checks

### Documentation structure and docs build

Read:
- `pages/zensical.toml`
- root `justfile` (docs recipes: `serve-docs`, `build-docs`, `build-diagrams`)

Use them for:
- docs navigation and page structure
- Zensical / Material build behavior
- docs build and serving commands

### Security handling

Read:
- `SECURITY.md`

Use it for:
- vulnerability reporting workflow
- disclosure expectations
- handling of security-sensitive fixes

### Code and tests behavior

Read:
- `src/`
- `tests/`

Use them for:
- runtime behavior and API contracts
- architecture boundaries in real code
- fixture conventions and test patterns

## Repository map (high signal paths)

- `src/forze/`: core framework layers (application/domain/utils/base).
- `src/forze_kits/`: pre-built wiring above contracts (`domain/`; `aggregates/` with per-aggregate `handlers/`; `mapping/`; `dto/`; `integrations/` outbox + notify; `adapters/` secrets; `scopes/` e.g. `DistributedLockScope`). Core `forze.application` keeps contracts, execution, hooks, integrations only—must not import `forze_kits`. Not `forze_identity` (separate plane).
- `src/forze_identity/`: identity plane (`authn/`, `authz/`, `tenancy/`, `oidc/`, `oauth/` subpackages; import as `forze_identity.authn`, etc.).
- `src/forze_identity/builtin/`: shipped-in identity presets (`local/` file/env API keys, `idp/` Google/VK/Telegram Login OIDC); not for production unless you accept each preset's trust model.
- `src/forze_mock/`: in-memory mock adapters (`MockState`, optional `MockRoutedStateRegistry`, `tenancy/`, `execution/` deps module with identity/durable/search/dlock stubs).
- `src/forze_<integration>/`: one optional integration package per backend/transport (Postgres, Redis, Mongo, S3, GCS, BigQuery, Firestore, ClickHouse, Meilisearch, Temporal, Inngest, RabbitMQ, SQS, Socket.IO, FastAPI, outbound HTTP, Vault, …). **Do not maintain this list by hand.** The authoritative, always-current set is `[project.optional-dependencies]` (extras) and `[tool.hatch.build.targets.wheel]` (packages) in `pyproject.toml`; each `forze_<name>` maps to the `<name>` extra. Packages share a common shape (`kernel/` client + ports, `adapters/`, `execution/` deps + lifecycle, `__init__` exporting the public API); per-integration specifics live in that package's `__init__` and its docs page.
- `tests/unit/`: unit tests, typically mirroring `src` layout.
- `tests/integration/`: integration tests with external dependencies.
- `tests/perf/`: performance benchmarks (`-m perf`, excluded from `just test`; many use Docker, some in-process only).
- `pages/`: documentation source and build files.
- `examples/`: runnable, **test-backed** usage examples (each is exercised by a test under `tests/unit/test_examples/`, so examples can't silently rot). E.g. `order_fulfillment.py` runs the aggregate → event → saga → outbox → relay → inbox → downstream flow in-process.
- `skills/`: published [Agent Skills](https://agentskills.io/) for **app authors** (`SKILL.md` per skill; see `skills/AUTHORING.md`); install via README **Agent Skills** (e.g. `npx skills add morzecrew/forze`). Framework contribution uses `.claude/skills/` and canonical docs.

## Operating rules for agents

1. Prefer editing existing files over creating new top-level process documents.
2. Do not duplicate policy text from canonical files; link and follow it instead.
3. Validate architecture and tool constraints in `pyproject.toml` before code changes.
4. Use `justfile` commands as the default way to run tests and quality checks.
5. For user-visible behavior changes, update tests and docs together.
6. Record user-facing changes in `CHANGELOG.md` under `[Unreleased]`, keeping entries concise (see `CONTRIBUTING.md` → Changelog: one tight bullet per change, group multi-PR arcs, compact the section when it grows; always keep breaking/migration/public-API facts).
7. For security-sensitive work, follow `SECURITY.md` and minimize public detail.

## Cross-tool compatibility

If tool-specific directories exist (for example `.agent/` or `.cursor/`), they
should reference this routing file and canonical policy files, not redefine them.

Preferred pattern:
- central routing in `AGENTS.md`
- authoritative policy in canonical files
- tool-specific overlays that only point back to those sources

## Cursor Cloud specific instructions

### Overview

Forze is a Python library (not a runnable application). Development validation means running tests and quality checks, not starting a server.

### Prerequisites

The VM update script installs `uv`, `just`, and Python 3.13 via `uv`. After the update script runs, all dependencies are installed and the environment is ready.

### Key commands

See `justfile` and `CONTRIBUTING.md` for the full list. Quick reference:

- **Unit tests:** `just test tests/unit`
- **All tests (unit + integration):** `just test` (integration tests need Docker for testcontainers)
- **Performance tests:** `just perf` (benchmarks; Docker for container-backed perf, not required for every file)
- **Quality checks (lint/imports/dead-code/deps/security):** `just quality` (or `just quality -s` for strict)
- **Docs:** `just serve-docs` (live reload) · `just build-docs` (build the site)

### Caveats

- Integration tests (`tests/integration/`) require Docker (testcontainers). Many performance tests under `tests/perf/` also use Docker; perf tests without container fixtures (e.g. codec benchmarks) run in-process. Default CI (`just test`) excludes `-m perf`.
- The package version is derived from git tags via `hatch-vcs`; importing `forze.__version__` does not work—use `forze._version.__version__` instead.
- `uv sync` is called automatically by `justfile` recipes before test/quality commands, so manual re-sync is rarely needed.

<!-- BEGIN sqz-claude-guidance (auto-installed by sqz init; remove this block to disable) -->

## sqz — Context Compression (READ FIRST)

sqz is installed in this project. It compresses tool output so large
files, long logs, and verbose command output cost far fewer tokens.
There are **two ways** sqz is wired in, and you should prefer each
one in the situations below.

### Preferred tools (MCP)

The `sqz-mcp` server is registered in this project's MCP config. It
exposes three read-only tools that compress their output through the
sqz pipeline:

- **`sqz_read_file`** — read a file from disk and return a compressed
  view. **PREFER this over the built-in `Read` tool** for any file
  larger than ~2KB or any file you might read more than once in the
  same session. Repeat reads return a 13-token `§ref:HASH§` reference
  instead of the full content.

- **`sqz_grep`** — search files for a literal string or regex.
  **PREFER this over the built-in `Grep`** for anything that might
  match more than a handful of lines. Caps at 200 matches by default;
  raise with `max_matches` if needed.

- **`sqz_list_dir`** — list a directory. Skips `.git`, `node_modules`,
  `target`, `dist`, `build`, `vendor`, `__pycache__` so the output
  stays focused. **PREFER this over `ls -la` via Bash** when you want
  to see a project layout.

The built-in `Read`, `Grep`, `Glob` tools remain available. Use them for:
- Tiny config files (<1KB) where compression can't help.
- Byte-exact reads you'll hash or diff (lockfiles, signatures).
- Globbing (sqz has no glob tool; `Glob` is still the right choice).

### Bash commands (hooked automatically)

When you run a shell command through the `Bash` tool, a PreToolUse hook
rewrites it to pipe output through `sqz compress`. This is transparent:
you don't need to remember to add anything, but it's useful to know
that these commands get compressed automatically:

```bash
git status           # → git status 2>&1 | sqz compress --cmd git
cargo test           # → cargo test 2>&1 | sqz compress --cmd cargo
docker ps            # → docker ps 2>&1 | sqz compress --cmd docker
kubectl get pods     # → kubectl get pods 2>&1 | sqz compress --cmd kubectl
```

The rewrite is skipped for interactive commands (`vim`, `ssh`,
`python`), compound commands (`a && b`, `a > file.txt`), and anything
already going through sqz.

### Escape hatch — when you see a `§ref:HASH§` token

If tool output contains a `§ref:a1b2c3d4§` token and you need the full
content it points at, resolve it. Three equivalent ways:

- Shell: `/usr/local/bin/sqz expand a1b2c3d4` (or paste the whole token
  `/usr/local/bin/sqz expand §ref:a1b2c3d4§`).
- MCP tool: call `expand` with `{ "prefix": "a1b2c3d4" }`.
- To get uncompressed output for one command: prefix it with
  `SQZ_NO_DEDUP=1` (e.g. `SQZ_NO_DEDUP=1 git log | sqz compress`).

If the compressed output is actively making the task harder (looping
on refs, small retries replacing one big read), call the `passthrough`
MCP tool to get raw text.

### When NOT to use sqz tools

- Writing or editing files — use the built-in `Write`/`Edit` tools.
  sqz has no write tools (by design; see issue #5 follow-up).
- Running commands interactively or in watch mode.
- Reading very small files (<1KB) where compression can't help.

<!-- END sqz-claude-guidance -->

<!-- BEGIN sqz-agents-guidance (auto-installed by sqz init; remove this block to disable) -->

## sqz — Token-Optimized CLI Output

When running shell commands whose output may be long (directory listings,
git log/diff, test runners, build logs, `docker ps`, `kubectl get`, etc.),
pipe the output through `sqz compress` to reduce token consumption.

`sqz` is a stdin-to-stdout compressor, not a command wrapper. The correct
usage is to pipe the command's output into `sqz compress`:

```bash
# Instead of:     Use:
git status        git status 2>&1 | /usr/local/bin/sqz compress
cargo test        cargo test 2>&1 | /usr/local/bin/sqz compress
git log -10       git log -10 2>&1 | /usr/local/bin/sqz compress
docker ps         docker ps 2>&1 | /usr/local/bin/sqz compress
ls -la            ls -la 2>&1 | /usr/local/bin/sqz compress
```

The `2>&1` captures stderr too, which is useful for commands like `cargo
test` where diagnostics go to stderr. `sqz compress` filters and compresses
the combined output while preserving filenames, paths, and identifiers.
It typically saves 60-90% tokens on verbose commands.

Do NOT pipe output for:
- Interactive commands (`vim`, `ssh`, `python`, REPLs)
- Compound commands with shell operators (`cmd && other`, `cmd > file.txt`,
  `cmd; other`) — run those directly
- Short commands whose output is already a few lines

If `sqz` is not on PATH, run commands normally.

The `sqz-mcp` MCP server is also available — Codex reads it from
`~/.codex/config.toml` under `[mcp_servers.sqz]`. It exposes three
tools: `compress` (the default pipeline), `passthrough` (return text
unchanged — the escape hatch below), and `expand` (resolve a
`§ref:HASH§` token back to the original bytes).

## Escape hatch — when sqz output confuses you

If you see a `§ref:HASH§` token and can't parse it, or compressed
output is leading you to make lots of small retries instead of one
big request, use one of these:

- **`/usr/local/bin/sqz expand <prefix>`** — resolve a dedup ref back to the
  original bytes. Accepts bare hex (`sqz expand a1b2c3d4`) or the full
  token pasted verbatim (`sqz expand §ref:a1b2c3d4§`).
- **`SQZ_NO_DEDUP=1`** — set this env var for one command to disable
  dedup: `SQZ_NO_DEDUP=1 git status 2>&1 | sqz compress`. You'll get
  the full compressed output with no `§ref:…§` tokens.
- **`--no-cache`** — same opt-out as a CLI flag:
  `git status 2>&1 | sqz compress --no-cache`.

If you're using the MCP server, the `passthrough` tool returns raw
text and the `expand` tool resolves refs — call them when you need
data sqz hasn't touched.

<!-- END sqz-agents-guidance -->
