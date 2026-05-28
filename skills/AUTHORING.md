# Authoring Forze Agent Skills

This file is for **maintainers** of `skills/` in the Forze repository. It is not published as an installable skill (`npx skills add` only picks up `*/SKILL.md`).

## Audience

Skills target engineers building **applications** that depend on Forze (`forze`, `forze_postgres`, `forze_inngest`, etc.) from PyPI — not contributors changing the Forze monorepo.

| Teach in skills | Do not teach in skills |
|-----------------|------------------------|
| Logical specs, handlers, ports, wiring, integration extras | Moving code between `forze` and integration packages |
| `DepsPlan`, built-in `*DepsModule`, `forze_mock` tests | Import-linter contracts, CHANGELOG, CI, `AGENTS.md` workflow |
| Custom `DepsModule` in **your app** (advanced skill) | Reading `src/forze_*` to implement framework adapters |

Framework contributors should use [`AGENTS.md`](../AGENTS.md), [canonical docs](https://morzecrew.github.io/forze/), and [`.claude/skills/`](../.claude/skills/).

## Vocabulary

| Prefer | Avoid |
|--------|--------|
| logical spec / application spec | kernel spec (unless quoting API names) |
| integration package (`forze_inngest`) | adapter package vs core contracts |
| your application / service | this repository |
| shipped `forze_*` package | repo layout under `src/` |

## Links

- **Published docs:** `https://morzecrew.github.io/forze/docs/...` (see [`pages/mkdocs.yml`](../pages/mkdocs.yml) `site_url`). Use trailing-slash paths, e.g. `.../docs/integrations/inngest/`.
- **Cross-skill:** relative paths only, e.g. [`forze-wiring`](forze-wiring/SKILL.md).
- **Never** link to `../../src/`, `../../tests/`, or `../../pages/` — installed skills are copied outside the Forze repo and those paths break.

## Skill structure

Each `SKILL.md` should include:

1. YAML frontmatter: `name`, `description` (when the agent should load it — app-focused).
2. Body: patterns, minimal examples, gotchas.
3. **Anti-patterns** — only mistakes an **app team** can make (not monorepo package boundaries).
4. **Reference** — at least one published doc URL plus sibling skills when relevant.

Optional **Gotchas** section for migration notes and debugging.

## Anti-patterns policy

**Include:** adapter imports in handlers; missing `route=spec.name`; unfrozen registry; binding identity inside handlers; wrong port for the integration.

**Exclude:** “keep X in `forze_inngest` not core contracts”; pointers to Forze `tests/`; “no core contract changes” (reframe as “use `TokenVerifierPort` from the OIDC package”).

## Dependency skills

- **`forze-deps-consumption`** — default: plain vs routed deps, built-in modules, debugging merge conflicts.
- **`forze-custom-deps`** — advanced: private `DepsModule` / `DepKey` in the application.

Do not publish `forze-deps-modules` (retired name).

## Adding or changing a skill

1. Add `skills/<name>/SKILL.md` with app-scoped `description`.
2. Link integration topics to an existing page under `pages/docs/` (add a doc page if missing).
3. Update [`skills/README.md`](README.md) and root [`README.md`](../README.md) Agent Skills table.
4. Grep: `rg '../../(pages|src|tests)' skills/` should return nothing in `SKILL.md` files.
