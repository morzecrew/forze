# RFC 0008 — Docs language switcher (en default, ru alternate)

- **Status:** 📝 Draft
- **Related:** [`pages/zensical.toml`](../pages/zensical.toml) (the config being split), the root [`justfile`](../justfile) docs recipes (`serve-docs`, `build-docs`), `.github/workflows/docs-dev.yaml` + `docs-release.yaml` and `deploy-docs-version.sh` (the publish path `mike --deploy-prefix ru` extends).
- **Scope:** `pages/` + docs CI only. Zero `src/` changes.
- **Depends on:** nothing. Composes with the existing mike versioning setup.

## 0. Summary

Add a Russian translation of the docs site with the theme's header language
switcher, keeping English as the default. The instructions this RFC was asked
to evaluate describe a Zensical i18n feature set (`[project.theme.languages]`
dictionary, folder-based localization inside one `docs/` tree, automatic asset
fallback) that **does not exist** in the Zensical we build with (0.0.50). What
does exist — verified in the installed package — is the Material-style
`extra.alternate` static switcher plus a fully independent build per language.

The plan therefore is the classic Material-for-MkDocs multi-language shape,
adapted to this repo's mike-versioned deployment:

- two configs (`zensical.toml` en + `zensical.ru.toml` ru), two builds;
- ru content in a sibling tree `pages/docs_ru/` mirroring `pages/docs/`,
  sharing generated/static assets via symlinks;
- ru deployed with the same mike flow under `--deploy-prefix ru`, so URLs
  compose as `/forze/<version>/…` (en) and `/forze/ru/<version>/…` (ru), each
  prefix with its own `versions.json` and version selector;
- the header switcher via `[[project.extra.alternate]]` in both configs;
- an AI-driven translation pipeline with a glossary, mechanical rules, and a
  drift manifest so stale translations are detectable, not silent.

## 1. Assumption audit of the provided instructions

Every claim checked against the installed toolchain:
`zensical 0.0.50` (`.venv/.../zensical/`) and the pinned mike fork
`2.2.0+zensical-0.1.0` (`squidfunk/mike @ 2d4ad79`, see `pyproject.toml`
`[tool.uv.sources]`).

| # | Claim in the instructions | Verdict | Evidence |
|---|---|---|---|
| 1 | `[project.theme.languages]` dictionary registers languages and "natively enables the drop-down switcher" | ❌ **No such config key.** The theme has a single `theme.language` string (default `"en"`); nothing in the config model reads a `languages` table | `zensical/config.py:513` — `set_default(theme, "language", "en", str)`; no other `theme.languages` handling anywhere in the package |
| 2 | A language drop-down exists in the header | ✅ **True, but driven differently.** The drop-down renders iff `config.extra.alternate` is set — a list of `{name, link, lang}` entries, i.e. static links you author yourself | `templates/partials/header.html:45` (`{% if config.extra.alternate %}`), `templates/partials/alternate.html:12` |
| 3 | "Strict folder-based localization": put `de/`, `es/` clones inside `docs/` and Zensical routes them | ❌ **Not a Zensical feature.** A build has exactly one nav, one `theme.language`, one search index. Subfolders inside `docs/` are just more pages of the *English* build: English header/nav/UI chrome, mixed search index, and orphan-page warnings for anything not in the explicit `nav` (ours is fully explicit in `zensical.toml`) | `pages/zensical.toml` nav block; no per-folder language logic in `zensical/config.py` or templates |
| 4 | "Do not duplicate `assets/`; Zensical provides automatic asset fallbacks, reading media from the parent root if missing in sub-folders" | ❌ **No such mechanism.** Nothing in the package implements parent-directory asset fallback. (This describes the `mkdocs-static-i18n` plugin's behavior — a MkDocs plugin, not applicable: Zensical is not plugin-compatible with it) | grep over the installed package: no fallback resolution code |
| 5 | `site_url` "required for proper language routing" | ⚠️ **Misleading.** `site_url` matters for canonical URLs, sitemap and the version selector base — there is no "language routing" for it to feed. Each language build gets its **own** `site_url` (see §3.2) | `templates/base.html:30-32` uses `alternate` entries verbatim via the `url` filter |
| 6 | Config example (`name = "AI Translated Docs"` under `[project]`) | ⚠️ Wrong schema — Zensical uses `site_name`, as our existing config does | `pages/zensical.toml:2` |

Also missing from the instructions, and load-bearing for this repo:

- **Versioning.** Docs deploy through mike (`extra.version.provider = "mike"`,
  `docs-release.yaml` / `docs-dev.yaml`): every deploy is a snapshot under
  `/<minor>/` with `latest`/`dev` aliases. A language scheme that ignores this
  produces a switcher that breaks on every versioned URL.
- **Explicit nav with English labels** in `zensical.toml` — group labels
  ("Get Started", "Learn", …) are config strings, so a ru build needs its own
  nav, not just translated pages.
- **Generated assets**: `docs/_diagrams/` is produced by `just build-diagrams`
  (gitignored), so "clone the docs folder" would either miss diagrams or
  duplicate a build product.
- **English injected content**: `pymdownx.snippets.auto_append` pushes
  `_includes/abbreviations.md` (English `<abbr>` expansions) into *every* page
  of a build.

One piece of good news verified: Zensical ships complete Russian UI
translations (`templates/partials/languages/ru.html` — search box, nav labels,
"Выберите язык", etc.), activated by `theme.language = "ru"`.

## 2. What the toolchain actually gives us

Facts the design builds on (all verified in the installed packages):

1. **Switcher** = `config.extra.alternate` list → header drop-down
   (`partials/alternate.html`) + SEO `<link rel="alternate" hreflang=…>` tags
   on every page (`base.html:30-32`). Links are static; no page-context
   preservation out of the box.
2. **Per-language chrome** = one build per language with its own
   `theme.language`, nav, search index, sitemap.
3. **`zensical build -f/--config-file PATH`** exists → multiple configs in one
   `pages/` directory work.
4. **The pinned mike fork understands Zensical natively**: it discovers
   `zensical.toml`, runs `zensical build --clean --config-file <cfg>`
   (`mike/utils.py:44-52`), and supports `-F/--config-file` plus
   `--deploy-prefix` (`mike/driver.py:111-113`) — everything scoped per
   prefix, **including a separate `versions.json`**, which is exactly the
   isolation a second language needs.

## 3. Design

### 3.1 URL layout

```
/forze/                     → root redirect → /forze/<latest>/     (exists today)
/forze/0.6/…                → English, version 0.6                 (exists today)
/forze/dev/…                → English, main channel                (exists today)
/forze/ru/                  → root redirect → /forze/ru/<latest>/  (new)
/forze/ru/0.6/…             → Russian, version 0.6                 (new)
/forze/ru/dev/…             → Russian, main channel                (new)
```

Language is the **outer** axis (mike deploy prefix), version the inner one.
Each axis keeps its own selector: mike maintains `versions.json` per prefix,
so the version drop-down inside `/ru/` lists ru snapshots only.

### 3.2 Two configs

`pages/zensical.toml` stays as-is (English) plus the `extra.alternate` block.
New `pages/zensical.ru.toml`:

- `site_url = "https://morzecrew.github.io/forze/ru/"`
- `theme.language = "ru"` (activates bundled ru UI strings)
- `docs_dir = "docs_ru"`, `site_dir = "site_ru"` (no clobbering of local
  `site/`), `dev_addr = "localhost:8046"` (parallel local serve)
- nav: same page paths, **Russian group labels**
- `pymdownx.snippets`: same `base_path` (snippets from `../examples` stay in
  English code — correct), but `auto_append = ["_includes/abbreviations.ru.md"]`
- same `extra.version` block (mike provider) and theme/palette/features
- everything else copied from the en config

Both configs get:

```toml
[[project.extra.alternate]]
name = "English"
link = "/forze/"
lang = "en"

[[project.extra.alternate]]
name = "Русский"
link = "/forze/ru/"
lang = "ru"
```

v1 links point at the language **root** (each root redirects to `latest` in
that language) — switching language drops page/version context. That is what
Material's own multi-language sites do; a JS enhancement that rewrites the
link to the mirrored current page is Phase 4.

**Config duplication is accepted** (TOML has no include), guarded by a parity
check (§3.5). Extracting a shared generator script is possible later if drift
actually bites.

### 3.3 Content tree

```
pages/
├── zensical.toml            # en
├── zensical.ru.toml         # ru
├── docs/                    # en content (unchanged)
│   ├── _images/  _diagrams/  _javascripts/  _stylesheets/
│   └── **/*.md
├── docs_ru/                 # ru content, mirrors docs/**/*.md
│   ├── _images      → symlink ../docs/_images
│   ├── _diagrams    → symlink ../docs/_diagrams   # picks up build-diagrams output
│   ├── _javascripts → symlink ../docs/_javascripts
│   ├── _stylesheets → symlink ../docs/_stylesheets
│   └── **/*.md                                    # translated
└── _includes/
    ├── abbreviations.md
    └── abbreviations.ru.md
```

Symlinks give the "shared assets" the instructions wrongly attributed to
Zensical: one copy on disk, both builds see them, `build-diagrams` output
lands once. (Git and ubuntu runners handle symlinks fine; if Windows
contributors ever matter, swap for a copy step in the just recipe — decision
recorded, not blocking.)

D2 diagrams contain English labels; v1 ships them untranslated (they are
architecture vocabulary that the glossary keeps in English anyway).
Per-language diagram builds would mean translated `.d2` sources — explicitly
out of scope.

### 3.4 Build & deploy

Local (`justfile`):

- `serve-docs-ru`: `uv run zensical serve -f zensical.ru.toml`
- `build-docs-ru`: `build-diagrams` + `uv run zensical build -f zensical.ru.toml`
- optional `build-docs-all`: both builds + copy `site_ru` → `site/ru` for a
  combined local preview mimicking production paths.

CI — extend the existing scripts, same flow twice:

- `deploy-docs-version.sh`: after the en deploy, run
  `uv run mike deploy --push [--update-aliases] --branch gh-pages -F zensical.ru.toml --deploy-prefix ru "$minor" [latest]`
  and, when `move_latest`, `mike set-default --push --branch gh-pages --deploy-prefix ru latest`.
- `docs-dev.yaml`: same for the `dev` channel.
- The `docs-deploy` concurrency group already serializes pushes; the two
  deploys per run just extend the critical section.

**No backfill**: only versions released after this ships get a ru snapshot.
The ru `versions.json` starts short; that is correct, not a bug.

### 3.5 Guardrails

- **Nav parity check** (add to docs CI / `just quality` docs step): parse both
  configs with `zensical.config.parse_config`, assert the *set of page paths*
  in both navs is identical, and that every nav path exists under its
  `docs_dir`. Catches "added an en page, forgot the ru mirror" at build time
  instead of as a 404.
- **Strict builds** (`zensical build -s`) in CI for both languages, so orphan
  pages / broken snippet paths fail loud.
- **hreflang**: comes for free from `extra.alternate` (root-level only in v1).

## 4. Translation pipeline (AI-driven)

Mechanical rules for the translation pass (these become a prompt/skill doc
under `pages/` or `.claude/skills/` when Phase 1 starts):

1. **Never translate**: code blocks, inline code, snippet include directives
   (`--8<--` paths), file paths, port/class/config identifiers, D2 diagram
   references, front-matter keys.
2. **Glossary** of Forze terms that stay English (aggregate, outbox, saga,
   port, adapter, tenant, DST, …) with the sanctioned ru phrasing around them.
   One file, versioned next to the content, fed to every translation run.
3. **Anchors shift**: `toc.permalink` slugs derive from headings, so ru pages
   get ru slugs. In-tree relative links between ru pages must use ru anchors;
   the strict build plus a link checker over `site_ru` catches misses.
4. **Admonition titles, tab labels, alt text**: translate.
5. **Changelog** (`get-started/changelog.md` includes `CHANGELOG.md` via
   snippet): stays English inside a ru shell page — translating a changelog
   is a treadmill with negative ROI.

**Drift tracking** — the part that keeps this honest long-term:
`pages/i18n-status.json` maps each en page path → the git blob hash of the en
source at translation time. A `just docs-i18n-status` recipe diffs current
hashes against the manifest and prints three buckets: missing / stale /
current. The AI retranslation loop consumes exactly that list; CI can warn
(not fail) on staleness so translation lag never blocks an en docs fix.

## 5. Alternatives considered

- **A. Single build, `docs/ru/` inside the en tree** (what the instructions
  describe): rejected — English nav/UI/search for ru readers, orphan-page
  warnings, no per-language sitemap/hreflang. It only *looks* cheaper.
- **B. `mkdocs-static-i18n`-style plugin**: rejected — Zensical does not run
  MkDocs plugins; no Zensical-native i18n plugin exists at 0.0.50.
- **C. Separate repo/site for ru**: rejected — kills the switcher, doubles CI
  and versioning plumbing for nothing.
- **D. Wait for native Zensical i18n**: Zensical is pre-1.0 and may grow this;
  nothing in this design fights a future native feature (the content mirror
  and glossary transfer verbatim; the config split collapses back).

## 6. Risks & open questions

1. **Zensical 0.0.x is a moving target** — the fork-pinned mike + `>=0.0.43`
   floor means behavior can shift under us; the parity/strict checks are the
   canary. Re-verify `extra.alternate` handling on Zensical upgrades.
2. **Cyrillic search quality**: `theme.language = "ru"` sets the search
   language, but verify tokenization/stemming quality on the built ru site
   during Phase 1 pilot review; acceptable-but-imperfect search is fine for
   v1.
3. **Hero/overrides**: `overrides/main.html`, `hero.js`, and landing-page
   markup may embed English strings outside markdown. Audit during Phase 1;
   worst case the ru landing page carries its own hero copy in
   `docs_ru/index.md`.
4. **Nav-label-only drift** (paths match, ru labels stale after an en rename):
   parity check covers paths, not labels. Accepted residual; labels are ~40
   strings reviewed whenever nav changes.
5. **Scope lever**: nothing forces full coverage — the ru nav is independent,
   so a reduced ru tree (e.g. Get Started + Learn first) is legal. Given the
   translation is AI-driven, the recommendation is full-tree from day one
   (partial coverage creates cross-link 404 headaches that cost more than the
   extra translation), with human review effort tiered toward high-traffic
   pages.

## 7. Phasing

- **P1 — Scaffolding + full translation pass.** `zensical.ru.toml`, `docs_ru/`
  with asset symlinks, `abbreviations.ru.md`, glossary, AI translation of the
  full tree, `serve-docs-ru`/`build-docs-ru` recipes, nav parity check,
  `extra.alternate` in both configs. Exit: both builds green in strict mode
  locally, switcher works between local sites.
- **P2 — CI wiring, dev channel first.** Extend `docs-dev.yaml` → verify
  `/forze/ru/dev/` end-to-end on gh-pages (switcher, version selector,
  root redirect via `set-default --deploy-prefix ru`). Then extend
  `deploy-docs-version.sh` + `docs-release.yaml`. Exit: next release publishes
  both languages.
- **P3 — Drift tooling.** `i18n-status.json` manifest, `docs-i18n-status`
  recipe, CI staleness warning, retranslation loop doc.
- **P4 (optional) — polish.** JS page-preserving language switch; per-page
  hreflang pairs; translated D2 diagrams if ever demanded.

## 8. Decisions

| # | Decision |
| --- | --- |
| 1 | **Two configs, two builds** (`zensical.toml` over `docs/`, `zensical.ru.toml` over `docs_ru/`). Zensical 0.0.50 ships no native i18n and runs no MkDocs plugins, so `mkdocs-static-i18n`-style approaches are unavailable, not merely unchosen |
| 2 | The switcher is `extra.alternate` declared in **both** configs; ru deploys via `mike --deploy-prefix ru` with its own `versions.json`, so language and version selection stay independent |
| 3 | A single build with `docs/ru/` inside the en tree is rejected: ru readers would get English nav/UI/search, the build emits orphan-page warnings, and there is no per-language sitemap or hreflang. It only *looks* cheaper |
| 4 | A separate repo or site for ru is rejected — it kills the switcher and doubles CI and versioning plumbing for nothing |
| 5 | Nav parity is checked **by path, not by label**. Label drift after an en rename is an accepted residual (~40 strings, reviewed whenever nav changes) |
| 6 | Translation is AI-driven against a fixed glossary, with an `i18n-status.json` drift manifest and a CI staleness warning. Full-tree coverage from day one: partial coverage creates cross-link 404s that cost more than the extra translation |
| 7 | Nothing here fights a future native Zensical i18n — the content mirror and glossary transfer verbatim and the config split collapses back. Consequence: the design may be *replaced* cheaply, so no effort is spent making it permanent |
