---
name: altitude-docs
description: Write or polish Forze documentation pages to a consistent, production-grade standard using the altitude model (a deliberate high-level → low-level descent), per-archetype page contracts, a shared consistency layer, and a ship rubric. Use when creating, revising, reviewing, or aligning any page under pages/docs/ — and when deciding where new doc content belongs.
---

# Altitude docs

A method for writing and polishing Forze's documentation so every page reads
like it was written by one careful author: it opens at the right height,
descends from plain-language orientation to concrete detail **in order**, hands
the reader on cleanly, and reaches for a component only when the component earns
its place.

The core idea is **altitude**: each page is a controlled descent through fixed
bands, from "why this exists" down to "the exact edge case". Each kind of page
is allowed a different *altitude range* — a tutorial stays high, a reference
page sits low, an explanation traverses most of the span. "Balanced" means the
page covers its range and never inverts or skips a band.

## Use this skill when

- Writing a new page under `pages/docs/`.
- Polishing or reviewing an existing page for consistency, depth, or flow.
- Aligning a whole archetype directory (e.g. `get-started/`) to one standard.
- Deciding which archetype — and therefore which directory — new content belongs in.

Two modes:

- **Author** — a page does not exist yet. Run the full procedure from step 1.
- **Polish** — a page exists. Run the procedure as a diff: identify the archetype,
  find where the page leaves its altitude range or breaks a contract, fix only that.
  Do not rewrite a page that already passes the rubric.

This skill governs *how* a page is built, not *what is true*. Every API name,
symbol, and behavior must be verified against `src/` (see Accuracy). Never invent
or copy an API from an old draft.

## The procedure

1. **Identify the archetype** from the target directory (or, for new content,
   from the reader's need — see Diátaxis fit below). This fixes the altitude
   range, the page contract, and the handoff style.
2. **Set the altitude range** for that archetype (the table below). Note the
   entry band and the floor.
3. **Apply the page contract** — opening job, section skeleton, floor, handoff,
   allowed components, code policy.
4. **Apply the consistency layer** — voice, opening, handoff, component
   discipline, code, diagrams, accuracy. These ride on top of every contract.
5. **Run the ship rubric.** A page ships only when all ten checks pass.
6. **Verify the build** — clean build, "No issues found" (see Build & verify).

## Part 1 — The altitude model

Every page descends through these five bands, **in order**. A page may start
below band 1 and may stop early, but it must never jump back up or skip a
required band on the way down.

1. **Orientation** — why this exists, what problem it removes. Plain language,
   no API names. The reader learns whether they're on the right page.
2. **Mental model** — the shape of the thing. One diagram, one analogy, or two
   to three sentences. Concept nouns only (Aggregate, port, operation).
3. **The shape in code** — the smallest *real* anchor. One canonical snippet,
   ideally a `--8<--` include from `examples/`. The reader sees it concretely.
4. **Mechanics** — the moving parts: the API surface, the table of
   methods/options, how it actually runs.
5. **Edge & operational detail** — caveats, failure modes, tuning, exhaustive
   lookup. The reader resolves a specific question.

Diátaxis fit (one page = one job; never mix two jobs on a page):

- Tutorial → learning, study, practical (`get-started/`).
- How-to → a task, work, practical (`recipes/`, `integrations/`).
- Explanation → understanding, study, theoretical (`core-concepts/`, `in-depth/`).
- Reference → information, work, theoretical (`reference/`).

### Altitude range per archetype

| Archetype | Enters at | Floors at | Must NOT |
|---|---|---|---|
| `get-started/` (tutorial) | 1 | 3 (touch 4 once) | bottom out at 5; enumerate edge cases |
| `core-concepts/` (explanation) | 1 | 4 | dump raw lookup; dip to 5 except its one subject |
| `in-depth/` (deep explanation) | 1 | 4, dips to 5 on its subject | become a reference dump or a recipe |
| `recipes/` (how-to) | 1 (**one line**) | 4 | rebuild the mental model (band 2) — link to Learn |
| `integrations/` (reference-practical) | 1 (**one line**) | 4 | go conceptual (band 2) |
| `reference/` (reference) | band-1 one-liner, then 4 | 5 | narrative progression; "where next" footers |

"Imbalanced" failures are now nameable: a page that is all narrative has no
band 3 anchor; a page that opens on code skipped bands 1–2; a how-to that
re-teaches concepts dipped into band 2 it should have linked instead.

## Part 2 — Page contracts (Forze docs profile)

Universal, every page:

- Frontmatter has `title`, `summary`, and `icon`. No exceptions.
- Headings are sentence case ("What you just did", not "What You Just Did").
- Body starts at `##` — no explicit `# H1` (avoids markdownlint MD025).
  `index.md` is the only exception.
- One Diátaxis job per page.

**`get-started/` (tutorial)**
- *Opening:* lead paragraph naming the outcome ("by the end you have X running").
- *Skeleton:* Orientation → Prerequisites → numbered Steps → "What you just did"
  (a band-2 retro that names the concepts they just used) → handoff.
- *Floor:* band 3, one controlled dip to 4. *Handoff:* grid cards into Learn.
- *Code:* heavy, but every block is preceded by a one-line "why".

**`core-concepts/` and `in-depth/` (explanation)**
- *Opening:* required 2–4 sentence lead — what the page is and where it sits.
- *Skeleton:* topic-driven; do **not** force a uniform heading set.
- *Floor:* band 4 (`in-depth/` dips to 5 on its single subject).
- *Components:* a D2 diagram near the top to anchor band 2 (core-concepts does
  this on every page); an admonition for the one rule that matters most.
- *Handoff:* a closing sentence that bridges to the related idea via an inline
  cross-link (one or two links, woven into prose). The global prev/next footer
  already handles linear navigation, so the closer earns its place by adding a
  *conceptual* link — not by restating the next page. A `## See also` list fits
  when 3+ pages are genuinely related. Grid cards are reserved for true
  multi-destination pages (a section map, a cross-section CTA), never an ordinary
  explanation page.

**`recipes/` (how-to)**
- *Opening:* one-line problem statement, then point to the runnable
  `examples/recipes/<name>/`.
- *Skeleton:* narrative task steps (problem → model → wire → invoke → Notes).
- *Floor:* band 4. *Code:* `--8<--` from the example, never scratch code.
- *Handoff:* end at the task's natural close (commonly `## Notes`, `## Run it`,
  or a final caveat), and add a forward inline cross-link where a genuine next
  recipe exists. A `## Where next` grid card is optional — use it only for a hub
  recipe that heads a clear chain (e.g. CRUD → cache → idempotency). Do not
  manufacture sibling links to satisfy a template.

**`integrations/` (reference-practical)**
- *Opening:* exactly one band-1 sentence — what it provides and when to reach
  for it ("Document storage, search, and transactions on PostgreSQL — use it
  when you want one relational store behind documents and search.").
- *Skeleton (rigid):* Install → The client → Wire it → What it provides → Notes.
  Inbound transports (fastapi, socketio, http) swap "The client" for "Build the
  server" / "Route to operations". Backend extras (locks, push invalidation)
  come *after* the core five.
- *Floor:* band 4. *Handoff:* none — end at Notes. No footer.

**`reference/` (reference)**
- *Opening:* a band-1 one-liner, then pure lookup.
- *Skeleton:* bespoke per page (glossary, taxonomy, syntax) — this is correct.
- *Components:* tables. No diagrams, no narrative progression, no footer.

When unsure which archetype new content is, ask: is the reader *studying* or
*working*, and do they want *understanding* or *information/steps*? That places
it. If a draft wants to both teach a concept and give steps, it is two pages.

### Front-door pages (the appeal layer)

`index.md`, `get-started/introduction.md`, and the opening of `quickstart.md`
carry an extra job beyond orientation: they must make a skeptical engineer
*want* to continue. This appeal layer applies on these pages only — never on
reference, integration, or how-to pages, where punch is noise.

- **Lead with the problem, not the product.** The opening states the pain the
  reader recognizes; do not describe Forze before the first "what is it"
  section. The solution belongs in the sections that follow.
- **Contrast over adjectives.** Show the before/after — an *Instead of / With
  Forze* table, a concrete swap — not "powerful, flexible, modern".
- **Calm confidence, honesty as appeal.** Keep a "when not to use it". No hype.
- **Punch is allowed here** because these are prose pages. Everywhere else the
  structural contracts govern and punch is out of place.

Appeal is a distinct axis from altitude: a page can be altitude-perfect and dull.
These pages must pass both.

## The consistency layer

- **Voice:** second person, present tense, active voice; conversational but
  precise. ("You wire the module", not "The module is wired".)
- **Opening contract:** explanation and how-to require a lead paragraph;
  reference and integration require a one-line orienter. No page opens cold on
  a heading.
- **Handoff contract (one rule per archetype, no ad-hoc mixing):**
  grid cards for `get-started/` (a linear journey); recipes end at the task's
  natural close, with an optional `## Where next` grid card only for a hub recipe
  that heads a chain; an inline forward cross-link woven into a closing sentence
  for explanation (`core-concepts/`, `in-depth/`), or a short `## See also` list
  when 3+ pages are related; nothing for `reference/` and `integrations/` (end at
  last content).
- **Component discipline — the earns-its-place test:** keep a tab/card/admonition/
  diagram only if removing it loses information. If removing it loses only
  decoration, cut it. No patchwork of components; no walls of unbroken prose.
- **Code policy:** prefer `--8<--` includes of test-backed `examples/` over inline
  scratch code; inline only the smallest illustrative fragment; never whole-file
  includes (the marker comments would render).
- **Diagram policy:** D2 over Mermaid; one idea per diagram; balance edge
  directions to avoid the tall/wide blow-out (keep under ~4:1; flip
  `direction:` or shorten labels when three boxes in a row stretch out).
- **Accuracy:** every API symbol verified against current `src/` before it ships.
  Errors use `from forze.base.exceptions import exc` then `exc.<kind>(...)` — there
  is no `forze.base.errors` and no `ValidationError` symbol. Use the canonical
  vocabulary (appendix) consistently.
- **External citations:** the first time a page leans on an outside concept (DDD,
  Hexagonal Architecture, an RFC, a paper), link the canonical source with a
  tooltip title that glosses it in one line —
  `[Hexagonal Architecture](url "Alistair Cockburn — the original write-up")`.
  Strongest on explanation and front-door pages; don't litter every page.
- **Abbreviations:** define each non-obvious abbreviation once in
  `pages/_includes/abbreviations.md` (auto-appended site-wide, so any occurrence
  renders as an `<abbr>` tooltip — no per-page work). Skip anything a backend
  developer already knows (HTTP, API, SQL, URL, SDK, JWT, TTL, DTO, DSL) — define
  only the specialized or ambiguous ones where the expansion earns the tooltip.
  The include is self-contained: the tooltips are the lookup, so there is no
  separate glossary page or mirrored table to maintain.
- **No meta:** never tell the reader a page is test-backed, that "the example is
  the spec", or reference test files. Frame sections by behavior, not by how the
  docs are built.

## Ship rubric (definition of done)

A page ships only when all pass:

1. Single Diátaxis job — no mixing.
2. Frontmatter complete: `title` + `summary` + `icon`.
3. Opens per its opening contract (lead paragraph or one-line orienter).
4. Descends its altitude range in order — no inversion, no skipped required
   band, does not bottom out below its floor.
5. Every component earns its place.
6. Code is example-sourced where it can be; inline code is minimal.
7. Ends per its handoff contract.
8. Voice is second person / present / active; headings are sentence case.
9. Every API symbol verified against current `src/`.
10. Clean build passes (see below); warm-build link counts are ignored.

## Build & verify

- `just build-diagrams` — renders D2 sources to `docs/_diagrams/{light,dark}/`.
  Include a diagram page-relative: `../_diagrams/light/<name>.svg#only-light`
  (and the `#only-dark` twin).
- `just serve-docs` — live reload at `localhost:8045`.
- **Authoritative build check:** from `pages/`, `rm -rf site .cache && uv run
  zensical build` → expect "No issues found". Warm/incremental builds emit
  spurious, fluctuating "page does not exist" counts — they are noise. Only a
  clean build's count is real.
- **Snippets:** `pymdownx.snippets` is configured with
  `base_path = ["docs", "../examples", ".."]` and `check_paths = true` (a bad
  path fails the build). In an example file add inert markers
  `# --8<-- [start:name]` / `# --8<-- [end:name]`; reference inside a fenced
  block: `--8<-- "recipes/<name>/app.py:name"`.

## Appendix A — Canonical vocabulary

Use consistently; verify symbols against `src/` before asserting specifics.

- **Aggregate** — the central domain concept. Build one by subclassing `Document`
  (persistence base: id/rev/timestamps + invariants/update validators) and
  optionally `AggregateRoot` (records domain events). Do **not** call `Document`
  "the central entity" — `Document` is the persistence base; *Aggregate* is the
  concept. `Document`/`AggregateRoot` are siblings under `CoreModel`.
- **Specification** in prose; `…Spec` in code (`DocumentSpec`, `SearchSpec`).
- **Handler** carries business logic. **Operation** is the named, registered unit
  in the registry (handler + stage hooks); what `run_operation()` runs.
  **ResolvedOperation** is the operation materialized against an
  `ExecutionContext` at run time. Overview-level prose uses just "operation".
- **Port** pairs with **Adapter**. In prose call a port a **contract**, never a
  "protocol".
- Say "stage hooks on the operation registry" (not "operation plans") and
  "dependency registry" (not "dependency container").

## Appendix B — Zensical component matrix

Verified to render in this setup:

- Content tabs: `=== "Label"`.
- Grid cards: `<div class="grid cards" markdown>` … `</div>`.
- Grid cards with hyperlinks: `<div class="grid cards fz-cards" markdown>` … `</div>`.
- Admonitions: `!!! note` and collapsible `??? question`.
- Buttons: `[Text](url){ .md-button .md-button--primary }`.
- Lucide icons: `:lucide-zap:` inline and `icon:` in frontmatter.
- Light/dark D2 diagrams via the `#only-light` / `#only-dark` image suffix.
- Abbreviation tooltips: `*[TERM]: Expansion`, defined once in
  `pages/_includes/abbreviations.md` (kept **outside** `docs/` so it isn't built
  as an orphan page) and auto-appended via `pymdownx.snippets`. Requires the
  `abbr` extension and the `content.tooltips` theme feature — both enabled.
- Tooltips on a link or element: a link title `[text](url "tooltip")`, or
  `{ title="…" }` on an element via `attr_list`.

Does **not** render here:

- Material code annotations (`# (1)!` + a numbered list) — there is no annotation
  processor; the markers leak as literal `(1)!`. Use an inline code comment plus a
  following admonition or bullet instead. (`content.code.annotate` is a no-op.)

## Appendix C — Living exemplars

Align to these rather than to a frozen template (a living page drifts less):

- Explanation: `pages/docs/core-concepts/architecture.md`.
- How-to: `pages/docs/recipes/crud-fastapi-postgres.md`.
- Reference-practical: `pages/docs/integrations/postgres.md`.

If an exemplar itself drifts out of contract, fix the exemplar first — it is the
reference other pages are measured against.
