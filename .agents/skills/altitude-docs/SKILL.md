---
name: altitude-docs
description: Write, polish, review, or align documentation pages to a consistent, production-grade standard using the altitude model (a deliberate high-level to low-level descent), Diátaxis-based page contracts, a shared consistency layer, and a ship rubric. Use when creating, revising, reviewing, or aligning documentation pages, or when deciding where new doc content belongs.
---

# Altitude docs

A method for writing and polishing documentation so every page reads like it was
written by one careful author: it opens at the right height, descends from
plain-language orientation to concrete detail **in order**, hands the reader on
cleanly, and reaches for a component only when the component earns its place.

The core idea is **altitude**: each page is a controlled descent through fixed
bands, from "why this exists" down to "the exact edge case". Each kind of page is
allowed a different *altitude range* — a tutorial stays high, a reference page
sits low, an explanation traverses most of the span. "Balanced" means the page
covers its range and never inverts or skips a band.

This skill is repo-agnostic. It governs *how* a page is built, not *what is
true*. Every API name, symbol, and behavior must be verified against the source
of truth in the current repository (see Accuracy).

## Use this skill when

- Writing a new documentation page.
- Polishing or reviewing an existing page for consistency, depth, or flow.
- Aligning a whole archetype directory (e.g. all tutorials) to one standard.
- Deciding which archetype — and therefore which directory — new content belongs in.

Two modes:

- **Author** — a page does not exist yet. Run the full procedure from step 1.
- **Polish** — a page exists. Run the procedure as a diff: identify the archetype,
  find where the page leaves its altitude range or breaks a contract, fix only
  that. Do not rewrite a page that already passes the rubric.

## Adapt to your repository (do this first)

This skill is generic; before applying it, discover the project's conventions and
hold them for the rest of the task. Inspect the repo (or ask the user) to learn:

- **Docs root** — where pages live (e.g. `docs/`, `pages/docs/`, `site/`).
- **Archetype layout** — which directories map to which Diátaxis archetypes
  (e.g. `get-started/` → tutorial, `recipes/` → how-to, `reference/` → reference).
  If the repo has no clear layout, use the Diátaxis archetype names directly.
- **Source of truth** — where the real API/code lives, for accuracy checks.
- **Examples source** — a directory of runnable, tested examples to pull code
  from, if one exists.
- **Build & preview commands** — how docs are built and served locally, and what
  a clean build looks like (zero warnings/errors).
- **Component system** — the site generator and which components actually render
  (admonitions, tabs, cards, diagrams-as-code, snippet includes, tooltips).
- **House style** — heading case, frontmatter fields, voice, glossary, and any
  existing style guide.

Everything below refers to these by role (e.g. "the docs root", "the examples
source"), so substitute the project's real names as you go.

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
6. **Verify the build** — clean build with no issues (see Build & verify).

## Part 1 — The altitude model

Every page descends through these five bands, **in order**. A page may start
below band 1 and may stop early, but it must never jump back up or skip a
required band on the way down.

1. **Orientation** — why this exists, what problem it removes. Plain language, no
   API names. The reader learns whether they're on the right page.
2. **Mental model** — the shape of the thing. One diagram, one analogy, or two to
   three sentences. Concept nouns only.
3. **The shape in code** — the smallest *real* anchor. One canonical snippet,
   ideally an include from the examples source. The reader sees it concretely.
4. **Mechanics** — the moving parts: the API surface, the table of
   methods/options, how it actually runs.
5. **Edge & operational detail** — caveats, failure modes, tuning, exhaustive
   lookup. The reader resolves a specific question.

Diátaxis fit (one page = one job; never mix two jobs on a page):

- **Tutorial** → learning, study, practical.
- **How-to** → a task, work, practical.
- **Explanation** → understanding, study, theoretical.
- **Reference** → information, work, theoretical.

### Altitude range per archetype

| Archetype | Enters at | Floors at | Must NOT |
|---|---|---|---|
| Tutorial | 1 | 3 (touch 4 once) | bottom out at 5; enumerate edge cases |
| Explanation | 1 | 4 | dump raw lookup; dip to 5 except its one subject |
| Deep explanation | 1 | 4, dips to 5 on its subject | become a reference dump or a how-to |
| How-to | 1 (**one line**) | 4 | rebuild the mental model (band 2) — link to it instead |
| Reference-practical | 1 (**one line**) | 4 | go conceptual (band 2) |
| Reference (lookup) | band-1 one-liner, then 4 | 5 | narrative progression; "where next" footers |

"Imbalanced" failures are now nameable: a page that is all narrative has no band 3
anchor; a page that opens on code skipped bands 1–2; a how-to that re-teaches
concepts dipped into band 2 it should have linked instead.

## Part 2 — Page contracts

Universal, every page:

- Frontmatter carries the project's required fields (commonly `title` and a
  one-line `summary`/description; an `icon` if the site uses them). No exceptions.
- Headings follow the project's case convention (default to sentence case:
  "What you just did", not "What You Just Did").
- Body starts at the project's top body-heading level (commonly `##`), with a
  single page title — avoid multiple top-level headings.
- One Diátaxis job per page.

### Tutorial

- *Opening:* lead paragraph naming the outcome ("by the end you have X running").
- *Skeleton:* Orientation → Prerequisites → numbered Steps → "What you just did"
  (a band-2 retro that names the concepts they just used) → handoff.
- *Floor:* band 3, one controlled dip to 4. *Handoff:* cards into deeper learning.
- *Code:* heavy, but every block is preceded by a one-line "why".

### Explanation (and deep explanation)

- *Opening:* required 2–4 sentence lead — what the page is and where it sits.
- *Skeleton:* topic-driven; do **not** force a uniform heading set.
- *Floor:* band 4 (deep explanation dips to 5 on its single subject).
- *Components:* a diagram near the top to anchor band 2; an admonition for the one
  rule that matters most.
- *Handoff:* a closing sentence that bridges to the related idea via an inline
  cross-link (one or two links, woven into prose). If the site has a global
  prev/next footer, the closer earns its place by adding a *conceptual* link — not
  by restating the next page. A `## See also` list fits when 3+ pages are
  genuinely related. Reserve grid cards for true multi-destination pages.

### How-to

- *Opening:* one-line problem statement, then point to a runnable example if one
  exists.
- *Skeleton:* narrative task steps (problem → model → wire → invoke → Notes).
- *Floor:* band 4. *Code:* include from the example where possible, never scratch
  code when a tested example exists.
- *Handoff:* end at the task's natural close (commonly `## Notes` or `## Run it`),
  and add a forward inline cross-link where a genuine next task exists. A
  `## Where next` card is optional — use it only for a hub page that heads a clear
  chain. Do not manufacture sibling links to satisfy a template.

### Reference-practical

Integration/setup pages that are mostly lookup with a little wiring.

- *Opening:* exactly one band-1 sentence — what it provides and when to reach for
  it.
- *Skeleton (rigid):* Install → The thing → Wire it → What it provides → Notes.
  Adapt the middle step to the subject; keep the surrounding shape.
- *Floor:* band 4. *Handoff:* none — end at Notes. No footer.

### Reference (lookup)

- *Opening:* a band-1 one-liner, then pure lookup.
- *Skeleton:* bespoke per page (glossary, taxonomy, syntax) — this is correct.
- *Components:* tables. No diagrams, no narrative progression, no footer.

When unsure which archetype new content is, ask: is the reader *studying* or
*working*, and do they want *understanding* or *information/steps*? That places
it. If a draft wants to both teach a concept and give steps, it is two pages.

### Front-door pages (the appeal layer)

Landing pages, introductions, and the opening of a quickstart carry an extra job
beyond orientation: they must make a skeptical reader *want* to continue. This
appeal layer applies on these pages only — never on reference, integration, or
how-to pages, where punch is noise.

- **Lead with the problem, not the product.** The opening states the pain the
  reader recognizes; do not describe the product before the first "what is it"
  section. The solution belongs in the sections that follow.
- **Contrast over adjectives.** Show the before/after — a concrete swap — not
  "powerful, flexible, modern".
- **Calm confidence, honesty as appeal.** Keep a "when not to use it". No hype.
- **Punch is allowed here** because these are prose pages. Everywhere else the
  structural contracts govern and punch is out of place.

Appeal is a distinct axis from altitude: a page can be altitude-perfect and dull.
These pages must pass both.

## The consistency layer

- **Voice:** second person, present tense, active voice; conversational but
  precise. ("You wire the module", not "The module is wired".)
- **Opening contract:** explanation and how-to require a lead paragraph; reference
  and integration require a one-line orienter. No page opens cold on a heading.
- **Handoff contract (one rule per archetype, no ad-hoc mixing):** cards for
  tutorials (a linear journey); how-to pages end at the task's natural close, with
  an optional `## Where next` card only for a hub page that heads a chain; an
  inline forward cross-link woven into a closing sentence for explanation, or a
  short `## See also` list when 3+ pages are related; nothing for reference and
  integration pages (end at last content).
- **Component discipline — the earns-its-place test:** keep a tab/card/admonition/
  diagram only if removing it loses information. If removing it loses only
  decoration, cut it. No patchwork of components; no walls of unbroken prose. Use
  only components that actually render in the project's setup.
- **Code policy:** prefer includes of test-backed examples over inline scratch
  code; inline only the smallest illustrative fragment; never whole-file includes
  when the tooling would leak marker comments.
- **Diagram policy:** prefer diagrams-as-code over hand-drawn images; one idea per
  diagram; balance edge directions to avoid a tall/wide blow-out (keep under ~4:1;
  flip direction or shorten labels when boxes stretch out).
- **Accuracy:** every API symbol verified against the current source of truth
  before it ships. Never invent or copy an API from an old draft. Use the
  project's canonical vocabulary and error/exception idioms consistently.
- **External citations:** the first time a page leans on an outside concept (a
  design pattern, an RFC, a paper), link the canonical source with a tooltip title
  that glosses it in one line. Strongest on explanation and front-door pages;
  don't litter every page.
- **Abbreviations:** define each non-obvious abbreviation once, using the project's
  glossary or tooltip mechanism if one exists. Skip anything the target reader
  already knows; define only the specialized or ambiguous ones.
- **No meta:** never tell the reader a page is test-backed, that "the example is
  the spec", or reference test files. Frame sections by behavior, not by how the
  docs are built.

## Ship rubric (definition of done)

A page ships only when all pass:

1. Single Diátaxis job — no mixing.
2. Frontmatter complete per the project's required fields.
3. Opens per its opening contract (lead paragraph or one-line orienter).
4. Descends its altitude range in order — no inversion, no skipped required band,
   does not bottom out below its floor.
5. Every component earns its place.
6. Code is example-sourced where it can be; inline code is minimal.
7. Ends per its handoff contract.
8. Voice is second person / present / active; headings follow house case.
9. Every API symbol verified against the current source of truth.
10. Clean build passes (see below).

## Build & verify

- **Render diagrams first.** If the project uses diagrams-as-code, render sources
  to their output location before referencing them, and reference them with the
  project's path/theme convention.
- **Preview locally** with the project's serve command while iterating.
- **Authoritative build check:** run the project's clean/full build (clearing any
  cache) and expect a zero-issue result. Warm/incremental builds often emit
  spurious, fluctuating warnings (e.g. transient "page does not exist" counts) —
  treat those as noise; only a clean build's result is real.
- **Snippet includes:** when the tooling supports pulling code from external
  files, prefer that over inline code, and keep include paths valid (a bad path
  should fail the build). Mark regions in the source file and reference them from
  a fenced block.

## Appendix A — Maintaining a consistent vocabulary

Pages drift into inconsistency when the same concept is named different ways.
Keep a short, project-owned glossary of canonical terms and use it everywhere:

- Pick one name per concept and one casing convention; record near-synonyms you
  deliberately avoid ("say X, not Y").
- Distinguish the concept from its implementation type when both exist (the idea
  vs the base class that realizes it) and use each in its place.
- Verify every symbol against the source of truth before asserting specifics;
  never carry a name forward from an old draft without checking.

If the repository already has a glossary or style guide, defer to it and extend
it rather than inventing parallel terms.

## Appendix B — Component discipline by setup

Different site generators support different components, and a marker that renders
in one setup leaks as literal text in another. Before using a component:

- Confirm it renders in the project's generator (admonitions, tabs, grid cards,
  buttons, icons, light/dark diagrams, tooltips, snippet includes).
- If a feature is a no-op in the setup (e.g. an unsupported code-annotation
  syntax), use a supported substitute (an inline comment plus a following
  admonition or bullet) instead of leaking markup.

Capture the verified component list once for the repo so later pages don't
rediscover it.

## Appendix C — Living exemplars

Align new pages to the best existing page of each archetype in the repo rather
than to a frozen template — a living page drifts less. Pick one exemplar per
archetype (one explanation, one how-to, one reference-practical, etc.) and treat
it as the reference others are measured against. If an exemplar itself drifts out
of contract, fix the exemplar first.
