---
name: altitude-docs
description: Use when writing, restructuring, polishing, or reviewing a documentation page — tutorial, quickstart, how-to, reference, explanation, landing page — or when deciding which archetype new content belongs in. Not for docstrings, RFCs, or changelogs.
roles: [author]
gate: none
gate_reason: page quality is a rubric a reader applies, with no artifact a program can refuse
---

# Altitude docs

A method for writing and polishing documentation so every page reads like it was
written by one careful author: it opens at the right height, descends from
plain-language orientation to concrete detail **in order**, hands the reader on
cleanly, and reaches for a component only when the component earns its place.

The core idea is **altitude**: a controlled descent through fixed bands, from
"why this exists" down to "the exact edge case". Each kind of page is allowed a
different *range* — a tutorial stays high, a reference page sits low.

This skill governs *how* a page is built, not *what is true*. Every API name,
symbol and behavior is verified against the current repository — see the
accuracy rules in [references/contracts.md](references/contracts.md).

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

## The five bands

Every page is a controlled descent through these, **in order**. Each archetype
declares which bands it *requires*; a page may start below band 1 and may stop
early, and bands outside its declared range are simply not its job. What it must
never do is jump back up, or skip a band its own archetype requires on the way
down.

| # | Band | Carries |
|---|---|---|
| 1 | Orientation | Why this exists, what problem it removes. Plain language, no API names. |
| 2 | Mental model | The shape of the thing — one diagram, one analogy, or two sentences. Concept nouns only. |
| 3 | The shape in code | The smallest *real* anchor. One canonical snippet, ideally included from the examples source. |
| 4 | Mechanics | The moving parts: API surface, the table of methods and options, how it runs. |
| 5 | Edge and operational detail | Caveats, failure modes, tuning, exhaustive lookup. |

Each archetype is allowed a different *range*. A tutorial enters at 1 and floors
at 3; a lookup reference gives band 1 one line and lives at 4–5; a how-to must
not rebuild the mental model but link to it. "Imbalanced" then becomes
nameable: all narrative and no band-3 anchor, or an opening on code that skipped
bands 1–2.

**One page, one job.** The Diátaxis compass asks whether the content informs
*doing* or *knowing*, and whether the reader is *studying* or *working* —
tutorial, how-to, reference, explanation fall out of the two answers. A draft
that answers two of them is two pages, cross-linked. The usual conflation is
tutorial versus how-to: a tutorial is a lesson whose author owns the reader's
success on one controlled path; a how-to is directions to a competent reader who
owns the outcome, and may branch.

The full bands, compass and per-archetype ranges are in
[references/altitude.md](references/altitude.md); what each archetype owes its
reader, and the consistency rules across all of them, are in
[references/contracts.md](references/contracts.md). The step-by-step is in
[references/procedure.md](references/procedure.md).

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

## References

- `references/altitude.md` — the five bands, the Diátaxis compass, the range per archetype
- `references/contracts.md` — what each archetype owes its reader, and the consistency layer
- `references/procedure.md` — the authoring procedure, and how to verify what it produced
- `references/vocabulary.md` — keeping one term per concept across a docs tree
- `references/components.md` — which components a setup earns, and when they are noise
- `references/exemplars.md` — the pages a tree is measured against

## Related skills

- `rfc-writer` — design proposals and RFCs; they argue for decisions, docs pages serve readers.
- `keep-a-changelog` — the changelog has its own format; don't fold release notes into docs pages.
- `python-google-docstrings` / `python-rest-docstrings` — the docstrings that reference pages are often generated from.
