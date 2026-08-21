# Page contracts, band by band

What each archetype owes its reader, and the
consistency rules that hold across all of them.

## Page contracts

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
  (a band-2 retro that names the concepts they just used — the one sanctioned
  upward move, allowed only here, after the steps are done) → handoff.
- *Floor:* band 3, one controlled dip to 4. *Handoff:* cards into deeper learning.
- *Path:* single and reliable — no forks, no "alternatively", no choices; every
  step must work as written. Explicit about basics; theory gets one line and a
  link to its explanation page, never a mid-step lecture.
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
- *Assumptions:* the reader is competent — basics stay implicit. Unlike a
  tutorial, a how-to may fork ("if you use X, do Y") to cover real-world
  variation; keep each branch one line and don't multiply them.
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

When unsure which archetype new content is, run the compass (Part 1): action or
cognition, acquisition or application.

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
  precise. ("You wire the module", not "The module is wired".) Never promise
  ease — cut "simply", "easily", "just": the word insults the reader the moment
  the step fails.
- **Instructions:** one action per numbered step, imperative verb first. State
  the condition before the instruction ("If you use X, set Y" — not "Set Y if
  you use X"), so readers can skip inapplicable steps without reading them; put
  the goal before the action when it isn't obvious ("To enable retries, set…").
- **Opening contract:** explanation and how-to require a lead paragraph; reference
  and integration require a one-line orienter. No page opens cold on a heading.
- **Handoff contract:** exactly the per-archetype rule from Part 2 — cards for
  tutorials, natural close for how-to, inline link or `## See also` for
  explanation, nothing for reference and integration. No ad-hoc mixing.
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
