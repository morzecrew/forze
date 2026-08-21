# Writing an RFC: anatomy and style

The shape of the document and how its prose behaves. Read this before
writing a new RFC; `SKILL.md` carries the decisions that outlive it.

## Anatomy

Read `references/rfc-template.md` before writing a new RFC and start from it. The shape:

**Header block** (bullet list directly under the H1, before any section):

- `**Status:**` — emoji + word, optionally annotated ("execution-ready — one PR", "design locked, not scheduled")
- `**Scope:**` — a dense paragraph: what this RFC covers *and what it deliberately does not*. This is the paragraph someone reads to decide whether to read the rest.
- `**Related:**` — links to the code being touched (relative links into the repo), other RFCs, prior art, external references
- `**Discussion:**` (optional) — link to where the design was or is being debated (PR, issue, thread); a reader who disagrees goes there instead of forking the document
- `**Origin:**` (optional) — where the design was ported or generalized from, if anywhere

**Numbered sections.** The full set, for a substantial RFC:

1. **Summary** — what ships, in a few sentences
2. **Motivation** — the problem, with evidence from the actual codebase
3. **Current state** — what exists today, verified against the code, not from memory
4. **Goals / Non-goals** — explicit both ways
5. **Design** — the core; subsections per workstream or component, with real signatures/schemas/code blocks where they pin the design. Where a choice was contested, keep the rejected alternative and why it lost — one sentence for minor calls, an `### Alternatives considered` subsection when the choice shaped the design
6. **Tests** — how the design is verified
7. **Docs** — what documentation ships with it
8. **Out of scope** — named and *reasoned*: each item says why it's excluded and what would change that
9. **Risks** — honest failure modes, including risks of the document being misread
10. **Unresolved questions** — what must be settled before the design counts as locked, vs. what implementation is free to settle; naming an unknown beats resolving it silently mid-build
11. **Decisions** — a numbered table of decisions, each carrying a **grade** (see "Decision grades" below); this is what makes pickup cheap and re-litigation unnecessary. Where a decision constrains the future non-obviously, the row says so — the consequences of one decision are the context of the next. Decisions the RFC deliberately leaves to implementation belong here too, graded `OPEN`, rather than being left out
12. **Phasing** — what lands first, what's gated on what

**Scale to the RFC's weight.** A small design-lock RFC needs only the header block, Design, Non-goals, and the Decision table. Don't pad a two-page RFC to twelve sections; don't collapse a system-wide proposal into three. Keep section numbering contiguous for whatever subset is used.

## Style

- **Ground every claim in the code.** "Current state" and "Motivation" cite files, line-level facts, and measured numbers — link them with relative paths. An RFC that argues from memory is a fiction with headings.
- **Record decisions with their why — and their cost.** The decision table is the contract; the body carries the reasoning. Rejected alternatives get a sentence saying why they lost (an alternative recorded with its trade-off stays rejected; one recorded as merely "rejected" gets re-proposed). A decision that closes a door later says so in its row.
- **Timely beats polished.** A rough RFC that exists beats a perfect one that doesn't (Oxide's RFD rule: "timely rather than polished"). Draft prose may be rough; the Scope paragraph and the decision table may not.
- **Be honest about limits.** If a mechanism is deferred, gated, or known-incomplete, say so in the RFC rather than letting the reader discover it. Fail-closed wording ("refused", "raises", "deliberately unscheduled") beats optimistic vagueness.
- **Dense beats long.** Prefer one load-bearing paragraph over three thin ones. This applies inside the RFC; the index entry is governed by the rule below, which is the opposite instinct.
