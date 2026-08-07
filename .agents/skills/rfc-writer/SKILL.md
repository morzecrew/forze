---
name: rfc-writer
description: Author and maintain numbered RFC design documents in a project's rfcs/ directory, tracked by an INDEX.md. Use whenever the user asks to write an RFC, design proposal, design doc, technical spec, or architecture proposal; wants to record a design decision, its alternatives, or why an approach was rejected before building; asks to update an RFC's status after shipping; or asks to set up / clean up an rfcs/ directory or its index.
---

# RFC Authoring and Maintenance

This skill authors and maintains lightweight RFCs — numbered Markdown design proposals that live in the repository next to the code they describe. An RFC captures a design *before* (or while) it is built: the problem, the current state of the code, the locked decisions with their rationale, and what is deliberately out of scope. The collection is tracked by a single `INDEX.md` so the whole design history is scannable in one table.

RFCs here are working documents, not bureaucracy: they exist so that decisions survive context loss, so that a picked-up design is "a single small PR, nothing more", and so that rejected alternatives don't get re-litigated.

## Use this skill when

- The user asks to write an RFC, design doc, design proposal, technical spec, or architecture proposal
- The user wants to record or lock a design decision — with the alternatives it beat, or why an approach was rejected — before implementing it
- The user asks to update an RFC (status change, execution notes, marking it shipped or rejected)
- The user asks to create, index, or reorganize an `rfcs/` (or `rfc/`) directory
- A large feature discussion should be captured as a durable document

## Do not use this skill when

- The user wants product documentation, a README, or user-facing docs (not a design proposal)
- The user wants an ADR in a repo that already has an established ADR convention — follow that convention instead
- The change is trivial enough that a commit message or PR description carries it

## Directory and index

**Location.** RFCs live in a single flat directory at the repo root: `rfcs/` (preferred default) or `rfc/`. Before creating anything, look for an existing directory of either name and follow it. If neither exists, create `rfcs/`.

**Gitignore is the user's call, not yours.** Some projects commit RFCs; others gitignore them as local working notes. Never add or remove a `.gitignore` entry for the RFC directory unless explicitly asked. If the directory is gitignored, the `INDEX.md` header should say so (see the template) so readers know why it isn't in the repo history.

**INDEX.md is the source of truth for the collection.** It carries three things:

1. The **next free number**, stated explicitly — numbers collide when minted in parallel, so the index names the next one and every RFC creation updates it in the same change.
2. The **index table**: `| # | Title | Status | One-line |`, one row per RFC, number linked to the file.
3. The **status legend**.

If the directory exists but has a `README.md` in this role, treat it as the index. If asked to set up fresh, use `INDEX.md` — copy `references/index-template.md`.

## Numbering and filenames

- Numbers are 4-digit, zero-padded, monotonically increasing: `0001`, `0002`, …
- To allocate: read the "next free number" from `INDEX.md`, cross-check against `ls` of the directory (the index can be stale), and take the next unused integer.
- Filename: `NNNN-kebab-case-title.md`. Keep the number in the filename and the `# RFC NNNN — Title` H1 in sync — they drift otherwise, and links break both ways.
- Never renumber existing RFCs. Numbers are identifiers, not an ordering to be tidied.

## Statuses

- 📝 **Draft** — proposed, not started (a "design locked, demand-gated" RFC is still Draft)
- 🚧 **In progress** — partially shipped
- ✅ **Complete** — fully shipped
- ❌ **Rejected / withdrawn** — keep the file; a recorded rejection prevents re-litigating

Status lives in two places that must agree: the `**Status:**` line in the RFC header and the Status column of the index table. Update both in the same change.

## RFC anatomy

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
11. **Decisions** — a numbered table of locked decisions; this is what makes pickup cheap and re-litigation unnecessary. Where a decision constrains the future non-obviously, the row says so — the consequences of one decision are the context of the next
12. **Phasing** — what lands first, what's gated on what

**Scale to the RFC's weight.** A small design-lock RFC needs only the header block, Design, Non-goals, and the Decision table. Don't pad a two-page RFC to twelve sections; don't collapse a system-wide proposal into three. Keep section numbering contiguous for whatever subset is used.

## Writing style

- **Ground every claim in the code.** "Current state" and "Motivation" cite files, line-level facts, and measured numbers — link them with relative paths. An RFC that argues from memory is a fiction with headings.
- **Record decisions with their why — and their cost.** The decision table is the contract; the body carries the reasoning. Rejected alternatives get a sentence saying why they lost (an alternative recorded with its trade-off stays rejected; one recorded as merely "rejected" gets re-proposed). A decision that closes a door later says so in its row.
- **Timely beats polished.** A rough RFC that exists beats a perfect one that doesn't (Oxide's RFD rule: "timely rather than polished"). Draft prose may be rough; the Scope paragraph and the decision table may not.
- **Be honest about limits.** If a mechanism is deferred, gated, or known-incomplete, say so in the RFC rather than letting the reader discover it. Fail-closed wording ("refused", "raises", "deliberately unscheduled") beats optimistic vagueness.
- **Dense beats long.** Prefer one load-bearing paragraph over three thin ones. The index one-liner especially: it must be self-contained enough that a reader can skip the RFC.

## Workflows

### A — Create a new RFC

1. Locate the RFC directory (`rfcs/` or `rfc/`); if none exists, run Workflow D first.
2. Allocate the next number (index + `ls` cross-check).
3. Read `references/rfc-template.md`; write `NNNN-kebab-title.md` from it, scaled to the design's weight. Investigate the actual code before writing "Current state" — this is most of the work.
4. Add the index row (number linked, status 📝, dense one-liner) and bump the next-free number, in the same change.

### B — Update an existing RFC

1. When work ships partially or fully, update the `**Status:**` line — and annotate it with what shipped and when ("Shipped 2026-06-29: …; only P5 remains").
2. If execution diverged from the design, record the divergence in the RFC (a status note or an amendment in the relevant section) — don't silently rewrite history; the decision log stays append-only.
3. Mirror the status (and, if the shape changed, the one-liner) in the index table.
4. Rejected designs get ❌ and stay in the directory.

### C — Maintain the index

When asked to sync or clean up: verify every file in the directory has an index row and vice versa; verify statuses in headers and table agree; verify the next-free number is actually free; fix drift. Report what was out of sync.

### D — Initialize an RFC directory

1. Create `rfcs/` (unless the user wants `rfc/` or one already exists).
2. Create `INDEX.md` from `references/index-template.md`, filling in the project name and setting the next free number to `0001`.
3. Do not touch `.gitignore` — mention that committing vs. ignoring the directory is the user's choice.

## References

- `references/rfc-template.md` — RFC skeleton with per-section guidance; read before writing any new RFC
- `references/index-template.md` — INDEX.md skeleton; use when initializing a directory

## Related skills

- `altitude-docs` — the user-facing documentation that ships after the design; the RFC's Docs section points at it
- `self-audit` — adversarial review of the branch that executed an RFC, before merge
- `keep-a-changelog` — records what shipped; the RFC records why it was built that way
