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

**One non-numbered resident: `EXECUTION-LOG.md`.** `flag-dont-flip` writes it, and it holds what execution found wherever the code and these designs disagreed. It is not an RFC — no number, no status, no row in the index table — so `rfc_index.py` ignores it along with anything else not named `NNNN-*.md`. Where it exists, the index links to it in prose above the table, because a reader who is deciding which RFC to open needs to know that the document they are about to trust has a companion recording where it turned out to be wrong. Do not create it here: it is written by the first completed execution unit, which records its drift count even at zero. Creating it alongside an empty directory would give it nothing to claim.

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
11. **Decisions** — a numbered table of decisions, each carrying a **grade** (see "Decision grades" below); this is what makes pickup cheap and re-litigation unnecessary. Where a decision constrains the future non-obviously, the row says so — the consequences of one decision are the context of the next. Decisions the RFC deliberately leaves to implementation belong here too, graded `OPEN`, rather than being left out
12. **Phasing** — what lands first, what's gated on what

**Scale to the RFC's weight.** A small design-lock RFC needs only the header block, Design, Non-goals, and the Decision table. Don't pad a two-page RFC to twelve sections; don't collapse a system-wide proposal into three. Keep section numbering contiguous for whatever subset is used.

## Writing style

- **Ground every claim in the code.** "Current state" and "Motivation" cite files, line-level facts, and measured numbers — link them with relative paths. An RFC that argues from memory is a fiction with headings.
- **Record decisions with their why — and their cost.** The decision table is the contract; the body carries the reasoning. Rejected alternatives get a sentence saying why they lost (an alternative recorded with its trade-off stays rejected; one recorded as merely "rejected" gets re-proposed). A decision that closes a door later says so in its row.
- **Timely beats polished.** A rough RFC that exists beats a perfect one that doesn't (Oxide's RFD rule: "timely rather than polished"). Draft prose may be rough; the Scope paragraph and the decision table may not.
- **Be honest about limits.** If a mechanism is deferred, gated, or known-incomplete, say so in the RFC rather than letting the reader discover it. Fail-closed wording ("refused", "raises", "deliberately unscheduled") beats optimistic vagueness.
- **Dense beats long.** Prefer one load-bearing paragraph over three thin ones. This applies inside the RFC; the index entry is governed by the rule below, which is the opposite instinct.

## Decision grades

Every row in the Decisions table carries a grade. The grade tells whoever executes the RFC what to do when the code disagrees with the document — `flag-dont-flip` owns that behaviour, this skill owns the vocabulary, and both read the same three words:

| Grade | Meaning | What it asks of an executor |
|---|---|---|
| `LOCKED` | Settled. Reopening is expensive, or the consequences reach beyond this RFC. | Halt on conflict and surface it. The author decides, in the RFC. |
| `ASSUMED` | Believed correct, not load-bearing. | Depart if building it proves the assumption wrong, and log the departure. |
| `OPEN` | Deliberately delegated to implementation. | Decide it, and log the decision with its rationale. |

**Grade honestly — most rows are `ASSUMED`.** `LOCKED` is not a way of saying "I mean it". Marking rows `LOCKED` by default makes halting routine, and routine halts get waved through, which costs you the one signal the grade exists to send. Reserve it for decisions whose reversal would invalidate other work.

**`OPEN` is not the same as leaving a row out.** An `OPEN` row records that the author considered the question and chose to delegate it. An absent row records nothing: the question still gets answered, by whoever reaches it first, and no later reader can tell it was ever a decision. Writing the row down is cheap and front-loads the questions execution would otherwise answer alone.

## Reconciling what execution learned

Execution finds things the design could not. When it does, the executor **proposes** rows — in `EXECUTION-LOG.md`, with the evidence that produced them — and the author appends them. Three rails:

- **The decision table is append-only.** A superseded row stays, marked superseded, naming the row that replaced it. The history of a decision is the part that stops it being re-litigated.
- **Never amend the RFC's prose to match what was built.** It reads as tidying, and it destroys the only evidence that a decision changed at all — which is precisely what a later reader needs in order to trust the document. Record the change; don't erase the disagreement.
- **An accepted row cites the log entry it came from** — `Added by execution 2026-08-14 — see [EXECUTION-LOG.md](EXECUTION-LOG.md) D-001` at the end of the row. The row states the decision; the entry holds what was actually found, what was built instead, and what it cost. Without the link the row reads as something the author thought of, which loses the one fact that makes it credible: it was forced by contact with the code.

An RFC whose prose has been quietly retrofitted is worse than one that is visibly out of date: the second tells you to check, the first does not.

## The index one-liner: routing, not summary

**The one-liner exists to tell a reader which RFC to open, not what it decided.** It has one job — discriminate this design from the others in the table — and that takes far less text than summarising it. "Get a backup off the machine that took it" is forty characters and separates its RFC from twenty others; the design, the decisions and the trade-offs belong in the file it points at.

The rules:

- **One sentence. Aim for 200 characters, and treat 300 as the ceiling.** A table of thirty rows is then a couple of thousand characters, which is what makes the index cheap enough to consult on every lookup.
- **State the problem and the shape of the answer.** Not the mechanism, not the alternatives, not the numbers.
- **The index records what an RFC *is*, never what happened to it.** No "shipped 2026-08-04", no phase-by-phase progress, no defects found, no amendment history. Status lives in the Status column; everything else lives in the RFC — its `**Status:**` annotation, its Decisions table, its execution notes. An entry that grows each time work lands has become a changelog, and the whole table is then re-read on every allocation.
- **Write it once.** Revisit it only when the RFC's *subject* changes — not when its state does.

This is the one place in the skill where completeness is the wrong target. An index entry dense enough to substitute for opening the file has stopped being an index: every future lookup pays for content that belongs to one document.

## Workflows

The bookkeeping — number allocation, file creation from the template, index-row and next-free-number updates, drift detection — is mechanical, and `scripts/rfc_index.py` does it without the collisions hand-allocation produces:

```bash
python3 scripts/rfc_index.py check          # index vs files, H1 vs filename, statuses, next-free
python3 scripts/rfc_index.py next           # next free number
python3 scripts/rfc_index.py new "Title"    # allocate + instantiate template + index row + bump
python3 scripts/rfc_index.py new "Title" --number 42   # a reserved number, or re-creating a deleted RFC
```

(Paths relative to this skill's directory; from a repository root the script is at `skills/rfc-writer/scripts/rfc_index.py`. Read-only except `new`; add `--root DIR` — before or after the subcommand — if the repo isn't the cwd.) The thinking — what the design says, what the one-liner claims, when a status changes — is yours.

### A — Create a new RFC

1. Locate the RFC directory (`rfcs/` or `rfc/`); if none exists, run Workflow D first.
2. Allocate the next number and instantiate the file: `rfc_index.py new "Title"` — it mints the number, writes the template, adds the index row, and bumps the next-free claim. Steps 3 and 4 stay yours: it leaves the template unfilled and writes a literal `TODO: one-line summary` in the index. By hand: read the next-free number from the index and cross-check against `ls` — numbers collide when minted in parallel.
3. Fill the file from `references/rfc-template.md`'s shape, scaled to the design's weight. Investigate the actual code before writing "Current state" — this is most of the work.
4. Replace the placeholder index one-liner with one sentence that says which design this is — see the one-liner rules above. The summary the RFC deserves goes in the RFC's Summary section.

### B — Update an existing RFC

1. When work ships partially or fully, update the `**Status:**` line — and annotate it with what shipped and when ("Shipped 2026-06-29: …; only P5 remains").
2. If execution diverged from the design, the divergence is already in `EXECUTION-LOG.md`; what lands here is the decision row it proposed, appended and citing its entry. Don't silently rewrite history, and don't restate the log's narrative in the RFC — the row is the contract, the entry is the evidence, and duplicating one into the other means they will disagree later.
3. Mirror the status in the index table. Leave the one-liner alone unless the RFC's *subject* changed — shipping, phasing and amendments are the RFC's history, not the index's.
4. Rejected designs get ❌ and stay in the directory.

### C — Maintain the index

Run `rfc_index.py check` — it reports every file without an index row and vice versa, H1-vs-filename mismatches, header-vs-table status disagreements, duplicate numbers, and a next-free number that isn't free. Fix what it names (the fixes are judgment: which status is true, what the one-liner should say), then re-run until green. Report what was out of sync.

### D — Initialize an RFC directory

1. Create `rfcs/` (unless the user wants `rfc/` or one already exists).
2. Create `INDEX.md` from `references/index-template.md`, filling in the project name and setting the next free number to `0001`.
3. Do not touch `.gitignore` — mention that committing vs. ignoring the directory is the user's choice.
4. Do not create `EXECUTION-LOG.md`. It is `flag-dont-flip`'s, and the first completed execution unit writes it; the index gains its pointer at the same time.

## References

- `references/rfc-template.md` — RFC skeleton with per-section guidance; read before writing any new RFC
- `references/index-template.md` — INDEX.md skeleton; use when initializing a directory

## Related skills

- `flag-dont-flip` — executes an RFC against its grades, owns `EXECUTION-LOG.md`, and proposes the rows execution turned up
- `altitude-docs` — the user-facing documentation that ships after the design; the RFC's Docs section points at it
- `self-audit` — adversarial review of the branch that executed an RFC, before merge
- `keep-a-changelog` — records what shipped; the RFC records why it was built that way
