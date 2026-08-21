---
name: rfc-writer
description: Use when asked to write an RFC, design proposal, design doc, technical spec, or architecture proposal; to record a decision and the alternatives it beat before building; to update an RFC's status after shipping; or to set up an rfcs/ directory. Not for user-facing documentation.
roles: [author]
gate: rfc-index
---

# RFC Authoring and Maintenance

This skill authors and maintains lightweight RFCs — numbered Markdown design proposals that live in the repository next to the code they describe. An RFC captures a design *before* (or while) it is built: the problem, the current state of the code, the locked decisions with their rationale, and what is deliberately out of scope. The collection is tracked by a single `INDEX.md` so the whole design history is scannable in one table.

RFCs here are working documents, not bureaucracy: they exist so that decisions survive context loss, so that a picked-up design is "a single small PR, nothing more", and so that rejected alternatives don't get re-litigated.

## The mechanical half is a script

Location, numbering, filenames, statuses, index rows and the next free number are conventions a program applies. `scripts/rfc_index.py` applies them, and its `check` is the gate:

```bash
python3 scripts/rfc_index.py check          # index vs files, H1 vs filename, statuses, next-free,
                                            # decision table present and fully graded, Related links resolve
python3 scripts/rfc_index.py next           # next free number
python3 scripts/rfc_index.py new "Title"    # allocate + instantiate template + index row + bump
```

(From a repository root the script is at `skills/rfc-writer/scripts/rfc_index.py`. Read-only except `new`; add `--root DIR` — before or after the subcommand — if the repo isn't the cwd.) The procedure around it, the document's anatomy, and its prose style are in [references/workflows.md](references/workflows.md) and [references/authoring.md](references/authoring.md).

What is left in this file is what no check decides: which grade a decision deserves, what the one-liner claims, and what happens when execution disagrees with the document.

## Decision grades

Every row in the Decisions table carries a grade. The grade tells whoever executes the RFC what to do when the code disagrees with the document — `flag-dont-flip` owns that behaviour, this skill owns the vocabulary, and both read the same three words:

| Grade | Meaning | What it asks of an executor |
|---|---|---|
| `LOCKED` | Settled. Reopening is expensive, or the consequences reach beyond this RFC. | Halt on conflict and surface it. The author decides, in the RFC. |
| `ASSUMED` | Believed correct, not load-bearing. | Depart if building it proves the assumption wrong, and log the departure. |
| `OPEN` | Deliberately delegated to implementation. | Decide it, and log the decision with its rationale. |

**Grade honestly — most rows are `ASSUMED`.** `LOCKED` is not a way of saying "I mean it". Marking rows `LOCKED` by default makes halting routine, and routine halts get waved through, which costs you the one signal the grade exists to send. Reserve it for decisions whose reversal would invalidate other work.

**`OPEN` is not the same as leaving a row out.** An `OPEN` row records that the author considered the question and chose to delegate it. An absent row records nothing: the question still gets answered, by whoever reaches it first, and no later reader can tell it was ever a decision. Writing the row down is cheap and front-loads the questions execution would otherwise answer alone.

## The index one-liner routes; it does not summarise

Its one job is to tell a reader **which** RFC to open, and that takes far less
text than saying what the RFC decided. One sentence, aiming at 200 characters
with 300 as the ceiling — `rfc_index.py check` warns past either. State the
problem and the shape of the answer; the mechanism, the alternatives and the
numbers belong in the file it points at.

**The index records what an RFC *is*, never what happened to it.** No shipped
dates, no phase progress, no amendment history — an entry that grows each time
work lands has become a changelog, and the whole table is re-read on every
lookup. Write it once; revisit it only when the RFC's *subject* changes. Full
rules in [references/workflows.md](references/workflows.md).

## Reconciling what execution learned

Execution finds things the design could not. The executor **proposes** rows, in
its task log with the evidence that produced them; the author appends them. Three
rails, in full in [references/workflows.md](references/workflows.md):

- **The decision table is append-only.** A superseded row stays, marked superseded, naming the row that replaced it — the history is what stops a decision being re-litigated.
- **Never amend the RFC's prose to match what was built.** It reads as tidying and destroys the only evidence that a decision changed. An RFC visibly out of date tells you to check; a quietly retrofitted one does not.
- **An accepted row cites the entry it came from** — `see logs/T-0142.md (D-3, attempt 2)`. Without the link the row reads as something the author thought of, losing the one fact that makes it credible: it was forced by contact with the code.

## References

- `references/rfc-template.md` — RFC skeleton with per-section guidance; read before writing any new RFC
- `references/index-template.md` — INDEX.md skeleton; use when initializing a directory
- `references/authoring.md` — the twelve sections, and how the prose behaves
- `references/workflows.md` — create, update, maintain the index, initialize a directory

## Related skills

- `flag-dont-flip` — executes an RFC against its grades, owns the task logs, and proposes the rows execution turned up. Absent it, an accepted row still cites where the finding came from; an uncited row reads as something the author thought of.
- `altitude-docs` — the user-facing documentation that ships after the design; the RFC's Docs section points at it
- `self-audit` — adversarial review of the branch that executed an RFC, before merge
- `keep-a-changelog` — records what shipped; the RFC records why it was built that way
