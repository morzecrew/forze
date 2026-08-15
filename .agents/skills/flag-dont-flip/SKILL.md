---
name: flag-dont-flip
description: Executes work against an existing RFC or design doc without silently changing its decisions. Produces a reviewable file-by-file plan before writing code, halts on conflicts with LOCKED decisions instead of resolving them, and records every departure in an append-only execution log — `rfcs/EXECUTION-LOG.md` — classified by whether it was knowable at design time, with a drift count per unit of work and the decision rows it proposes back. Use this whenever implementing against an RFC, spec, ADR, or design doc — including when the user says "implement RFC-N", "execute the plan", "build what we agreed", or starts coding in a repo with an `rfcs/` directory. Also use when a design decision needs to change mid-implementation, when the code has drifted from its spec, or when someone asks why the implementation doesn't match the doc.
---

# Flag, Don't Flip

An RFC stops being useful the moment the code silently disagrees with it. Not because the code is wrong — the code is often right — but because a reader can no longer tell which parts of the document still describe reality. Every hunk of the diff then has to be reviewed as a potential unannounced design change, which is why reviewing agent-written code against a spec costs more than reviewing the spec did.

This skill makes the diff locally reviewable: every departure from the RFC is either announced or a defect. Nothing in between.

**The rule:** when execution meets a decision the RFC settled differently, flag it. Never flip it in place and move on.

## Use this skill when

- Implementing against an RFC, spec, ADR, or design doc — "implement RFC-014", "build the retry handling we agreed on"
- A design decision needs to change mid-implementation
- The implementation has drifted from the document describing it
- Deciding whether an under-specified RFC is ready to execute at all

## Do not use this skill when

- *Authoring* the design — that's `rfc-writer`; this skill starts once a document exists
- The change has no design document and no meaningful design surface — a typo fix does not need a plan gate, and a plan gate on a two-line change is friction that gets the whole practice switched off
- A bug needs fixing against no spec — that's `reproduce-then-fix`

If work is underway, no RFC exists, and the change is clearly load-bearing, say so and hand off to `rfc-writer` rather than inventing decisions inside the implementation.

## Decision grades

The RFC's decision table carries a grade per row. `rfc-writer` owns the format; this skill owns the behavior.

| Grade | Meaning | Executor behavior on conflict |
|---|---|---|
| `LOCKED` | Settled. Cost of reopening is high or consequences reach beyond this RFC. | **Halt.** Surface the conflict and wait. Do not implement either side. |
| `ASSUMED` | Believed correct, not load-bearing. | Depart if execution shows it wrong. Log it. Continue. |
| `OPEN` | Deliberately delegated to implementation. | Decide it. Log the decision and its rationale. Continue. |

An unlisted decision is not `OPEN`. `OPEN` means the author looked at it and chose not to settle it; unlisted means nobody looked. Treat unlisted decisions as gaps — they get logged with the same weight as a departure, because a gap filled silently is indistinguishable in the diff from a decision reversed silently.

## The loop

### 1. Orient

Read the RFC's decision table and its rejected-alternatives section before reading any code. Rejected alternatives are the highest-value part of the document during execution: they are the shapes the implementation will keep wanting to reach for, and each was already argued down.

If the RFC's status says it has already been executed, expect drift and check the execution notes first.

### 2. Plan

Produce a plan and stop. Do not write code in the same turn as the plan.

The plan contains:

- **Files touched**, each with one line on what changes and why
- **Decision mapping** — for each non-trivial choice in the plan, which decision-table row governs it
- **Decisions this plan needs that the RFC does not settle** — the load-bearing list

The third section is what this whole skill exists for. It converts specification gaps into cheap questions *before* any code embodies an answer. A gap found here costs one paragraph; the same gap found in review costs a re-implementation.

**Readiness gate:** if the third list contains three or more load-bearing entries, the RFC is not ready to execute. Report that and stop. Executing an under-specified RFC does not produce an implementation — it produces a second, undocumented design, expressed in code and discoverable only by reading it.

### 3. Gate

The plan is reviewed before execution starts. `LOCKED` conflicts identified in planning are resolved by the human, in the RFC, not in the plan.

### 4. Execute

Follow the approved plan. When reality diverges from it mid-execution, the same rules apply — a plan is not a licence, and the decision grades still govern.

### 5. Log

Every departure gets a record, appended, never edited in place:

```markdown
## D-003 — Retry budget scoped to batch, not message

- **Touches:** RFC 0014 §5.2, decisions row 4 (`ASSUMED`)
- **RFC said:** per-message retry counter
- **Built:** per-batch retry budget
- **Because:** redelivery resets per-message counters, so a per-message
  counter cannot bound total work for a poison message
- **Class:** spec-gap
- **Consequence:** a poison message can consume up to N x batch attempts
  before reaching the dead-letter queue
- **Proposed row (RFC 0014):** retry budget is per-batch; poison ceiling is
  batch_size x max_attempts
```

Where the design was followed *and the alternative is worth naming*, say so in the same entry under **Deliberately not applied:** — an illustrative sketch you chose not to implement is a decision, and the reader who finds the code disagreeing with the sketch needs to know it was seen rather than missed.

The log is one file, and `references/execution-log-template.md` is its skeleton.

### 6. Reconcile

Propose decision-table rows back to the RFC. Do not write them silently, and **never edit the RFC to match what was built** — that launders the flip and destroys the record that a decision changed at all. The append-only table is the point.

Record what the author decided in the log — a dated table of `RFC | row | outcome | grade | decision | from`, carrying refusals as well as acceptances, since a refusal is written down nowhere else — and have each new RFC row cite the entry it came from (`Added by execution <date> — see EXECUTION-LOG.md D-001`). That back-link is what makes the round trip auditable in both directions: from the row to the evidence, and from the entry to whether anything came of it. An entry whose proposal was never accepted or refused is visible as such, which is the state a log without acceptance records cannot show.

## Where the log lives

`rfcs/EXECUTION-LOG.md`, beside the documents it records departures from. Where there is no RFC directory, put it next to the design doc being executed, under that name.

**The first completed execution unit creates it** — not the first departure, and not the directory's creation. A unit that departed from nothing still executed, and `Drift count: 0` is the claim that says so; a log that only appears once something goes wrong cannot distinguish a clean run from an unexamined one, which is the distinction it exists for. Creating it while initializing an RFC directory is the case to avoid: nothing has been executed yet, so the file would carry no claim at all.

Three properties, each load-bearing:

- **It is not in the RFC.** Both this skill and `rfc-writer` forbid editing an RFC's prose to match what was built, so the log cannot live there — it would be the same laundering with an extra step. It is a sibling document, and `rfc-writer` treats it as the one non-numbered resident of `rfcs/`.
- **One file for the whole collection, not one per RFC.** Departures cross documents: the run that executes RFC 0006 is the run that finds RFC 0005 was built wrong. A per-RFC log has nowhere to put that, and splitting it hides exactly the cross-document findings that are hardest to find any other way.
- **It carries no number and is not an RFC.** It is not a design, it has no status, and it never appears in the index table. `INDEX.md` links to it in prose instead.

## Log structure

Entries are grouped by **execution unit** — the branch, wave, or phase the work shipped as. Each group opens with what it covered and the number the whole practice is measured by:

```markdown
# Wave 2 · Seed and observability

Branch `feature/wave-2-seed-and-observability`. RFC 0006 P1, RFC 0003 P3.

**Drift count: 1** — D-023, against wave 1, found by wave 2.
```

- **Write the drift count on every group, including when it is zero.** "No drift" is a claim being made, not an absence, and a reader cannot tell an honest zero from a group whose author never looked. Where it is non-zero, name the entries and say what they were against — drift found by a later unit against an earlier one is the most valuable line in the log, and the easiest to leave implicit.
- **`D-NNN` numbers run continuously across the whole file** and are never reset per group. They are identifiers: RFC decision rows cite them, and a number that means two things breaks every citation that used it.
- **Departures from the execution plan are recorded too**, not only departures from an RFC. Re-scoping a wave mid-flight is a decision someone will later want the reasoning for, and it belongs in the same file as the reasoning it came from.

Two sections close a group, and both exist because a log that is only ever appended to is a log nobody re-reads:

- **Rules distilled** — the generalisations the entries support, one line each, cited back to the finding (`distill-the-rule`). This is the step that turns the log from a record into something that changes the next unit's behaviour.
- **Carried into the next unit** — what the group leaves unfinished or newly known, with closed items struck through rather than deleted.

`self-audit`'s post-execution pass lands here too, as a dated findings table. Its findings are departures the executor did not notice, which is the same kind of record as the ones they did — and keeping them in separate places means nobody counts them together.

## Classifying a departure

One question decides the class: **could this have been known before code existed?**

| Class | Test | What it means |
|---|---|---|
| `discovery` | No — only building it revealed this | Healthy. The RFC was right to be silent. |
| `spec-gap` | Yes — the RFC was silent or pitched at the wrong altitude | The design process missed something. Feeds `rfc-writer`. |
| `drift` | Yes — the RFC covered it and it was built otherwise anyway | **A defect.** Not a record of a decision, a record of a mistake. |
| `irreducible` | Neither — no amount of design settles it | Stop. Spike it. Ship the information, not the code. |

`drift` entries should be zero. A non-zero `drift` count is a finding against the executor, not against the document — and it is the class that makes review expensive, because it is the one the reader cannot anticipate.

`irreducible` is the one class where the correct move is to stop implementing. A throwaway implementation whose only deliverable is information is cheaper than a real one built on a guess, and much cheaper than the RFC amendment that follows discovering the guess was wrong.

## Honesty floor

- **A group is always opened in the log, even when it has no entries.** "Nothing departed" is a claim being made, not an absence. Omitting the group is not the same as asserting nothing changed, and reviewers cannot tell the two apart — the drift count exists to be written down at zero.
- **A `LOCKED` row is never flipped by the executor**, including when the flip is obviously correct. Obviously-correct flips are exactly the ones that need a second reader, because the executor's confidence is the thing under test.
- **Rationale is a mechanism, not a preference.** "Cleaner" and "more idiomatic" are not reasons; they are the sound of drift being written down as discovery.
- **Departures that change a contract** — error kinds, retry semantics, ordering guarantees, public surface — are logged even when they look like implementation detail. If another implementation of the same port would now behave differently, it is a contract change, and the shared conformance battery is re-run rather than assumed still valid (`reading-isnt-proof`).

## Failure modes

- **Plan theatre.** A plan that restates the RFC and lists no open decisions has skipped the only step that pays. If the third section is empty on a substantial change, the plan was not actually written against the code.
- **Retroactive logging.** Deviations reconstructed at the end are reconstructed from the code, which means they describe what was built rather than what was decided, and `drift` becomes indistinguishable from `discovery`.
- **Grade inflation.** Marking rows `LOCKED` by default makes halting routine, and routine halts get waved through. Grade honestly; most rows are `ASSUMED`.
- **Log as landfill.** An execution log nobody reads is overhead. It earns its cost only when it is periodically classified and distilled — see `distill-the-rule`. The per-group *Rules distilled* section is where that happens; a log with entries and no distilled rules is one nobody has re-read.
- **The log with no proposals accepted.** Entries accumulate, `Proposed row` lines pile up, and no RFC ever gains a row. The reconciliation half is the half that changes the documents; without it the log is a private diary of disagreements with a spec that still says the old thing.

## References

- `references/execution-log-template.md` — the log's skeleton, with the classes table and a worked entry; use when creating the file for the first time

## Related skills

- `rfc-writer` — authors the decision table and grades, and owns reconciliation
- `self-audit` — adversarial pass at branch completion, including conformance to the decision table
- `distill-the-rule` — turns a group's entries into the *Rules distilled* section that closes it
- `ratchet-what-you-build` — moves the execution log off convention and into enforcement
- `pr-review-loop` — carries this unit's departures and drift count into review
- `reading-isnt-proof` — a contract-class departure invalidates the shared battery until it is re-run
