---
name: flag-dont-flip
description: Use when implementing against an RFC, spec, ADR, or design doc that carries graded decisions; when a decision needs to change mid-implementation; or when code has drifted from its spec. Not for authoring the design, and not for changes with no design document.
roles: [implement, revert]
gate: log-check
---

# Flag, Don't Flip

When reality contradicts a decision, **report the contradiction — do not quietly
pick the other branch.**

The decision was made by someone with context you do not have. Finding that it no
longer holds is worth knowing; acting on it without recording it destroys that,
and leaves a codebase disagreeing with its own specification with nothing to say
when or why. Every hunk of the diff then has to be read as a possible unannounced
design change, which is why reviewing code against a spec costs more than
reviewing the spec did.

If work is underway, no design document exists, and the change is clearly
load-bearing, say so and hand off to `rfc-writer` rather than inventing
decisions inside the implementation.

This body runs past the collection's 1,500-token budget, and what is left is
rules rather than argument: the grades, the plan gate, the entry, the classes.
Splitting any of them out would put the thing you need mid-task one file away
from the moment you need it. Everything explanatory has already moved.

## The grade decides the action

| Grade | On contradiction | Logged action | Never |
|---|---|---|---|
| `LOCKED` | **Halt.** Write the entry, stop, escalate. | `halted` | Proceed — not even when the alternative is obviously better. |
| `ASSUMED` | **Depart.** Write the entry, build the better option, carry on. | `departed` | Halt. You were licensed to decide this. |
| `OPEN` | **Decide.** Write the entry recording the choice and why, carry on. | `decided` | Halt, or hand back half an implementation. |
| `UNLISTED` | **Decide, and owe a row.** Write the entry with the row it proposes. | `decided` | Treat it as `OPEN`. |

`rfc-writer` owns the table's format; this skill owns the behaviour against it.

Two symmetric failures. **Flipping a lock** leaves the spec fiction and nobody
knows. **Halting on an assumption** costs a human round-trip that the grading
existed to avoid. Over-caution is a real failure here, not a safe default.

**An unlisted decision is not `OPEN`.** `OPEN` means the author looked and chose
not to settle it. Unlisted means nobody looked. A gap filled silently is
indistinguishable in the diff from a decision reversed silently, so it carries
the same weight as a departure and always owes a proposed row back.

## Plan first, and stop

Produce a plan and **never write code in the same turn as the plan.** The plan
carries: files touched, one line each; for every non-trivial choice, which
decision row governs it; and **the decisions the plan needs that the spec does
not settle.**

That third list is what this exists for. A gap found there costs a paragraph;
the same gap found in review costs a re-implementation.

**Readiness gate:** three or more load-bearing entries in that list means the
spec is not ready to execute. Report and stop. Executing an under-specified spec
does not produce an implementation — it produces a second, undocumented design,
expressed in code and discoverable only by reading it.

Read the rejected-alternatives section before any code. Those are the shapes the
implementation will keep reaching for, and each one was already argued down.

## Halting on a `LOCKED` row is a success

Stopping with working code and an unfinished task is the correct outcome. State
it plainly:

> Halted on D-3 (`LOCKED`): sessions in Redis. This environment has no Redis
> service. Entry written to `logs/T-0142.md`. Needs a human decision.

Do not soften it into "I went ahead with Postgres sessions since Redis wasn't
available, let me know if you want it changed." That is a flip wearing a
disclaimer.

## Write the entry before you act

Not after. An entry written afterwards is a rationalisation of a decision already
taken, and reads like one — deviations reconstructed at the end are reconstructed
*from the code*, so they describe what was built rather than what was decided.

Append to `logs/<task-id>.md`. **One file per task**, never a shared log, which
is a write hotspot the moment two workers run at once. **Append-only:** never
edit or delete an existing entry, including your own from an earlier attempt; if
you were wrong, append a new entry saying so.

```divergence
decision: D-3
grade: LOCKED
class: spec-gap
at: 2026-08-20T11:04:12Z
attempt: 2
claim: sessions cannot live in Redis; this deployment has no Redis service
evidence: infra/compose.yaml:1-40
action: halted
proposal: LOCKED — sessions live in Postgres until a Redis service is provisioned
```

Two fields do the work, and both are in full in
[references/entry-format.md](references/entry-format.md). **`grade` is copied
from the task as it stands now**, never re-read from the current spec — the log
records what was in force when you acted. **`evidence` must be locatable by
someone else**: a `path:line`, a range, or a backticked command with its output.
"Redis isn't available here" is a claim, and `claim` is where claims go; an entry
whose evidence cannot be found is discarded, and a discarded entry counts as
none.

And never amend the spec from inside a task. Editing its prose, table or grades
launders the flip; your entry *is* the amendment proposal. Where you followed the
design but the alternative is worth naming, record that too — the reference shows
the shape.

## Class answers one question: could this have been known before code existed?

| Class | Test | What it means |
|---|---|---|
| `discovery` | No — only building it revealed this | Healthy. The spec was right to be silent. |
| `spec-gap` | Yes — the spec was silent, or pitched at the wrong altitude | The design process missed something. |
| `drift` | Yes — the spec covered it and it was built otherwise anyway | **A defect.** A record of a mistake, not of a decision. |
| `irreducible` | Neither — no amount of design settles it | Stop and spike. Ship the information, not the code. |

`drift` should be zero, and a non-zero count is a finding against the executor
rather than the document — it is the class a reader cannot anticipate, which is
what makes review expensive.

**Write `Drift count: N` in every log, including at zero.** A missing count and
an honest zero read identically, and only one of them is a claim. Revise it the
way everything else here is revised — **append a new count line**, never edit the
earlier one. The checker reads the last one, so the file keeps both the claim you
made first and the one that turned out to be true.

## Silence is what gets caught

No tool detects that you violated a lock; that is not mechanically detectable.
What is trivially detectable is the **absence of an entry** in an area a locked
decision governs. So when in doubt about whether a contradiction is worth
reporting, report it. A surplus entry costs a reader ten seconds; a missing one
is an unexplained divergence found months later.

The check cannot tell honored-quietly from worked-around-quietly, so compliant
work in a touched `LOCKED` area owes an entry too: a **close-out** — `kind:
resolved`, `action: decided`, no `class`, evidence locating the compliant
implementation. Shape and legality in
[references/entry-format.md](references/entry-format.md).

## Checking the log

```bash
python3 scripts/log_check.py --log logs/T-0142.md --root . \
    --task tasks/T-0142.json --base origin/main
```

It checks the schema, grade-to-action legality both ways, the drift count against
the entries, that every citation resolves under `--root`, and — given `--task`
and `--base` — that every `LOCKED` decision whose declared paths the diff touched
has an entry. Run it in CI as the `decisions-reported` gate.

A decision declaring no `paths` is **reported as skipped, never as passed**: a
silence check that guesses is a silence check that approves. Task file and log
skeletons: [references/log-template.md](references/log-template.md).

Where `rfc-writer` is not installed, decision tables may be absent or ungraded.
Treat every decision as `LOCKED` and halt on any contradiction rather than
guessing a grade.

## Failure modes

- **Plan theatre.** A plan that restates the spec and lists no unsettled decisions has skipped the only step that pays.
- **Grade inflation.** Marking rows `LOCKED` by default makes halting routine, and routine halts get waved through. Most rows are `ASSUMED`.
- **The log with no proposals accepted.** Entries accumulate, `proposal` lines pile up, and no spec ever gains a row. Without the reconciliation half, the log is a private diary of disagreements with a document that still says the old thing.

## Related skills

- `rfc-writer` — authors the decision table and grades, and owns reconciliation. Absent it, treat every decision as `LOCKED`, as above.
- `self-audit` — adversarial pass at task completion; its findings are departures the executor did not notice, and belong in the same log. Absent it, the drift count is self-reported and worth less.
- `distill-the-rule` — turns a run of entries into rules that change the next task's behaviour. Per-task logs have no place to close a unit, so this is the step that stops the collection being a record nobody re-reads; absent it, re-read the logs periodically anyway.
- `ratchet-what-you-build` — the reason `log_check.py` is a CI gate rather than a habit.
- `reading-isnt-proof` — a contract-class departure invalidates the shared battery until it is re-run.
