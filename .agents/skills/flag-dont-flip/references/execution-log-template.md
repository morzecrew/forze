# EXECUTION-LOG.md template

Copy the skeleton below into `rfcs/EXECUTION-LOG.md` when the first execution unit completes — whether or not it departed from anything, since `Drift count: 0` is a claim worth recording. Not when the RFC directory is created: nothing has executed yet, so the file would carry no claim at all.

The classes table is reproduced *inside* the log on purpose. The log is read by people who are not executing anything — reviewers, the RFC author, whoever picks the work up in six months — and a class name whose test lives in a skill file they do not have is a label they cannot check.

---

```markdown
# Execution log

Where building something disagreed with the design for it, written down at the
moment it happened. Nothing here is revised afterwards to agree with what was
later settled, and nothing here has been folded back into an RFC's own text.

The decision rows below are put forward for the author to accept or refuse.
Execution does not write them into a decision table itself.

## Classes

| Class | Test | Meaning |
|---|---|---|
| `discovery` | Could not have been known before code existed | Healthy — the RFC was right to be silent |
| `spec-gap` | Could have been known; the RFC was silent or at the wrong altitude | The design process missed something |
| `drift` | The RFC covered it and it was built otherwise anyway | **A defect** |
| `irreducible` | No amount of design settles it | Stop and spike |

---

# <Unit 1 · Name>

Branch `<branch>`. <Which RFCs and phases this unit covered.>

**Drift count: 0.**

## D-001 — <what departed, in a sentence a reader can scan>

- **Touches:** RFC <NNNN> §<N>, decisions row <N> (`<GRADE>`). <Or: nothing in
  the decision table covers this — it was unlisted.>
- **RFC said:** <the claim, quoted where the wording matters>
- **Built:** <what exists instead>
- **Because:** <the mechanism that forced it — not "cleaner", not "more idiomatic">
- **Class:** `<class>`. <Why that class and not the neighbouring one.>
- **Consequence:** <what is now true that the RFC's reader would not expect,
  including what door this closes>
- **Proposed row (RFC <NNNN>):** `<GRADE>` — <the row, written as it would
  appear in the decision table>

<Optional, where the design was followed but the alternative is worth naming:>

**Deliberately not applied:** <what the RFC sketches that was not built, and why
the existing code stands.>

## Decision-row outcomes — <YYYY-MM-DD>

<What the author accepted, refused, or superseded. A refusal is recorded here
and nowhere else, so it must be distinguishable from an acceptance at a glance
— otherwise a proposal that was turned down reads as an RFC row that exists.
Superseded rows stay in the RFC struck through, naming the row that replaced
them; `Row` is empty where nothing was appended.>

| RFC | Row | Outcome | Grade | Decision | From |
|---|---|---|---|---|---|
| <NNNN> | <N> | Accepted | `<GRADE>` | <the decision, one dense sentence> | D-001 |
| <NNNN> | — | Refused | — | <what was proposed, and the author's reason> | D-002 |

## Audit findings — <YYYY-MM-DD>

<Scope of the post-execution audit: commits, files, lines. Then the findings
table, ranked by severity, each with its status.>

| # | Severity | Finding | Status |
|---|---|---|---|
| A-1 | High | <what is wrong, and the concrete failure it causes> | Fixed — <how> |

**Sabotage sweep: <n> mutations, <n> killed.** <What they covered.>

**What remains distrusted.** <What the audit could not verify, and why.>

## Rules distilled

<Generalisations the entries above support, worth carrying into later units.>

- **<The rule, as one transferable line.>** <Why it holds, and the finding it
  came from.> (D-001.)

## Carried into the next unit

- **<What this unit leaves unfinished or newly known.>** <Detail.>
- **~~<A carried item that has since been closed.>~~** Closed <YYYY-MM-DD>:
  <what closed it>.

---

# <Unit 2 · Name>

Branch `<branch>`. <Scope.>

**Drift count: <n>** — <which entries, and what they were against. Drift found
by a later unit against an earlier one is named here explicitly.>

## D-002 — <...>
```

---

## Notes

- **`Built:` becomes `Found:` when nothing was built.** Some departures are discoveries about what already existed — the RFC said a module does not exist and it does. "Built" would be a small lie in the field a reader checks first.
- **Numbering is continuous across the whole file.** `D-001` in unit 1, `D-002` in unit 2 — never reset per unit. RFC decision rows cite these identifiers, and a number that means two things breaks every citation that used it.
- **Heading levels:** units at `#`, entries and unit-closing sections at `##`. The document has one `# Execution log` title and then reads as a sequence of units.
- **The drift count is written even at zero.** A missing count and an honest zero are indistinguishable to a reader, and only one of them is a claim.
- **Deleting nothing is the whole discipline.** Carried items get struck through when closed; superseded proposals stay with their outcome recorded. An entry that turns out to have been wrong gets a later entry saying so, not an edit.
- **The unit is whatever the work actually shipped as** — a wave, a branch, an RFC phase, a single PR. Group by the thing a reviewer looked at in one sitting, because that is the boundary a drift count is meaningful across.
