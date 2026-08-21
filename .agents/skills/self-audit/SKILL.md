---
name: self-audit
description: Use when a branch, fix series, or document set of your own is finished and about to be merged or handed off, or when the user says "self-audit", "audit your work", or "check your own changes". Not for reviewing someone else's code, and not mid-flight.
roles: [implement]
gate: audit-scope
---

# Self-Audit

A self-audit is a deliberate post-execution pass where you become the adversary of your own work. The deliverable is finished — the RFC executed, the fix series written, the document drafted — and before it merges or ships, you re-enter it with one assumption: **it contains defects, and your job is to find them.** Not to confirm correctness, not to summarize what was done — to find what's wrong.

This works because the author's blind spots are systematic, not random. The same few places hide defects every time, so an audit that walks those places deliberately finds real bugs that the writing pass — and often the test suite — missed. A clean audit is a valid outcome — but on substantial branches (thousands of lines) it is rare, so it must arrive as a report carrying its evidence: the scope walked, the checks actually performed, and what remains uncertain (see Fixing and reporting). Never manufacture a finding to avoid reporting clean.

## Establish the scope first

Audit a defined body of work, not "the repo":

- **Code on a branch:** diff against the merge base (`git merge-base <base> HEAD`), list the commits, count the lines. State the scope in the report ("whole branch, 8 commits / ~6.5k lines"). Include everything the branch touched: source, tests, docs, config, CI. `scripts/audit_scope.py scope --base main` produces exactly this — commits, per-kind file and line counts, and the scope statement to open the report with (path relative to this skill's directory; from a repository root it is `skills/self-audit/scripts/audit_scope.py`).
- **Non-code work:** enumerate the artifacts produced (documents, configs, diagrams, plans) and audit that set.
- **Re-read the spec first.** Before reading the diff, re-read whatever the work claims to implement — the RFC, the task description, the ticket. The audit's first axis is deliverable-vs-spec, and you can't check fidelity from memory.

## The sweep

Ten passes, in [references/passes.md](references/passes.md) — load it before
auditing anything. They run in order, and the order matters: the cheap
structural passes narrow what the expensive verification passes have to cover.

Two of them carry the weight. **Verification honesty** — every claim in your own
report traced to something that ran, because a claim checked by reading is a
hypothesis wearing a result's clothes. And **sabotage before coverage, in that
order** — a clean sabotage sweep measures your imagination rather than your
tests, and it leaves you feeling finished, which is exactly when you stop
looking.

## Fixing and reporting

- **Fix as you find, on the same branch.** The work is your own and unmerged — clear defects get fixed immediately, then re-audited (§8). Leave open only what genuinely needs the user's decision (a spec change, a scope call), and say so.
- **Report findings, not activities.** For each finding: where, what's wrong, why it matters (the concrete failure it causes), and its status — fixed or open. Rank by severity.
- **State the scope and the residue.** What was audited, what wasn't, and what you'd still distrust. A no-findings audit is reported the same way: the scope, the checks actually performed, the evidence they produced, and the remaining uncertainty — that report is what lets a reader tell clean from shallow.
- **Where the work executed a spec, the report has a durable home:** a dated findings section in that task's log, `logs/<task-id>.md` (`flag-dont-flip`). These findings are departures the executor did not notice, and filing them apart from the ones they did means nobody ever counts the two together. The log's checker reads only the fenced entries, so a prose findings section sits there without disturbing it.
- **Distill rules.** When a finding generalizes, record it as a one-line rule ("any suffix/subset helper needs its empty case decided explicitly"; "test a wrapper against every state of the vocabulary it wraps") — these compound across future work. If the project keeps notes or memory, put them there.

## Related skills

- `reading-isnt-proof` — pass 9's discipline expanded into a full method for multi-implementation contracts
- `fewer-tests-more-proof` — when the audit's real finding is the suite itself: ritual tests, per-backend copies, flake-retry volume
- `flag-dont-flip` — owns the task logs pass 10 audits against, and grades the decisions it checks. Absent it, pass 10 still runs: diff the branch against the decision table and report every unlogged departure as a finding.
- `less-code-same-behavior` — pass 6 at codebase scale, with the same NO ACTION discipline
