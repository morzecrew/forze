---
name: self-audit
description: Adversarially audit your own just-finished work — a branch after RFC/feature execution, a fix series, a document set — hunting for defects you introduced, before merge or handoff. Use whenever the user says "do self-audit", "audit your work", "self-audit this branch", "check your own changes", or asks to review what was just built before merging; also applicable to non-code deliverables (docs, configs, infra).
---

# Self-Audit

A self-audit is a deliberate post-execution pass where you become the adversary of your own work. The deliverable is finished — the RFC executed, the fix series written, the document drafted — and before it merges or ships, you re-enter it with one assumption: **it contains defects, and your job is to find them.** Not to confirm correctness, not to summarize what was done — to find what's wrong.

This works because the author's blind spots are systematic, not random. The same few places hide defects every time, so an audit that walks those places deliberately finds real bugs that the writing pass — and often the test suite — missed. On substantial branches (thousands of lines), a competent self-audit that finds *nothing* is evidence of a shallow audit, not a clean branch.

## Use this skill when

- The user says "do self-audit", "self-audit", "audit your work", or similar
- A branch is complete (RFC execution, feature, fix series) and about to be merged
- A multi-commit body of your own work needs a defect hunt before handoff
- The user asks you to double-check work you produced earlier in the session or in prior sessions

## Do not use this skill when

- Reviewing *someone else's* PR or code — use a code-review flow; self-audit's leverage comes from auditing the author's own blind spots
- The user wants a summary or changelog of what was done — that's reporting, not auditing
- Work is still mid-flight — audit a finished unit, otherwise findings and WIP blur

## Establish the scope first

Audit a defined body of work, not "the repo":

- **Code on a branch:** diff against the merge base (`git merge-base <base> HEAD`), list the commits, count the lines. State the scope in the report ("whole branch, 8 commits / ~6.5k lines"). Include everything the branch touched: source, tests, docs, config, CI.
- **Non-code work:** enumerate the artifacts produced (documents, configs, diagrams, plans) and audit that set.
- **Re-read the spec first.** Before reading the diff, re-read whatever the work claims to implement — the RFC, the task description, the ticket. The audit's first axis is deliverable-vs-spec, and you can't check fidelity from memory.

## The audit passes

Walk these deliberately. Each names a place where author blind spots concentrate, and why.

### 1. The extras beyond the spec

Defects concentrate in what was added *beyond* the spec — the ergonomic wrappers, convenience helpers, "while I'm here" additions. The spec'd core received design attention (and usually has an RFC section pinning it); the extras were improvised mid-execution and received none. Audit them hardest. Also check the inverse: what did the spec ask for that was silently dropped or quietly narrowed? Divergence is fine; *unrecorded* divergence is a defect.

### 2. Wrapper × underlying-state interactions

When new convenience code wraps an existing vocabulary — a state machine, a lifecycle, a protocol, a template — check the wrapper against **every state** of the thing it wraps, not just the state the happy path exercises. The classic failure: a wrapper that unconditionally "finishes" a job works fine for running jobs and silently corrupts paused ones. Every underlying state the wrapper wasn't tested against is a live hypothesis.

### 3. Boundary and empty cases of new helpers

Any helper answering "is X a prefix/suffix/subset/match of Y" — or any reduce-over-a-collection — has an empty case, and it must be *decided*, not inherited from whatever the implementation happens to return (the empty list is a suffix of every sequence; an empty ruleset "passes" everything). Check zero, one, first, last, missing, duplicate.

### 4. Discipline drift against the surroundings

New code must follow the invariants its file already enforces: if every mutation in the file is under a lock, is yours? If every sibling classifies errors before retrying, does yours? Inconsistency here is either a real defect (a race, a swallowed error) or a misleading signal for the next reader — both are findings. For documents: does the new section follow the structure, terminology, and claims discipline of its siblings?

### 5. Failure paths and unreachable branches

Happy paths get exercised by development itself; failure paths only run when things go wrong, so they are where untested behavior hides. Trace them explicitly: What happens on misconfiguration — does it fail loudly once, or spin in a retry loop warning forever? Can an error raised inside a loop reach the branch that's supposed to handle it, or does an inner catch-all swallow it, making the outer handler unreachable? Is anything silently dropped where it should refuse?

### 6. Duplication you introduced

Executing a multi-part change tempts copy-paste: the same setup block in four test legs, the same classify-or-reraise stanza twice in one file. Find your repeats and extract them — duplication found *now* is cheap; found later it has already diverged.

### 7. Lies in prose

Comments, docstrings, error messages, and docs written during execution drift from what the code finally does. Re-read every prose claim in the diff against the behavior beside it: a comment saying "only a mapping guard" when the code also handles a second case, a doc promising "one-line migration" when the honest claim is a re-encryption sweep. Over-claims and stale claims are defects — the reader trusts prose *more* than code.

### 8. Your own fixes

Fixes written during the branch (including fixes from earlier audit passes) are new code with the same defect rate — often worse, because they were written under "just fix it" pressure. Re-audit each one: what did the fix move, and what does its new position break? A fix that relocates resolution into a retry loop converts a clean configuration error into an infinite warn loop. The audit isn't done until the fixes it produced have been audited too.

### 9. Verification honesty

Distrust every "tested" and "covered" claim, including your own — reading a test and believing it is not evidence (if the `reading-isnt-proof` skill is available, apply it here):

- **Run the suite** — actually run it, in the audit, and report the real output.
- **Verified-red:** for each fix, is there a test that demonstrably fails without the fix? If it wasn't run red, you don't know it guards anything.
- **Sabotage spot-checks:** for load-bearing checks, break the guarded behavior and confirm the check fails. When a sabotage *passes*, don't conclude "blind check" or "fine" — find out **why** (it may be a second, independent guard; it may be a dead assertion). The why is the finding.
- **Patch coverage:** measure coverage of the new lines specifically, with the full test profile (unit-only can be wildly misleading). The gaps that matter most are **detection branches** — code that only runs when the bug it detects is present, which is exactly the code that must not be dead.

## Translating to non-code work

The passes generalize: spec fidelity (§1) and prose honesty (§7) apply verbatim to documents; boundary cases (§3) become "what does the reader do in the case this section doesn't cover"; discipline drift (§4) becomes consistency with sibling documents; verification honesty (§9) becomes checking every link resolves, every number traces to a source, every claimed behavior was actually observed.

## Fixing and reporting

- **Fix as you find, on the same branch.** The work is your own and unmerged — clear defects get fixed immediately, then re-audited (§8). Leave open only what genuinely needs the user's decision (a spec change, a scope call), and say so.
- **Report findings, not activities.** For each finding: where, what's wrong, why it matters (the concrete failure it causes), and its status — fixed or open. Rank by severity.
- **State the scope and the residue.** What was audited, what wasn't, and what you'd still distrust. "No findings" on a large branch demands an explanation of why the audit is believed thorough rather than shallow.
- **Distill rules.** When a finding generalizes, record it as a one-line rule ("any suffix/subset helper needs its empty case decided explicitly"; "test a wrapper against every state of the vocabulary it wraps") — these compound across future work. If the project keeps notes or memory, put them there.
