# The ten audit passes

Where an author's blind spots concentrate, one pass each, plus how the same
passes read against work that is not code. `SKILL.md` carries the scope rule
and the reporting rule; this is the sweep itself.

## The audit passes

Walk these deliberately. Each names a place where author blind spots concentrate, and why.

### 1. The extras beyond the spec

Defects concentrate in what was added *beyond* the spec — the ergonomic wrappers, convenience helpers, "while I'm here" additions. The spec'd core received design attention (and usually has an RFC section pinning it); the extras were improvised mid-execution and received none. Audit them hardest. Also check the inverse: what did the spec ask for that was silently dropped or quietly narrowed? Divergence is fine; *unrecorded* divergence is a defect.

### 2. Wrapper × underlying-state interactions

When new convenience code wraps an existing vocabulary — a state machine, a lifecycle, a protocol, a template — check the wrapper against **every state** of the thing it wraps, not just the state the happy path exercises. The classic failure: a wrapper that unconditionally "finishes" a job works fine for running jobs and silently corrupts paused ones. Every underlying state the wrapper wasn't tested against is a live hypothesis.

### 3. Boundary and empty cases of new helpers

Any helper answering "is X a prefix/suffix/subset/match of Y" — or any reduce-over-a-collection — has an empty case, and it must be *decided*, not inherited from whatever the implementation happens to return (the empty list is a suffix of every sequence; an empty ruleset "passes" everything). Check zero, one, first, last, missing, duplicate.

### 4. Discipline drift against the surroundings

New code must follow the invariants its file already enforces: if every mutation in the file is under a lock, is yours? If every sibling classifies errors before retrying, does yours? Inconsistency here is either a real defect (a race, a swallowed error) or a misleading signal for the next reader — both are findings. Concurrency and security drift deserve the hardest look because running the code doesn't reveal them: a mutation outside the file's locking discipline, new external input reaching an interpreter or a path join, a permission widened "temporarily", a secret in a log line or test fixture. For documents: does the new section follow the structure, terminology, and claims discipline of its siblings?

### 5. Failure paths and unreachable branches

Happy paths get exercised by development itself; failure paths only run when things go wrong, so they are where untested behavior hides. Trace them explicitly: What happens on misconfiguration — does it fail loudly once, or spin in a retry loop warning forever? Can an error raised inside a loop reach the branch that's supposed to handle it, or does an inner catch-all swallow it, making the outer handler unreachable? Is anything silently dropped where it should refuse?

Give **cleanup paths** their own pass — `finally` blocks, teardown hooks, deferred writes. Anything there that can fail will replace the outcome propagating through it and skip everything after it, so a blip in bookkeeping destroys the real result: the exception the caller needed, the metric that records what happened. Ask of every cleanup statement, "if this raises, what did it just outrank?" An advisory write must never be able to outrank the outcome it trails.

### 6. Duplication you introduced

Executing a multi-part change tempts copy-paste: the same setup block in four test legs, the same classify-or-reraise stanza twice in one file. Find your repeats and extract them — duplication found *now* is cheap; found later it has already diverged. (The `less-code-same-behavior` skill is this pass at codebase scale, including when the honest verdict is to leave a repeat alone.)

### 7. Lies in prose

Comments, docstrings, error messages, and docs written during execution drift from what the code finally does. Re-read every prose claim in the diff against the behavior beside it: a comment saying "only a mapping guard" when the code also handles a second case, a doc promising "one-line migration" when the honest claim is a re-encryption sweep. Over-claims and stale claims are defects — the reader trusts prose *more* than code. Names are prose too: a helper named `ensure_x` that sometimes doesn't, a variable named for what it used to hold. A lying name misleads at every call site, not just where it's defined.

### 8. Your own fixes

Fixes written during the branch (including fixes from earlier audit passes) are new code with the same defect rate — often worse, because they were written under "just fix it" pressure. Re-audit each one: what did the fix move, and what does its new position break? A fix that relocates resolution into a retry loop converts a clean configuration error into an infinite warn loop. The audit isn't done until the fixes it produced have been audited too.

### 9. Verification honesty

Distrust every "tested" and "covered" claim, including your own — reading a test and believing it is not evidence (if the `reading-isnt-proof` skill is available, apply it here):

- **Run the suite** — actually run it, in the audit, and report the real output.
- **Verified-red:** for each fix, is there a test that demonstrably fails without the fix? If it wasn't run red, you don't know it guards anything.
- **Sabotage spot-checks:** for load-bearing checks, break the guarded behavior and confirm the check fails. When a sabotage *passes*, don't conclude "blind check" or "fine" — find out **why** (it may be a second, independent guard; it may be a dead assertion). The why is the finding. This is mutation testing by hand — PIT and Stryker automate the sweep, reporting killed/detected and surviving mutants — and a surviving sabotage is the literature's *equivalent mutant* question: whether a mutation changes behavior at all is undecidable in general, so the tools stop at "survived" and deciding whether a survivor is an equivalent mutant is separate manual assessment — the why is yours to find.
- **Patch coverage:** measure coverage of the new lines specifically, with the full test profile (unit-only can be wildly misleading). `scripts/audit_scope.py patch-coverage --base main --report coverage.xml` intersects the added lines with a Cobertura or LCOV report and names the uncovered ones; it reports `n/a` rather than a percentage when nothing was measured, because "100% of zero lines" is the vacuous pass this rule exists to prevent. The gaps that matter most are **detection branches** — code that only runs when the bug it detects is present, which is exactly the code that must not be dead.
- **Sabotage and coverage are not substitutes — run both, in that order.** Sabotage only probes the code you thought to mutate, so a clean sweep measures your imagination rather than your tests — and it leaves you *feeling* finished, which is exactly when you stop looking. Coverage finds the branch you never considered at all. When the two disagree, coverage is the one saying something new: a sabotage sweep that passes everything, sitting next to an uncovered detection branch, means that branch is **unproven** — neither shown to work nor shown to be reachable. Calling it dead is a separate claim, and it needs its own evidence: execute it deliberately, or trace the control flow that would reach it. A branch deleted on the strength of a coverage report alone is a branch deleted because nobody wrote its test.

### 10. Conformance to the decision table

If the work executed an RFC, spec or design doc, diff the branch against its decision table. **Every departure either appears in the execution log or is a finding — and so does every `OPEN` row.**

The `OPEN` half is easy to miss, because choosing one of the options an RFC delegated is not a departure from it: conformance can look perfect while the choice that was made, and why, exists nowhere but the code. Walk the `OPEN` rows separately and check each one has a logged decision with its rationale. An `OPEN` row that execution never answered is the other finding — the plan needed it and nobody noticed.

This pass differs in kind from the other nine, and it is worth knowing why. The rest are judgment calls — whether a boundary case matters, whether a wrapper is really unsafe in that state. This one has an oracle: a departure is either recorded or it isn't, and the document says which. That makes it the cheapest pass to run, and the only one where **"found nothing" is a credible result** rather than a sign the audit was shallow.

It also catches the one failure the executor cannot catch alone. `flag-dont-flip` logs departures the executor *noticed*; this pass finds the ones they didn't, which are the departures a reviewer has no way to anticipate. Read the decision table first and the diff second — the other order lets the code tell you what the design "must have meant".

A departure from a `LOCKED` row is a finding regardless of how right the code looks. That grade means reopening was expensive enough to want a second reader, and an executor's confidence is the thing under test.

## Translating to non-code work

The passes generalize: spec fidelity (§1) and prose honesty (§7) apply verbatim to documents; boundary cases (§3) become "what does the reader do in the case this section doesn't cover"; discipline drift (§4) becomes consistency with sibling documents; verification honesty (§9) becomes checking every link resolves, every number traces to a source, every claimed behavior was actually observed.
