---
name: reading-isnt-proof
description: Use when one contract has two or more implementations — adapters, backends, clients, or a fake standing in for the real thing — and you are about to report that nothing tests something. Not for a single implementation, and not where a shared battery already exists.
roles: [implement, review]
gate: none
gate_reason: the battery this demands is the gate; it lives in the project under audit, not here
---

# Reading Isn't Proof

When two or more implementations share one contract, **a code read that concludes
"they agree" is a hypothesis, not a result.** Do not report "there's a test gap,
but no defect" and stop. Write the executable comparison, run it, and let it
decide.

If a gap is worth naming out loud, it is worth the ~30 minutes the battery costs.

**One implementation is out of scope, and that is not a hedge.** A promise with a
single implementation is a definition, not a claim that can diverge — this rule
does not license writing batteries for everything. Two is the threshold, and a
fake plus the real thing it stands in for is the best case of two, because that
pair is what everyone else's tests are silently trusting. Where a shared battery
for the contract already exists, extend it rather than fork it.

## The rule

> Close a named test gap in a shared contract even when you believe there is no
> defect behind it. The battery is cheap; the reading is not proof.

The reason is not that code reads are usually wrong. It's the *shape* of the
error they make: **you generalize from the part you inspected to the whole.** You
read the method where the interesting promise lives, satisfy yourself that all
implementations agree there, and conclude "this contract is consistent" — never
putting the neighbouring method's behavior side by side, because you have already
decided. A battery carries no such prior. It checks the axis you skipped.

## Procedure

1. **Enumerate the promises.** Read the contract's own docstrings/spec text and
   list every testable guarantee. Grep for promise language across the interface:
   `idempotent`, `no-op`, `never`, `always`, `guaranteed`, `exactly once`,
   `at most one`, `must never`, `fails closed`. Written promises with no test are
   the highest-yield place to look.
2. **One shared module, one check per promise**, parametrised over *every*
   implementation. Not per-backend test files — those are how the gap formed.
3. **Assert the discriminating detail, not the outcome** (see below).
4. **Include a positive control** so the battery cannot pass vacuously.
5. **Run it before deciding whether there was a defect.** Then report what it
   actually said.

For the concrete file shape this converged on, see
[references/battery-template.md](references/battery-template.md).

## Three properties, or the battery proves nothing

A battery that runs green against every implementation may be measuring nothing
at all. Each of these has been the difference, and all three are in
[references/battery-craft.md](references/battery-craft.md) with the case that
produced them:

- **Assert the discriminating detail.** Checking that both implementations raise is not conformance; checking that both raise the *same kind*, with the same message exposure and the same retryability, is.
- **Exercise the discriminating state.** A contract that differs only on the empty case, the concurrent case, or the second call is a contract whose battery must reach those states deliberately.
- **Keep a positive control.** One assertion that fails when the shared code is broken. Without it, "all backends agree" and "the battery never ran" are the same green.

## Reporting

State three things: what the battery covers, what it found, and what you changed.

- Found a divergence → name the axis, both behaviors, and the user-visible
  consequence (status code, wrong value, duplicate execution).
- Found nothing → "battery green across N implementations; the gap was coverage,
  not correctness." That is a finished job, not an empty one.

## Quick checklist

- [ ] Does this contract have ≥2 implementations?
- [ ] Am I about to report a gap without running anything?
- [ ] Is there one shared module, parametrised over all implementations?
- [ ] Does every check assert a discriminating detail (kind, state, value)?
- [ ] Does each check run in the state the production caller actually produces?
- [ ] Is there a positive control that makes the key check observable?
- [ ] Can each check fail for a reason I can name out loud?
- [ ] Did I run it *before* concluding whether a defect exists?

## Related skills

- `fewer-tests-more-proof` — the suite-wide economics: battery-ifying a
  multi-implementation contract is one of its consolidation moves; this skill
  owns the battery craft.
- `self-audit` — its verification-honesty pass is where this rule fires during a
  branch audit.
- `readable-code` — check names are the battery's documentation; each one
  states its claim. Absent it, name each check for the promise it tests, not
  for the function it calls.
- `rfc-writer` — when the battery surfaces a contract question too big to settle
  in the fix.
