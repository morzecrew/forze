---
name: reading-isnt-proof
description: Never close a test gap in a multi-implementation contract on the strength of a code read - when one contract has two or more implementations and you are about to say "nothing tests X", write the shared conformance battery and run it, even when you believe there is no defect. Use when auditing ports/interfaces/adapters with several backends, verifying a mock or fake against the real implementation it stands in for, reporting a test gap, or when the user mentions conformance, parity, differential testing, "do they behave the same", or cross-backend behavior.
---

# Reading Isn't Proof

When two or more implementations share one contract, **a code read that concludes
"they agree" is a hypothesis, not a result.** Do not report "there's a test gap,
but no defect" and stop. Write the executable comparison, run it, and let it
decide.

If a gap is worth naming out loud, it is worth the ~30 minutes the battery costs.

## Use this skill when

Both of these hold:

1. **One contract, two or more implementations.** A port with several adapters, an
   interface with several backends, a spec with several clients. A mock/fake plus
   one real implementation counts — and is the best case, because that pair is
   what everyone else's tests are silently trusting.
2. **You are about to say "no test covers X"** — whatever you currently believe
   about X.

Also reach for it when: auditing adapter parity, verifying a mock against a real
backend, or the user says "conformance", "differential", "parity", or "do they
behave the same?".

## Do not use this skill when

- The code has **one** implementation. A promise with one implementation is a
  definition, not a claim that can diverge — this rule does not license writing
  batteries for everything.
- A shared battery for that contract already exists — extend it, don't fork it.

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

## Assert the discriminating detail

This is where per-backend tests leak. A test written in isolation naturally asks
*"does it fail?"*. Only a shared battery asks *"fails **how**, and identically
everywhere?"*

| Weak — passes on divergent behavior | Strong — pins the contract |
| --- | --- |
| `pytest.raises(BaseError)` | assert the error **kind/code**: `exc.kind == CONFLICT` |
| matching an error *message* | assert the classified kind; messages are not contract |
| "did not raise" | assert the **resulting state** you expected it to reach |
| `assert result` | assert the value, the count, the ordering |
| retryable-ness assumed | assert the retryable/terminal classification explicitly |

Error kinds matter more than they look: they usually map to a transport status.
Two stores raising different kinds for the same client mistake means the same bug
returns **409 on one deployment and 400 on another**, decided by nothing but which
backend was wired.

Label every assertion with the implementation under test (`assert ..., h.backend`)
so a failure names which one disagreed.

## Exercise the discriminating state

Asserting the right detail is only half of it — the check must also run in the state
where the promise could actually break. A leg that sets up whatever state is
convenient can assert the discriminating detail perfectly and still be blind, because
the divergence lives in a state it never reaches.

The tell is a mismatch between the battery's setup and the **production caller's**
ordering. Ask of each promise: *which state does the real caller put this in, and is
that the state I set up?*

Concretely: a store method documented "unlike the other writes, this one is **not**
guarded on `RUNNING`" was exercised only against `RUNNING` runs — the single state in
which a spurious guard is invisible. The real caller writes it from a `finally`,
always *after* the terminal write. Adding a guard to one backend left the battery
green; adding the after-terminal leg made the two implementations disagree at once.

Promise language (step 1) tells you *what* to test. This tells you *where from*.

## Positive control

At least one check must establish the state that makes the interesting check
observable. If the battery would still be green with the feature ripped out, it is
measuring nothing. Run that as a literal experiment on the highest-stakes check —
rip the behavior out, watch for red. That is mutation testing by hand, and a
battery that cannot kill the ripped-out mutant scores zero.

Concretely: to prove "release leaves a claim it doesn't own alone", you need a
check that a held claim *is* refused. Without it, "still refused afterwards" could
mean "refused for an unrelated reason" or "nothing was ever claimed".

## The case that produced it

Sweeping an `IdempotencyPort` with three implementations (in-memory mock,
Postgres, Redis): read `fail` and `commit` on all three, saw the same
compare-and-set-on-the-exact-pending-claim logic, and reported —

> "There's a genuine test gap … but no defect. I didn't manufacture one."

The reading was right about `fail`/`commit`. The user asked for the battery
anyway. It found a real divergence **on the first run**, in `begin` — the method
that hadn't been put side by side: reusing an idempotency key with a *different*
payload raised `conflict` on mock+Postgres and `precondition` on Redis. 409 vs
400 for the identical client mistake.

Why nothing had caught it: each backend's own suite asserted only *that* it raised
— `pytest.raises(CoreException)`, or a message match. Never the kind.

## Anti-patterns this rule must not become

- **Manufacturing findings.** If the battery comes back green, say so plainly. A
  clean result after three probe rounds is a legitimate outcome, and reporting it
  honestly is part of the rule, not a failure of it. Never dress a green run up as
  a near-miss.
- **Battery-as-ritual.** A check asserting "did not raise" adds a green tick and
  no information. If a check cannot fail for a reason you can name, delete it.
- **Testing everything.** The trigger is a *named* gap in a *shared* contract,
  which is rare. Run the new battery legs and the existing tests on the paths you
  touched — not the whole suite, not new suites for single-implementation code.
- **Fixing the mock to match a bug.** When implementations disagree, decide which
  behavior the contract *should* have, write it into the contract's docs, then
  converge the outliers. A divergence resolved by copying whatever the majority
  does leaves the contract still unwritten.

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
- `self-documenting-code`, `naming-things` — check names are the battery's
  documentation; each one states its claim.
- `rfc-writer` — when the battery surfaces a contract question too big to settle
  in the fix.
