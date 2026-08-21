# Writing a battery that can actually fail

The three properties a conformance check needs, the case that produced this
rule, and the shapes it must not turn into. `SKILL.md` carries the rule and
the procedure.

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
