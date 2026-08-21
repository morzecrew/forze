---
name: reproduce-then-fix
description: Use when fixing any bug, flaky test, or incident; when a fix is proposed with no failing reproduction; when asked to just patch it; or when an error disappeared and nobody can say why. Not for triaging whether something is a bug at all.
roles: [implement, revert]
gate: verified-red
---

# Reproduce, Then Fix

A fix you can't watch fail is a guess, and a fix you can't explain is a coincidence. Both regress. The discipline that prevents this is one loop, applied to every bug regardless of size: **reproduce red → minimize → explain the mechanism → fix the cause → watch red turn green → keep the repro.** Each step exists because skipping it produces a specific, well-known failure: patches that mask symptoms, "fixed" bugs that were never the bug you saw, and regression tests that would pass with the bug still present.

## The loop

Six steps, in [references/the-loop.md](references/the-loop.md): reproduce red,
minimize, explain the mechanism, fix the cause, watch the same red turn green,
keep the reproduction.

Three of them are the ones people skip, and each skip has a signature:

- **Red first.** A test written after the fix has never been seen failing, so nothing establishes it would have caught this. If it cannot be made to fail against the unfixed code, it is not a reproduction.
- **Explain the mechanism.** A fix that works without an explanation is a coincidence you have decided to trust. Until you can say why the wrong behaviour happened, you cannot say the fix addresses it rather than perturbing it.
- **Fix the cause, not the observable.** Suppressing the symptom — a catch, a retry, a guard on the value that was already wrong — closes the ticket and leaves the defect, now harder to find.

## Certifying the red

```bash
python3 scripts/verified_red.py --test "pytest tests/test_auth.py::test_expired" --fix fix.patch
```

It runs the test twice in a throwaway worktree — once without the fix, once with
— and certifies only if the first run is red and the second green. That is the
one claim a reproduction makes and the one nobody checks by hand, because by the
time the fix is written the red run is a memory.

## No unexplained transitions

The loop's underlying rule runs in both directions:

- **No unexplained green.** When something passes that you expected to fail — a sabotage that didn't trip a test, a bug that "went away" after an unrelated change, a repro you can no longer trigger — find out why before moving on. It's either a second guard you didn't know about, a dead check, or the bug relocating. All three are worth knowing; none is "fine".
- **No unexplained red→green.** "It works now" without a mechanism means the trigger condition is still latent — timing, ordering, cache state — and will return on its own schedule.

## When you cannot reproduce

Downgrade the claim honestly instead of shipping hope:

- Say "unreproduced" — never "fixed". A speculative fix may still ship, labeled as speculative.
- Make the next occurrence diagnosable: add the assertion, the log line, the metric at the suspected site — instrument for the hypothesis, so the field becomes your reproduction environment.
- Capture what the report *does* pin down (version, input shape, frequency) so the search space is smaller next time.

## Related skills

- `determinism-by-design` — the machinery that turns probabilistic failures into on-demand reproductions
- `self-audit` — verified-red comes from its verification-honesty pass (9); re-auditing your own fixes is its pass 8
- `reading-isnt-proof` — assert the discriminating detail in the kept regression test
- `error-taxonomy` — a bug that surfaced as the wrong error kind gets fixed at the classification, not the message
