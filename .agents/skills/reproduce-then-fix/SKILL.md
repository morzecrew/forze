---
name: reproduce-then-fix
description: Fix bugs by reproduction and root cause, never by plausible patch — no fix ships without a red reproduction seen failing first, an explained mechanism, and the repro kept as a regression test. Use when fixing any bug, flaky test, or incident; when asked to "just patch it" or "make the error go away"; when a fix is proposed without a failing test; when debugging something that "works on my machine"; or when the user mentions bug fixing, root cause, RCA, regression, reproduction, or "why did this break".
---

# Reproduce, Then Fix

A fix you can't watch fail is a guess, and a fix you can't explain is a coincidence. Both regress. The discipline that prevents this is one loop, applied to every bug regardless of size: **reproduce red → minimize → explain the mechanism → fix the cause → watch red turn green → keep the repro.** Each step exists because skipping it produces a specific, well-known failure: patches that mask symptoms, "fixed" bugs that were never the bug you saw, and regression tests that would pass with the bug still present.

## Use this skill when

- Fixing any bug — from a failing test to a production incident
- A fix is being proposed (by you or anyone) without a failing reproduction
- A test is flaky and someone wants to retry, skip, or delete it
- The error is gone but nobody can say why ("it works now")
- Investigating "works on my machine" / environment-dependent behavior

## Do not use this skill when

- The defect is a typo-level mistake whose mechanism is self-evident from the diff — write the regression test, but skip the ceremony
- Triaging *whether* something is a bug — that's investigation; this skill starts once a wrong behavior is established

## The loop

### 1. Reproduce — red first

Before touching the code, produce the failure on demand: a test, a script, a curl — anything executable and repeatable. **You must see it fail.** This is the step most often skipped and the one that pays for all the others: a reproduction is the only proof that the bug you're about to fix is the bug that was reported, and the only instrument that can later certify the fix.

If the failure is probabilistic (a race, a timing window), the reproduction step includes making it deterministic — force the interleaving, control the clock, fix the seed (`determinism-by-design`). A flake is a bug with a probabilistic repro, and retrying it away is deleting the evidence.

### 2. Minimize

Shrink the reproduction until every remaining element is load-bearing: smallest input, fewest steps, least state. Minimization is not cosmetic — it's the fastest root-cause instrument you have, because each element you fail to remove is a clue, and each one you remove eliminates a hypothesis. Bisection (over commits, over input, over config) is minimization along a different axis; use whichever converges faster.

### 3. Explain the mechanism

Before writing the fix, state the causal chain from input to wrong output: *this* state, through *this* path, produces *this* result because *this* assumption is false. If you cannot complete that sentence, you don't have a root cause yet — you have a correlation, and a patch written now will be aimed at the correlation.

The test for whether you're done explaining: the explanation predicts. It tells you which other inputs also fail, which don't, and what the minimal correct change is. An explanation that can't predict is a story.

### 4. Fix the cause, not the observable

The symptom test: **does the change remove the mechanism, or does it make the observable go away?** Retry-until-green, lengthened sleeps and timeouts, catch-and-ignore, widened tolerances, special-casing the reported input — each makes the report disappear while the mechanism keeps running. Sometimes a mitigation *is* the right immediate ship (an incident wants the bleeding stopped) — but then say so: label it a mitigation, keep the bug open, and let the root-cause fix close it.

One fix per cause. If the investigation surfaced two mechanisms, they get two reproductions, two explanations, and two separately verifiable fixes — a combined fix that "handles both" can't tell you which half worked.

### 5. Verify — the same red turns green

Run the *unchanged* reproduction against the fixed code and watch it pass. Then run it once more against the unfixed code if you can (stash, revert, flag) and watch it fail — this is **verified-red**: a regression test that has never been seen red proves nothing, because it might pass for reasons unrelated to the fix (wrong setup, wrong assertion, testing the wrong layer). Both observations together are the certificate: same instrument, red before, green after.

### 6. Keep the reproduction

The minimized repro becomes a permanent regression test, named after the behavior it pins (not the ticket number), asserting the discriminating detail — the specific wrong value or error kind, not merely "doesn't crash" (`reading-isnt-proof` has the assertion table). Then audit the fix itself: a fix is new code written under pressure, with the same defect rate as any code written under pressure (`self-audit` pass 8 — what did the fix move, and what does its new position break?).

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
