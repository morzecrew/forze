# The six-step loop

Each step with what it produces and how it can go wrong. `SKILL.md` carries the
rule the loop enforces; this is the loop.

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

Run the *unchanged* reproduction against the fixed code and watch it pass. Then run it once more against the unfixed code and watch it fail — this is **verified-red**: a regression test that has never been seen red proves nothing, because it might pass for reasons unrelated to the fix (wrong setup, wrong assertion, testing the wrong layer). Both observations together are the certificate: same instrument, red before, green after.

`scripts/verified_red.py` runs both halves and reports the verdict:

```bash
python3 scripts/verified_red.py --test-cmd "pytest tests/test_bug.py" --test-file tests/test_bug.py
python3 scripts/verified_red.py --base HEAD~1 --test-cmd "..." --test-file ...   # fix already committed
```

The red half runs in a throwaway git worktree at the base commit with only the named test files copied in, so your working tree is never touched and an interrupted run cannot strand your work. Pass `--test-file` for *every* new file the reproduction needs — the conftest, the fixture, the helper — since anything that exists in neither the base commit nor that list makes the red run die on a missing import instead of on the absent fix. That failure is refused rather than counted as red: a false red certifies a test that never ran, which is worse than running no check at all.

Pass `--allow-red-error` only when the import failure *is* the bug you fixed. Either half is killed after `--timeout-seconds` (default 900), and a killed run is not a red result either.

Exit 2 means not certified, and says which half broke — a red run that *passes* is the important one: the test does not guard what you fixed.

### 6. Keep the reproduction

The minimized repro becomes a permanent regression test, named after the behavior it pins (not the ticket number), asserting the discriminating detail — the specific wrong value or error kind, not merely "doesn't crash" (`reading-isnt-proof` has the assertion table). Then audit the fix itself: a fix is new code written under pressure, with the same defect rate as any code written under pressure (`self-audit` pass 8 — what did the fix move, and what does its new position break?).

**Typo-level defects skip the ceremony, not the test.** Where the mechanism is
self-evident from the diff — a transposed argument, an inverted comparison —
write the regression test and move on without the minimize-and-explain passes.
The test is what stops the defect returning; the ceremony is what explains a
mechanism that here needs no explaining.
