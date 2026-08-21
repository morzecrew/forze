---
name: distill-the-rule
description: Use when a session ends in a genuine surprise, when a defect's shape will clearly recur, when a sabotage or test passes unexpectedly, after a postmortem, or when the same mistake class shows up twice. Not for one-off trivia, or anything the repo already records.
roles: [author]
gate: none
gate_reason: whether a finding generalises is the judgement; a check counting rules would reward writing more of them
---

# Distill the Rule

Findings are perishable; rules compound. A debugging session that ends with "fixed it" has produced one repaired instance. The same session ending with *"any suffix/subset helper needs its empty case decided explicitly"* has produced a check that prevents the whole class — in every future codebase, forever, for the cost of one sentence. Distillation is the deliberate last step of any surprising piece of work: strip the finding's specifics down to the transferable mechanism, phrase it as a one-line rule with its trigger, and file it where future work will actually meet it.

This is how individual experience becomes leverage — and for an agent with persistent memory, it is the difference between having sessions and having judgment.

## What qualifies

Three properties, all required — they filter out both noise and diary entries:

1. **Surprise.** Your prior was wrong: the code did something you didn't predict, the test was blind where you trusted it, the tool lied. No surprise → the rule already existed in your head; writing it down again adds nothing.
2. **Cost.** The finding took real time, a real bug, or a real incident to acquire. Cost is the evidence that the rule wasn't obvious.
3. **Recurrence shape.** The mechanism — not the instance — will appear again. "This function had an off-by-one" has no shape; "boundary semantics of helpers get inherited from the implementation instead of decided" does.

## The distillation move

Strip every specific that doesn't carry the mechanism — file names, project names, the particular types — and keep exactly what transfers:

| Finding (specific) | Rule (distilled) |
| --- | --- |
| `_is_complete_suffix([], stored)` returned True, so an empty replay read as complete | Any "is X a prefix/suffix/subset of Y" helper must have its empty case *decided*, not inherited from the implementation |
| `track()` called `finish()` unconditionally and corrupted paused jobs | When you add a convenience wrapper over a vocabulary, test the wrapper against **every state** of that vocabulary |
| Removing a Meilisearch guard didn't turn the battery red — a second guard also filtered | When a sabotage passes, find out **why** before concluding "blind check" or "code is fine" — it may be a second guard, or a dead assertion |
| A grep for `from x import y` marked a live facade "dead" | Verify "dead code" claims with attribute-access greps too, not only `from`-imports |

**The transfer test:** would this rule have prevented the same finding in a *different* codebase? If it still mentions your module names, it's under-distilled (a bug note, not a rule). If it's "be careful with edge cases", it's over-distilled — vaporous advice with no trigger. The right altitude names a **recognizable situation** and a **decidable action**.

## Rule anatomy

One line, imperative, with its trigger condition built in — "when/any/before X, do Y" — so the rule fires on recognition, not on recall. Optionally: one sentence of *why* (the failure it prevents), and a link to the incident that produced it (the evidence is what separates a rule from an opinion, and the link keeps it re-examinable).

## Filing it

A rule filed where nobody will meet it again has not been distilled, only
written. The ladder — session note, project memory, `CLAUDE.md`, a skill, a check
— and the maintenance that keeps the set worth reading are in
[references/filing.md](references/filing.md).

The rule that decides the rung: **file it where the work that would break it
happens.** A rule about commit messages belongs in the commit path, not in a
document someone reads at onboarding.

## Prefer a gate to a new rule

Before writing the rule down, ask whether a **program could refuse instead**. A
rule is a hope that the next reader remembers; a gate is a thing that says no.
Where the leak is mechanically detectable — a format, a missing field, a check
that was widened rather than satisfied — the distilled output is a check, and the
one-line rule is its error message (`drift-to-gate`).

Rules are for what no program can decide. Writing one for something a gate could
have caught is how a collection of rules grows past the point where anyone reads
it, and every rule added after that dilutes the ones that were load-bearing.

## Related skills

- `self-audit` — its reporting step ("distill rules") feeds this skill; the audit finds, this skill keeps
- `ratchet-what-you-build` — the top of the escalation ladder: rules that graduate into enforcement
- `drift-to-gate` — what to do when a distilled rule is read, quoted, and reasoned around anyway
- `reproduce-then-fix` — explained mechanisms are exactly the findings worth distilling
- `rfc-writer` — decision-log discipline is the same move for design choices
