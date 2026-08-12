---
name: distill-the-rule
description: Convert surprising findings into durable one-line rules — after a debugging session, audit finding, review surprise, or incident, strip the specifics down to the transferable mechanism and file it where future work will actually recall it. Use when a session ends with a hard-won discovery, when a defect's shape will clearly recur, when a sabotage or test passes unexpectedly, after a postmortem, when the user says "remember this" or "lesson learned", or when the same class of mistake shows up a second time.
---

# Distill the Rule

Findings are perishable; rules compound. A debugging session that ends with "fixed it" has produced one repaired instance. The same session ending with *"any suffix/subset helper needs its empty case decided explicitly"* has produced a check that prevents the whole class — in every future codebase, forever, for the cost of one sentence. Distillation is the deliberate last step of any surprising piece of work: strip the finding's specifics down to the transferable mechanism, phrase it as a one-line rule with its trigger, and file it where future work will actually meet it.

This is how individual experience becomes leverage — and for an agent with persistent memory, it is the difference between having sessions and having judgment.

## Use this skill when

- A debugging session, audit, or review ends with a genuine surprise ("huh — I didn't expect that")
- A defect turns out to be an instance of a shape that will recur (wrapper × state, empty case, drifted invariant)
- A sabotage/mutation passes, a test fails to fail, or anything transitions unexplained-then-explained
- Closing an incident or postmortem — the corrective actions want a distilled rule each
- The user says "remember this", "lesson learned", or you notice the same mistake class twice

## Do not use this skill when

- The finding is one-off trivia with no recurring shape (a typo, a vendor's quirk you'll never meet again)
- The repo already records it — code comments, CLAUDE.md, existing rules; check before writing (a duplicate rule dilutes the collection it joins)
- The "rule" would just restate what happened — if it can't outlive its incident, it isn't a rule yet (see the test below)

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

## Where rules live — the escalation ladder

File the rule at the level matching its audience and authority; promote it when reality proves it out:

1. **Session note** — it dies with the context. Only for rules still being tested.
2. **Durable memory / decision log** — the default landing place; recallable in future sessions, linkable from later findings.
3. **Team convention** — CLAUDE.md/AGENTS.md, a review checklist, a skill: now it instructs everyone (and every agent), not just you.
4. **Enforcement** — a lint rule, a CI gate, a fail-closed guard. The ultimate distillation is one that no longer relies on being remembered (`ratchet-what-you-build` — the rule's final form is a ratchet).

A rule that keeps firing usefully earns promotion up the ladder; the promotion itself is cheap because the rule is already phrased as a trigger + action.

## Maintenance — rules are claims

- **Re-verify on contact.** When a rule fires, check it still matches reality before applying it; codebases move, and a stale rule confidently applied is worse than no rule (the recalled-memory problem — verify the flag/file/behavior still exists).
- **Delete disproven rules.** A rule contradicted by evidence gets removed or rewritten *with the new evidence linked* — a collection that only grows becomes noise that buries its own best entries.
- **Dedup before adding.** New finding, existing rule → link the finding as further evidence and sharpen the rule if needed; don't mint a near-duplicate (`error-taxonomy`'s canonical-codes discipline, applied to knowledge).

## Anti-patterns

- **The diary entry** — recording what happened instead of what transfers; activity logs are not rules.
- **The platitude** — "test edge cases", "be careful with concurrency": no trigger, no action, fires never.
- **Rule hoarding** — collecting rules into a pile nothing recalls from; a rule that can't be *met* (wrong file, no index, no trigger phrasing) might as well not exist.
- **Distilling everything** — the three-property filter exists because a collection's value density is what makes it worth consulting; ten sharp rules beat two hundred observations.

## Related skills

- `self-audit` — its reporting step ("distill rules") feeds this skill; the audit finds, this skill keeps
- `ratchet-what-you-build` — the top of the escalation ladder: rules that graduate into enforcement
- `drift-to-gate` — what to do when a distilled rule is read, quoted, and reasoned around anyway
- `reproduce-then-fix` — explained mechanisms are exactly the findings worth distilling
- `rfc-writer` — decision-log discipline is the same move for design choices
