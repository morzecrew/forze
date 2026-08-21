# Where a rule lives, and keeping the collection honest

The escalation ladder from a session note to an enforced check, and the
maintenance a set of rules needs to stay worth reading. `SKILL.md` carries the
qualification test and the distillation move.

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
