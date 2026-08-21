---
name: less-code-same-behavior
description: Use when asked to deduplicate, DRY up, consolidate, or converge divergent code, or to shrink a codebase without changing behavior. Not when behavior is meant to change, and not for reviewing a single fresh diff.
roles: [implement]
gate: none
gate_reason: scripts/usage_census.py answers who calls this; whether two things are one concern is the judgement
---

# Less Code, Same Behavior

A divergence-and-DRY audit hunts for places where a codebase spends more code than its behavior requires — the same block pasted N times, the same concern hand-rolled in N different shapes, one responsibility scattered across a dozen files, public surfaces exporting three times what anyone imports — and consolidates them into less code with **identical** behavior.

Two disciplines make this safe instead of destructive. First, every consolidation must respect the project's declared architecture: its layers, import contracts, and package boundaries are hard constraints, not obstacles. Second, the audit must be able to conclude **"leave it"** — a DRY pass that cannot reject its own findings produces churn, not consolidation.

## Map the constraints before proposing anything

Read the project's declared architecture first: layer definitions, import/dependency contracts (whatever tool enforces them — import linters, module-boundary rules, package manifests), and the public API surface (exported symbols, documented entry points, semver promises). These determine *where* consolidated code may live and *what* may not break. A consolidation plan written before this map is guesswork; the gate at the end must re-run whatever enforces these rules.

## The six passes

Run them in order; the cheap structural ones narrow what the expensive ones have
to read. Each in full, with the shape that gives it away and where the
consolidation should land, is in
[references/finding-passes.md](references/finding-passes.md).

| # | Pass | Looks for |
|---|---|---|
| 1 | Literal duplication | The same code in two places, copied |
| 2 | Same concern, divergent shapes | One job solved three ways, none of them wrong |
| 3 | Scatter | One responsibility spread across modules that each hold a piece |
| 4 | Surface bloat | A public API wider than anything calls |
| 5 | Type lies and config smells | A declared type the values do not honour |
| 6 | Parallel vocabularies | Two names for one concept, both live |

## Verify every claim before acting

- **Prove a copy is a copy.** Only merge code that changes for the same reason. Coincidental similarity — two blocks that look alike today but serve different owners with different futures — must stay separate; merging it manufactures coupling, the exact cost DRY exists to avoid. This is Sandi Metz's rule — "duplication is far cheaper than the wrong abstraction" — restated by Kent C. Dodds as AHA, *Avoid Hasty Abstractions*: merge on a proven shared reason-to-change, never on visual similarity.
- **Prove dead is dead.** Check every access pattern before deleting: `from`-imports, attribute access (`module.symbol(...)` — a grep for `from..import` alone misreads these as dead), re-export chains, reflection and string-based references, config/serialized references. A "dead shim" that is actually a load-bearing facade is a classic false positive.
- **Count before you conclude.** Importer counts, call-site counts, external-vs-internal usage splits. Numbers decide between shim-and-move, break-and-migrate, and leave-alone.

`scripts/usage_census.py <symbol>` does both mechanically — it counts every pattern above separately and splits internal from external usage:

```bash
python3 scripts/usage_census.py run_coverage --root /path/to/repo
python3 scripts/usage_census.py Helper --internal src/pkg/ --json
```

Exit 3 means nothing references it (a deletion candidate — the report then names what the tool still cannot see: dynamic dispatch by computed name, generated code, other repositories). The numbers are evidence for a verdict, not the verdict.

## NO ACTION is a first-class verdict

Most findings are not worth acting on, and saying so is the output. Duplication
that has never drifted, two implementations whose divergence is the point, an
abstraction that would need three parameters to cover both callers — each is a
finding whose correct verdict is to leave it. Calibration in full in
[references/finding-passes.md](references/finding-passes.md).

## The gate

Every consolidation step must prove behavior preservation before it counts:

- **Fowler-sized steps.** Refactoring's definition is a sequence of small behavior-preserving transformations, each too small to go wrong, run under green tests — so consolidate as a chain of gated steps, never a big-bang rewrite; a consolidation that can't be bisected can't be trusted. The moment a step wants a behavior change, that's the other hat: stop, split it into its own change.
- Full typecheck, lint, and the **architecture-contract check** (all import/layer rules green).
- The full relevant test suite, with counts recorded before and after — same tests, same results. A pure consolidation changes no test outcomes; if a test had to change, either the surface wasn't preserved (add the shim) or the change wasn't pure (split it out).
- Where a public surface gained a shim, add equivalence tests (old spelling ≡ new shape) — usually the only new tests a consolidation needs.

## Reporting

- Net line delta and the shapes collapsed ("3 hand-rolled dispatch blocks → 1 base + hooks", "14 modules → 6").
- Findings table: consolidated / NO ACTION with reason / deferred with the blocking boundary question.
- The gate's numbers, stated plainly.
- Remaining targets ranked, with an explicit stop-or-continue recommendation.

## Related skills

- `self-audit` — run it on the consolidation branch itself before merge
- `composition-over-inheritance` — when the shared shape for divergent implementations is being designed
- `fewer-tests-more-proof` — the same discipline applied to the test suite
