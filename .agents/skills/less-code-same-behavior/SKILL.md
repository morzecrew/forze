---
name: less-code-same-behavior
description: Deep divergence and DRY audit that shrinks a codebase without changing behavior — find copy-paste, same-concern-drift, scattered responsibilities, type-lying configs, wrong abstractions, and bloated public surfaces, then consolidate (or unwind) while respecting the project's declared layers and import contracts. Use when the user asks to deduplicate, DRY up, consolidate, converge divergent code, do a divergence analysis, shrink or simplify a codebase, reduce code size, or wants "less code with the same functionality".
---

# Less Code, Same Behavior

A divergence-and-DRY audit hunts for places where a codebase spends more code than its behavior requires — the same block pasted N times, the same concern hand-rolled in N different shapes, one responsibility scattered across a dozen files, public surfaces exporting three times what anyone imports — and consolidates them into less code with **identical** behavior.

Two disciplines make this safe instead of destructive. First, every consolidation must respect the project's declared architecture: its layers, import contracts, and package boundaries are hard constraints, not obstacles. Second, the audit must be able to conclude **"leave it"** — a DRY pass that cannot reject its own findings produces churn, not consolidation.

## Use this skill when

- The user asks to deduplicate, DRY up, consolidate, or "converge" code
- The user asks for a divergence analysis — same concern implemented differently in several places
- A codebase or subsystem should get smaller without behavior change
- Reviewing structure: scattered modules, bloated facades, config surfaces that grew by accretion

## Do not use this skill when

- Behavior is supposed to change — that's a feature or fix, and mixing it with consolidation makes both unverifiable
- Reviewing a single fresh diff — use a diff-scoped review; this skill audits an existing body of code
- The duplication is across repositories with independent release cycles — extracting a shared dependency is an architecture decision for the user, not a refactor

## Map the constraints before proposing anything

Read the project's declared architecture first: layer definitions, import/dependency contracts (whatever tool enforces them — import linters, module-boundary rules, package manifests), and the public API surface (exported symbols, documented entry points, semver promises). These determine *where* consolidated code may live and *what* may not break. A consolidation plan written before this map is guesswork; the gate at the end must re-run whatever enforces these rules.

## The finding passes

### 1. Literal duplication

The same block, byte-identical or near-identical, in several places — engine skeletons, setup stanzas, classify-and-reraise ladders, wiring blocks. Highest-confidence targets: extract once, point all sites at it. Copies found late have usually already begun to diverge; diff them carefully — an *intentional* difference hiding in a near-copy is a finding of its own (which one is right?).

### 2. Same concern, divergent shapes

N implementations of one mechanism, each hand-rolled: three proxy classes each owning their own method-dispatch boilerplate, five call sites each rebuilding the same conditional factory. The consolidation is a shared base or helper with override hooks — the variants keep their behavior, the mechanism exists once. This is where the largest wins hide, because no single site *looks* duplicated.

### 3. Scatter

One responsibility spread across many small modules, or its pieces misplaced across layers. Group by responsibility, not by accident of authorship. Preserve inbound import paths that are widely used (see churn containment).

### 4. Surface bloat

Facades and `__init__`-style export surfaces that grew by accretion. **Census actual external imports before cutting** — the decision between "cull", "tier into namespaces", and "leave it" is an evidence question, not taste. Symbols nobody imports externally get demoted (still reachable via submodules), not deleted.

### 5. Type lies and config smells

Structures whose types misdescribe their rules: a mode/kind discriminator field gating which "optional" fields are actually required (validated in a big post-init blob); ≥3 fields sharing a prefix (a struct wanting to exist); legacy dual fields reconciled by a resolver method duplicated per consumer. Consolidate into tagged unions / nested value objects on the *public construction* surface while keeping internal readers untouched via shims.

### 6. Parallel vocabularies

Two names for one concept across packages, or one name for two concepts. Sometimes the fix is a rename with a two-layer vocabulary (public spec vs internal mechanism); sometimes it's recognizing two things genuinely differ and must *not* be merged.

## Verify every claim before acting

- **Prove a copy is a copy.** Only merge code that changes for the same reason. Coincidental similarity — two blocks that look alike today but serve different owners with different futures — must stay separate; merging it manufactures coupling, the exact cost DRY exists to avoid. This is Sandi Metz's rule — "duplication is far cheaper than the wrong abstraction" — restated by Kent C. Dodds as AHA, *Avoid Hasty Abstractions*: merge on a proven shared reason-to-change, never on visual similarity.
- **Prove dead is dead.** Check every access pattern before deleting: `from`-imports, attribute access (`module.symbol(...)` — a grep for `from..import` alone misreads these as dead), re-export chains, reflection and string-based references, config/serialized references. A "dead shim" that is actually a load-bearing facade is a classic false positive.
- **Count before you conclude.** Importer counts, call-site counts, external-vs-internal usage splits. Numbers decide between shim-and-move, break-and-migrate, and leave-alone.

## Placement rules

- Extracted shared code lives in the **lowest layer that every consumer may legally import** — a neutral leaf with minimal dependencies. Shared vocabulary types (value objects used across packages) belong in the contract/shared layer, exported from one canonical location, not re-exported per consumer.
- **Never invert a layer or create a cycle to dedup.** If two copies can only be unified by violating a boundary, the duplication is the correct current state — record it, and surface the boundary question to the user as an architecture decision, separately from the refactor.
- A helper too small to justify its home is a smell in the other direction: don't mint a new top-level package for two classes when an existing leaf module fits.

## Churn containment

Consolidate internals; keep public surfaces stable.

- Widely-imported paths keep working via re-export shims — moving a helper with hundreds of importers should repoint **zero** of them.
- Public constructors keep their kwargs via aliases, converters, and property shims that dispatch into the new shape; internal readers keep their expressions.
- When call sites must move, repoint them mechanically (scripted, verified by grep audit), and keep the mechanical commit separate from any judgment commit.

## Calibration — when to say no

- **NO ACTION is a first-class verdict.** Record it with the reason: "correctly placed", "load-bearing facade", "justified by the runtime model", "false positive of the analysis tool". A third of honest audit findings die on inspection; that is the audit working.
- **Unwind wrong abstractions already in place.** An extraction serving two masters — flag parameters, mode conditionals, callers each exercising a different slice — is Metz's wrong abstraction, and her exit is the fastest way forward: inline it back into its callers, delete what each doesn't use, and let the honest shape re-emerge, or not. *More* code, same behavior, is sometimes this audit's correct verdict.
- **Don't churn for purity.** Idiomatic bare structures, deliberate re-exports, and small asymmetries that harm nothing stay. The metric is behavior-per-line, not style conformance.
- **Stop at diminishing returns, and say so.** When remaining targets are high-churn/low-density, the honest recommendation is "stop unless X specifically hurts" — an audit that can't end recommends its own busywork.

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
