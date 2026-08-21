# The six finding passes, and where consolidated code goes

What each pass looks for, with the shape that gives it away, plus the placement
and churn rules that decide where a consolidation lands. `SKILL.md` carries the
verification rule and the calibration that says when not to act.

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

## Placement rules

- Extracted shared code lives in the **lowest layer that every consumer may legally import** — a neutral leaf with minimal dependencies. Shared vocabulary types (value objects used across packages) belong in the contract/shared layer, exported from one canonical location, not re-exported per consumer.
- **Never invert a layer or create a cycle to dedup.** If two copies can only be unified by violating a boundary, the duplication is the correct current state — record it, and surface the boundary question to the user as an architecture decision, separately from the refactor.
- A helper too small to justify its home is a smell in the other direction: don't mint a new top-level package for two classes when an existing leaf module fits.

Duplication **across repositories with independent release cycles** is out of
scope. Extracting a shared dependency there couples two release trains, which is
an architecture decision for the user to take in an RFC, not a refactor to
perform inside one.

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
