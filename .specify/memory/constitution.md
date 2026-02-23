<!--
Sync Impact Report
==================
Version change: (none) → 1.0.0
Modified principles: N/A (initial creation)
Added sections: Core Principles (4), Quality Gates & Compliance, Development Workflow, Governance
Removed sections: none
Templates: plan-template.md ✅ updated (Constitution Check); spec-template.md ✅ aligned; tasks-template.md ✅ aligned; checklist-template.md — no changes; agent-file-template.md — no changes
Follow-up TODOs: none
-->

# Forze Constitution

## Core Principles

### I. Code Quality

- Code MUST be readable and self-explanatory: clear naming, single responsibility per unit, and explicit control flow. No hidden magic or global mutable state.
- Public APIs (modules, classes, functions) MUST have documented contracts: behavior, inputs, outputs, and failure modes.
- External boundaries (I/O, network, third-party APIs) MUST be encapsulated behind gateways/adapters with explicit error handling, timeouts, and retry policy where applicable.
- Style and formatting MUST follow project lint/format rules; tooling enforces consistency.
- Rationale: Maintainability and safe evolution depend on predictable, reviewable code and clear boundaries.

### II. Testing Standards

- Tests MUST assert observable behavior and contracts, not implementation details, so refactors do not require test rewrites.
- Test pyramid: many fast unit tests, fewer integration tests, minimal end-to-end tests. The default suite MUST complete in time suitable for frequent local runs.
- Tests MUST be deterministic and isolated: no dependence on execution order, shared mutable state, or unreliably external services unless explicitly part of the test.
- For every bug fix: add a failing test that reproduces the bug, then fix; keep the test to prevent regression.
- Rationale: Reliable feedback and regression prevention require stable, fast, behavior-focused tests.

### III. User Experience Consistency

- Similar actions MUST behave and look the same across the product; reuse shared components and patterns.
- Primary actions, state, and consequences MUST be clear; avoid clutter and ambiguous labels.
- Every user action MUST receive timely, visible feedback (loading, success/failure, inline validation).
- Errors MUST be presented in user language with actionable guidance, not raw system messages.
- Rationale: Predictable, understandable interfaces reduce errors and support costs.

### IV. Performance Requirements

- Performance is a product requirement: critical paths MUST be designed with responsiveness and resource use in mind.
- Optimizations MUST be driven by measurement (profiling, metrics, benchmarks), not speculation.
- Key operations MUST have defined latency/throughput expectations; validate in CI where feasible.
- Under load or dependency failure, the system MUST degrade gracefully with clear feedback rather than hanging or crashing.
- Rationale: Users and operators depend on predictable performance and graceful degradation.

## Quality Gates & Compliance

- All changes MUST satisfy the four core principles before merge. Reviews MUST verify:
  - Code quality: readability, contracts, boundary encapsulation, style.
  - Testing: behavior-focused tests, determinism, regression coverage where applicable.
  - UX: consistency, feedback, and error messaging for user-facing changes.
  - Performance: no regressions on critical paths; degradation handling where relevant.
- Exceptions (e.g., temporary technical debt) MUST be documented with a remediation plan and timeline.

## Development Workflow

- Feature work MUST be driven by specs and plans that reference this constitution. The Constitution Check in the implementation plan MUST pass before Phase 0 research and again after Phase 1 design.
- Code review MUST confirm principle compliance. Complexity or principle trade-offs MUST be justified in the plan or in review comments.
- Use project runtime guidance (e.g., README, quickstart, agent/developer docs) for day-to-day implementation; the constitution defines non-negotiable standards.

## Governance

- This constitution overrides conflicting local or ad-hoc practices. All PRs and reviews MUST verify compliance.
- Amendments require: documented proposal, impact on existing principles/sections, and version bump per semantic versioning:
  - **MAJOR**: Backward-incompatible change (principle removal or redefinition).
  - **MINOR**: New principle or section, or material expansion of guidance.
  - **PATCH**: Clarifications, wording, typo fixes, non-semantic refinements.
- Ratification and last-amended dates MUST be kept in ISO format (YYYY-MM-DD). Compliance expectations (quality gates, review) apply from the ratified version onward.

**Version**: 1.0.0 | **Ratified**: 2025-02-23 | **Last Amended**: 2025-02-23
