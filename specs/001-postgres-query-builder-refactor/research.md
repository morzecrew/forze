# Research: Postgres Query Builder Refactor

**Branch**: `001-postgres-query-builder-refactor`  
**Date**: 2025-02-23

## 1. Canonical operator names

**Decision**: Use the existing `Op` enum as the internal source of truth, but treat a small, explicit set of **public operator names** as canonical for input. For most operators the canonical name matches the enum value (e.g. `eq`, `neq`, `gte`, `not_in`, `is_null`, `contains`, `ancestor_of`, `descendant_of`, `match`, `level`). For two high‑frequency operators the public canonical names are:

- Membership: **`in`** (input)  
- Disjunction: **`or`** (input)

Input remains string‑keyed; the builder accepts only the canonical names, mapping `\"in\"`/`\"or\"` to their internal enum variants as needed.

**Rationale**: The enum is still the single source of truth for compilation, but using `\"in\"` and `\"or\"` as public names matches common vocabulary in APIs and config while keeping a one‑to‑one mapping from canonical name to behavior. Snake_case, symbol‑free names remain the default elsewhere and are safe for JSON/API.

**Alternatives considered**: (a) Keep symbols (e.g. `=`, `>=`) as canonical — rejected because they are awkward in JSON keys and vary by locale. (b) Introduce a separate “API name” layer — rejected to avoid redundant alias logic and drift.

---

## 2. Alias deprecation strategy

**Decision**: Hard cut: after refactor, the builder accepts only canonical names. No deprecated alias support in code. Migration path: release notes and changelog list the mapping from old aliases to canonical names; callers and persisted configs must be updated before upgrading.

**Rationale**: Simplest implementation and clearest contract. The spec allows either hard cut or one-release deprecation; a hard cut keeps the codebase free of alias branches and matches FR-001/FR-007. If real-world callers need a transition period, a follow-up could add an optional “compat” mode behind a flag, but it is out of scope for this refactor.

**Alternatives considered**: (b) One release with deprecated aliases and warnings — adds code paths and testing burden; deferred unless required.

---

## 3. String-to-operator parsing

**Decision**: Single function that maps a string key to `Op`: accept only the canonical name (e.g. the enum member name with normalized case). No `_canon_filter_op`-style alias list; invalid keys raise ValidationError with a message that references the canonical operator list (or “expected one of: …”).

**Rationale**: Removes the large alias Literal/union and the many branches in `_canon_filter_op`, satisfying FR-006 and FR-007. One place to add a new operator (enum + this parser + compilation branch).

**Alternatives considered**: (a) Keep alias function and only remove some aliases — still leaves redundant branches. (b) Accept enum values directly from typed callers — possible future option; input from JSON/config will remain string-keyed, so a string parser is still required.

---

## 4. Use of attrs

**Decision**: Use attrs only where it clearly simplifies construction or validation (e.g. a small value type for a filter node). The current filter input is dict-based (JsonDict); keep that as the public input shape. If we introduce an intermediate representation, attrs can be considered for that type; no requirement to change the external API to attrs models.

**Rationale**: User asked for “attrs for classes (simplify construction) where necessary.” The builder’s main interface is `build_filters(filters, types=...)` with dict input; changing that to an attrs-based input would be a larger, speculative change. Internal refactors (e.g. a small `FilterNode` or `OpSpec`) may use attrs if they reduce boilerplate.

**Alternatives considered**: (a) Full attrs-based filter DTOs for input — larger scope and may not simplify callers that build dicts from API/config. (b) No attrs — acceptable; we use attrs only if a clear win appears during implementation.

---

## 5. Testing scope (unit only to start)

**Decision**: Unit tests only for this refactor: test `build_filters` and internal helpers with in-memory inputs and mock or minimal type maps. No database required. Tests assert on composed SQL fragments and parameter lists (behavior), not on private function names. Cover: each canonical operator, AND combination, OR chains, validation errors for invalid keys and bad values.

**Rationale**: User specified “unit tests should be enough to begin with.” Builder is pure (inputs → SQL + params); integration tests with a real DB can be added later if needed (constitution allows test pyramid).

**Alternatives considered**: (a) Add integration tests now — deferred to keep scope minimal. (b) Rely only on existing tests — insufficient; we need explicit coverage for canonical-name-only and for the new structure.
