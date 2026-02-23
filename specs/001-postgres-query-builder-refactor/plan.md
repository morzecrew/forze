# Implementation Plan: Postgres Query Builder Refactor

**Branch**: `001-postgres-query-builder-refactor` | **Date**: 2025-02-23 | **Spec**: [spec.md](./spec.md)  
**Input**: Feature specification from `/specs/001-postgres-query-builder-refactor/spec.md`

## Summary

Refactor the postgres filter query builder so it is easier to maintain, use, and extend: expose a single canonical public name per operator (remove redundant string aliases), preserve all current behaviors (combining operators on a field, OR chains, full operator set), and structure the code so adding a new operator touches a minimal, well-defined set of places. Technical approach: keep the existing `Op` enum as the internal source of truth while defining a clear public vocabulary in which most operator names match the enum values, with explicit exceptions for membership (`in`) and disjunction (`or`); replace the legacy `_canon_filter_op` alias mapping with a single string→operator parser; and improve maintainability by splitting normalization and compilation into focused helpers (and optionally a small builder class) rather than one large function. Cover behavior with unit tests (pytest).

## Technical Context

**Language/Version**: Python 3.13+  
**Primary Dependencies**: psycopg (sql composition), attrs for classes where they simplify construction (e.g. optional value objects); existing forze base (ValidationError, JsonDict, introspect types).  
**Storage**: N/A (builder produces SQL fragments and parameter lists; persistence is caller’s responsibility).  
**Testing**: pytest; unit tests sufficient to begin with (filter build behavior, validation, AND/OR and operator coverage).  
**Target Platform**: Same as forze (Linux/server; Python 3.13+).  
**Project Type**: Single project (library).  
**Performance Goals**: No regression in builder overhead; builder remains in-process and not on hot path for latency.  
**Constraints**: Behavioral parity for all current operator families and combined/OR expressions; public input contract uses only canonical operator names after refactor (including `in` and `or` as the public names for membership and disjunction).  
**Scale/Scope**: Existing callers (gateways/repos) and any config/API that passes filter payloads; scope limited to `src/forze/infra/providers/postgres/builder/` and tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Verify alignment with `.specify/memory/constitution.md`:

- **I. Code Quality**: Readable code, documented contracts, boundaries encapsulated, style enforced.
- **II. Testing Standards**: Behavior-focused tests, deterministic and isolated, regression tests for fixes.
- **III. User Experience Consistency**: Consistent patterns, clear feedback, actionable errors (if user-facing).
- **IV. Performance Requirements**: Measured optimization, latency/throughput expectations, graceful degradation.

No exceptions. Refactor improves code quality and testability; no new external boundaries; validation errors will reference canonical operator names (actionable).

**Post–Phase 1 re-check**: Design aligns with all four principles. Unit-test-only scope and single-module refactor keep boundaries clear. No exceptions.

## Project Structure

### Documentation (this feature)

```text
specs/001-postgres-query-builder-refactor/
├── plan.md              # This file
├── research.md          # Phase 0
├── data-model.md        # Phase 1
├── quickstart.md        # Phase 1
├── contracts/           # Phase 1 (filter input contract)
└── tasks.md             # Phase 2 (/speckit.tasks)
```

### Source Code (repository root)

```text
src/forze/infra/providers/postgres/
├── builder/
│   ├── __init__.py      # build_filters public API
│   ├── filters.py       # refactor target: Op enum, normalize, compile
│   ├── coerce.py        # type coercion (unchanged or minor)
│   └── sorts.py         # placeholder; out of scope unless it shares ops
├── gateways/
│   ├── base.py          # uses build_filters
│   └── ...
├── introspect/
└── platform/

tests/
└── unit/
    └── infra/
        └── providers/
            └── postgres/
                └── builder/   # new or existing unit tests for filters
```

**Structure Decision**: Single project. Builder lives under `src/forze/infra/providers/postgres/builder/`. Unit tests under `tests/unit/...` mirror the package path. No new services or APIs; refactor is local to the builder module and its callers.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations. Table left empty.
