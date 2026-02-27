# Implementation Plan: Comprehensive Unit Tests for Forze Package

**Branch**: `003-forze-unit-tests` | **Date**: 2025-02-27 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/003-forze-unit-tests/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Provide comprehensive unit tests for all public modules across the forze monorepo (forze core, forze_fastapi, forze_postgres, forze_redis, forze_s3, forze_mongo, forze_temporal). Tests must run without external services, use pytest with in-memory/stub port implementations, and exclude stream and workflow ports per user directive. The Deps container and dependency resolution must be exercised; any bugs discovered are reported, not fixed in-place.

### User Constraints (from /speckit.plan input)

- **pytest**: Use pytest exclusively for the test suite.
- **Stub implementations**: Create in-memory (or similar) stub implementations for ports; do not rely solely on mocks.
- **Exclusions**: Do NOT test StreamPort or WorkflowPort.
- **Deps container**: Pay attention to dependencies; ensure Deps and ExecutionContext are properly exercised.
- **Bug policy**: If bugs are found, report them; do not fix in-place.

## Technical Context

**Language/Version**: Python 3.13+ (pyproject: `>=3.13,<3.15`)  
**Primary Dependencies**: attrs, pydantic, orjson, pyyaml; optional: fastapi, psycopg, redis, temporalio, aioboto3, pymongo  
**Storage**: N/A for unit tests (stubs/fakes only); integration packages use PostgreSQL, Redis, S3, Mongo, Temporal  
**Testing**: pytest 9.x, pytest-cov, pytest-mock, pytest-xdist; existing layout under `tests/unit/`  
**Target Platform**: Linux (typical dev hardware)  
**Project Type**: Single monorepo with multiple packages under `src/`  
**Performance Goals**: Full suite < 5 min; single package < 30 s (FR-005, US3)  
**Constraints**: No external I/O; deterministic; scoped by package/module  
**Scale/Scope**: 7 packages, ~100+ source modules; existing base/domain tests retained

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Verify alignment with `.specify/memory/constitution.md`:

- **I. Code Quality**: Readable code, documented contracts, boundaries encapsulated, style enforced. ✅
- **II. Testing Standards**: Behavior-focused tests, deterministic and isolated, regression tests for fixes. ✅ (plan aligns)
- **III. User Experience Consistency**: Consistent patterns, clear feedback, actionable errors (if user-facing). N/A (no user-facing changes).
- **IV. Performance Requirements**: Measured optimization, latency/throughput expectations, graceful degradation. ✅ (FR-005, US3)

Document any justified exception in Complexity Tracking.

### Post-Phase 1 Re-check

- **I. Code Quality**: Stub contracts documented in `contracts/`; test layout follows `.agent/rules/pytest-style.mdc`. ✅
- **II. Testing Standards**: research.md, data-model.md, quickstart.md define behavior-focused, isolated tests; stubs enable no-I/O runs. ✅
- **III. User Experience Consistency**: N/A.
- **IV. Performance Requirements**: quickstart.md documents scoped runs (< 30 s per package) and full suite (< 5 min). ✅

## Project Structure

### Documentation (this feature)

```text
specs/003-forze-unit-tests/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

**Later**: `/speckit.approve` (input: spec name) to verify acceptance criteria and DoD; `/speckit.publish` to get strategy (A/B/C) and exact commands—no push/merge until you confirm.

### Source Code (repository root)

```text
src/
├── forze/                    # Core package
│   ├── application/          # Composition, execution, facades, usecases, contracts
│   ├── domain/               # Models, mixins, validation
│   ├── base/                 # Primitives, errors, serialization
│   └── utils/
├── forze_fastapi/            # FastAPI integration
├── forze_postgres/           # PostgreSQL integration
├── forze_redis/              # Redis integration
├── forze_s3/                 # S3 integration
├── forze_mongo/              # MongoDB integration
└── forze_temporal/           # Temporal workflow integration

tests/
├── unit/
│   ├── test_forze/           # base, domain (existing)
│   ├── test_forze_fastapi/   # (existing)
│   ├── test_forze_postgres/  # (to add)
│   ├── test_forze_redis/     # (to add)
│   ├── test_forze_s3/        # (to add)
│   ├── test_forze_mongo/     # (to add)
│   ├── test_forze_temporal/   # (to add, excluding workflow port)
│   └── stubs/                # In-memory port implementations
└── integration/
```

**Structure Decision**: Single project with `src/` containing multiple packages. Tests mirror `src` under `tests/unit/test_<package>/`. Stub implementations live in `tests/unit/stubs/` for shared in-memory ports.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none) | — | — |
