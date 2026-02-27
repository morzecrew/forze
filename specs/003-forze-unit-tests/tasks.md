# Tasks: Comprehensive Unit Tests for Forze Package

**Input**: Design documents from `/specs/003-forze-unit-tests/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: This feature IS unit test implementation; all tasks deliver or validate tests.

**Organization**: Tasks grouped by user story to enable independent implementation and validation.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- Tests mirror src: `src/forze/application/foo.py` → `tests/unit/test_forze/application/test_foo.py`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Test infrastructure and directory structure

- [x] T001 Create tests/unit/stubs/ directory with __init__.py in tests/unit/stubs/__init__.py
- [x] T002 [P] Create test package tests/unit/test_forze_postgres/ with __init__.py and conftest.py (pytest.importorskip for psycopg)
- [x] T003 [P] Create test package tests/unit/test_forze_redis/ with __init__.py and conftest.py (pytest.importorskip for redis)
- [x] T004 [P] Create test package tests/unit/test_forze_s3/ with __init__.py and conftest.py (pytest.importorskip for aioboto3)
- [x] T005 [P] Create test package tests/unit/test_forze_mongo/ with __init__.py and conftest.py (pytest.importorskip for pymongo)
- [x] T006 [P] Create test package tests/unit/test_forze_temporal/ with __init__.py and conftest.py (pytest.importorskip for temporalio; exclude WorkflowPort)
- [x] T007 Create tests/unit/test_forze/application/ directory structure mirroring src/forze/application/ (execution, usecases, facades, composition, dsl)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Stub implementations and Deps/ExecutionContext tests. No user story work can begin until this phase is complete.

- [x] T008 Implement InMemoryDocumentPort stub in tests/unit/stubs/document.py (conform to DocumentPort; in-memory dict; exclude StreamPort/WorkflowPort)
- [x] T009 Implement InMemoryStoragePort stub in tests/unit/stubs/storage.py (conform to StoragePort; in-memory dict keyed by storage key)
- [x] T010 Implement InMemoryTxManagerPort stub in tests/unit/stubs/tx.py (conform to TxManagerPort; no-op async context manager)
- [x] T011 Implement InMemoryCounterPort stub in tests/unit/stubs/counter.py (conform to CounterPort; in-memory int)
- [x] T012 Implement InMemoryDocumentCachePort stub in tests/unit/stubs/cache.py (conform to DocumentCachePort; in-memory dict)
- [x] T013 Add unit tests for Deps (register, provide, exists, merge, without) in tests/unit/test_forze/application/contracts/test_deps.py
- [x] T014 Add unit tests for ExecutionContext.dep() resolution in tests/unit/test_forze/application/execution/test_context.py

**Checkpoint**: Stubs and Deps tests pass; `uv run pytest tests/unit/test_forze/application/contracts/ tests/unit/test_forze/application/execution/ tests/unit/stubs/ -v`

---

## Phase 3: User Story 1 - Safe refactoring across all packages (Priority: P1) 🎯 MVP

**Goal**: Tests for all packages so maintainers get immediate feedback when behavior changes.

**Independent Test**: Run `uv run pytest tests/unit -v`; all pass. Introduce a deliberate behavior change in any covered module; at least one test fails and identifies the affected behavior.

### Implementation for User Story 1

- [x] T015 [P] [US1] Add unit tests for forze.application.execution (context, usecase, plan, registry, resolvers) in tests/unit/test_forze/application/execution/
- [x] T016 [P] [US1] Add unit tests for forze.application.usecases.document in tests/unit/test_forze/application/usecases/test_document.py (use stubs)
- [x] T017 [P] [US1] Add unit tests for forze.application.usecases.storage in tests/unit/test_forze/application/usecases/test_storage.py (use stubs)
- [x] T018 [P] [US1] Add unit tests for forze.application.facades in tests/unit/test_forze/application/facades/
- [x] T019 [P] [US1] Add unit tests for forze.application.composition in tests/unit/test_forze/application/composition/
- [x] T020 [P] [US1] Add unit tests for forze.application.dsl.query in tests/unit/test_forze/application/dsl/test_query.py
- [x] T021 [P] [US1] Add unit tests for forze.utils.codecs in tests/unit/test_forze/utils/test_codecs.py
- [ ] T022 [P] [US1] Add unit tests for forze_fastapi (routing, forms, params, routers) in tests/unit/test_forze_fastapi/ (use stubs for DocumentPort, StoragePort)
- [x] T023 [P] [US1] Add unit tests for forze_postgres kernel (query render, gateways) and adapters in tests/unit/test_forze_postgres/ (mock PostgresClient)
- [ ] T024 [P] [US1] Add unit tests for forze_redis adapters (cache, counter, idempotency; exclude stream) in tests/unit/test_forze_redis/ (mock Redis client)
- [ ] T025 [P] [US1] Add unit tests for forze_s3 adapters and kernel in tests/unit/test_forze_s3/ (mock S3 client)
- [ ] T026 [P] [US1] Add unit tests for forze_mongo adapters and kernel in tests/unit/test_forze_mongo/ (mock Mongo client)
- [x] T027 [US1] Add conftest fixtures for shared stub-based Deps/ExecutionContext in tests/unit/test_forze/application/conftest.py

**Checkpoint**: `uv run pytest tests/unit -v` passes; introduce a behavior change and verify a test fails.

---

## Phase 4: User Story 2 - Coverage for all public-facing modules (Priority: P2)

**Goal**: Every public module has at least one unit test exercising its primary behavior.

**Independent Test**: `uv run pytest tests/unit --cov=src --cov-report=term-missing`; each public module has at least one test.

- [ ] T028 [US2] Run coverage report and document any uncovered public modules in specs/003-forze-unit-tests/coverage-gaps.md
- [ ] T029 [US2] Add unit tests for uncovered modules identified in T028 (exclude thin wrappers, compatibility shims, StreamPort, WorkflowPort)

**Checkpoint**: Coverage report shows all public modules have at least one test.

---

## Phase 5: User Story 3 - Fast, isolated feedback (Priority: P3)

**Goal**: Scoped runs complete in under 30s; no external services required.

**Independent Test**: `uv run pytest tests/unit/test_forze -v` completes in under 30s; `uv run pytest tests/unit -v` completes in under 5 min.

- [ ] T030 [US3] Ensure optional-deps tests use pytest.importorskip at module or test level in tests/unit/test_forze_postgres/, test_forze_redis/, test_forze_s3/, test_forze_mongo/, test_forze_temporal/
- [ ] T031 [US3] Verify scoped run timing: `uv run pytest tests/unit/test_forze -v` completes in under 30s; document in quickstart.md if needed
- [ ] T032 [US3] Verify full suite timing: `uv run pytest tests/unit -v` completes in under 5 min; document in quickstart.md if needed

**Checkpoint**: Scoped and full runs meet timing requirements.

---

## Phase 6: User Story 4 - Documentation through tests (Priority: P4)

**Goal**: Tests are readable; contributors can infer key behaviors and edge cases.

**Independent Test**: A contributor reads unit tests for a module and infers key behaviors without extensive prior context.

- [ ] T033 [P] [US4] Add module-level docstrings to stub implementations in tests/unit/stubs/*.py
- [ ] T034 [US4] Review test class and method names for clarity (TestX, test_<behavior>) per .agent/rules/pytest-style.mdc

**Checkpoint**: Tests are self-documenting.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and bug reporting.

- [ ] T035 [P] Update specs/003-forze-unit-tests/quickstart.md with any environment notes discovered during implementation
- [ ] T036 Run full quickstart validation: `uv run pytest tests/unit -v` from repo root; all pass
- [ ] T037 Create specs/003-forze-unit-tests/BUGS.md if any bugs were found during implementation; report only, do not fix (per user directive)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies – can start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 – BLOCKS all user story work
- **Phase 3 (US1)**: Depends on Phase 2 – MVP; safe refactoring
- **Phase 4 (US2)**: Depends on Phase 3 – coverage validation
- **Phase 5 (US3)**: Depends on Phase 3 – timing and isolation
- **Phase 6 (US4)**: Depends on Phase 3 – documentation quality
- **Phase 7 (Polish)**: Depends on Phases 3–6

### User Story Dependencies

- **US1 (P1)**: After Foundational – core implementation; all package tests
- **US2 (P2)**: After US1 – coverage gap-filling
- **US3 (P3)**: After US1 – optional deps, timing (can overlap with US2)
- **US4 (P4)**: After US1 – readability (can overlap with US2, US3)

### Parallel Opportunities

- T002–T006: Package setup can run in parallel
- T015–T026: Package test implementation can run in parallel (different packages)
- T033: Stub docstrings can run in parallel with T034

---

## Parallel Example: User Story 1

```bash
# Launch package test implementations in parallel:
Task: "Add unit tests for forze_postgres in tests/unit/test_forze_postgres/"
Task: "Add unit tests for forze_redis in tests/unit/test_forze_redis/"
Task: "Add unit tests for forze_s3 in tests/unit/test_forze_s3/"
Task: "Add unit tests for forze_mongo in tests/unit/test_forze_mongo/"
Task: "Add unit tests for forze_fastapi in tests/unit/test_forze_fastapi/"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (stubs + Deps tests)
3. Complete Phase 3: User Story 1 (all package tests)
4. **STOP and VALIDATE**: `uv run pytest tests/unit -v`; introduce behavior change, verify test fails
5. Proceed to US2/US3/US4 or deploy

### Incremental Delivery

1. Setup + Foundational → Stubs and Deps ready
2. US1 (forze.application first) → Test independently
3. US1 (integration packages) → Test each package independently
4. US2 → Coverage validation
5. US3 → Timing validation
6. US4 → Documentation review

---

## Notes

- **Bug policy**: If bugs are found, report in BUGS.md; do not fix in-place (per user directive).
- **Exclusions**: Do NOT test StreamPort or WorkflowPort.
- **Stubs**: Use in-memory implementations; conform to Protocol interfaces in contracts/stub-ports-schema.yaml.
- **Deps**: All usecase/facade tests must exercise Deps and ExecutionContext with stubbed ports.
