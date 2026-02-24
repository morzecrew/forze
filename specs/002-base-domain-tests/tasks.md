# Tasks: Base & Domain Unit Tests

**Input**: Design documents from `/specs/002-base-domain-tests/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

**Organization**: Tasks are grouped by user story (US1 = base layer tests, US2 = domain layer tests) so each story can be implemented and validated independently.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1 = base unit tests, US2 = domain unit tests
- Include exact file paths in descriptions

## Path Conventions

- Source: `src/forze/base/`, `src/forze/domain/`
- Tests: `tests/unit/base/`, `tests/unit/domain/` (mirror package layout where useful)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Test package layout and discovery so base/domain tests can run in isolation.

- [X] T001 Create test package structure: add `tests/unit/base/__init__.py` and `tests/unit/domain/__init__.py` so pytest discovers tests under `tests/unit/base` and `tests/unit/domain`
- [X] T002 [P] Add `tests/unit/base/primitives/__init__.py` and `tests/unit/base/serialization/__init__.py` to mirror `forze.base` layout
- [X] T003 [P] Add `tests/unit/domain/models/__init__.py`, `tests/unit/domain/validation/__init__.py`, and `tests/unit/domain/mixins/__init__.py` to mirror `forze.domain` layout

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: One shared prerequisite so all later tests run reliably.

- [X] T004 Add a minimal conftest or pytest hook in `tests/unit/conftest.py` or `tests/conftest.py` if the project needs a single place for base/domain test fixtures (e.g., importing forze). If no shared config is needed, document that in `specs/002-base-domain-tests/quickstart.md` and mark task done.

**Checkpoint**: Foundation ready — base and domain test implementation can proceed.

---

## Phase 3: User Story 1 – Base Layer Unit Tests (Priority: P1) – MVP

**Goal**: Unit tests for `forze.base` so changes to primitives, serialization, and errors are guarded by fast, deterministic tests.

**Independent Test**: Run `pytest tests/unit/base` and see all base-layer tests pass; break a behavior in `forze.base` and see at least one test fail.

### Implementation for User Story 1

- [X] T005 [P] [US1] Add unit tests for core errors and error handling in `tests/unit/base/test_errors.py` (CoreError hierarchy, ErrorHandler, error_handler decorator, handled decorator behavior)
- [X] T006 [P] [US1] Add unit tests for file helpers in `tests/unit/base/test_files.py` (read_yaml, read_text, iter_file and edge cases e.g. empty file)
- [X] T007 [P] [US1] Add unit tests for datetime primitive in `tests/unit/base/primitives/test_datetime.py` (utcnow returns timezone-aware UTC datetime)
- [X] T008 [P] [US1] Add unit tests for RuntimeVar in `tests/unit/base/primitives/test_runtime.py` (set_once, get, reset; thread-safety and CoreError on invalid use)
- [X] T009 [P] [US1] Add unit tests for string normalization in `tests/unit/base/primitives/test_string.py` (normalize_string: None, empty, Unicode NFC, whitespace collapse, invisible chars)
- [X] T010 [P] [US1] Add unit tests for UUID helpers in `tests/unit/base/primitives/test_uuid.py` (uuid4 with/without value, uuid7 timestamp roundtrip, uuid7_to_datetime, datetime_to_uuid7, edge cases)
- [X] T011 [P] [US1] Add unit tests for dict diff/patch in `tests/unit/base/serialization/test_diff.py` (apply_dict_patch, calculate_dict_difference, deep_dict_intersection; include list and nesting edge cases)
- [X] T012 [P] [US1] Add unit tests for Pydantic helpers in `tests/unit/base/serialization/test_pydantic.py` (pydantic_validate forbid_extra, pydantic_dump exclude options, pydantic_field_names, pydantic_model_hash stability)

**Checkpoint**: `pytest tests/unit/base` passes; base layer behavior is covered by unit tests.

---

## Phase 4: User Story 2 – Domain Layer Unit Tests (Priority: P2)

**Goal**: Unit tests for `forze.domain` so document update semantics, validators, and mixins are guarded by deterministic tests.

**Independent Test**: Run `pytest tests/unit/domain` and see all domain-layer tests pass; change a domain rule and see a test fail.

### Implementation for User Story 2

- [X] T013 [P] [US2] Add unit tests for base models in `tests/unit/domain/models/test_base.py` (CoreModel config, BaseDTO frozen behavior)
- [X] T014 [US2] Add unit tests for Document model in `tests/unit/domain/models/test_document.py` (update diff application, touch, validate_historical_consistency, _validate_update_data frozen-field rejection, last_update_at bump)
- [X] T015 [P] [US2] Add unit tests for update validators in `tests/unit/domain/validation/test_updates.py` (update_validator decorator signatures, collect_update_validators order and on_conflict behavior, UpdateValidatorMetadata fields)
- [X] T016 [P] [US2] Add unit tests for name mixins in `tests/unit/domain/mixins/test_name.py` (NameMixin, NameCreateCmdMixin, NameUpdateCmdMixin and optional name fields)
- [X] T017 [P] [US2] Add unit tests for number mixins in `tests/unit/domain/mixins/test_number.py` (NumberMixin, NumberCreateCmdMixin, NumberUpdateCmdMixin)
- [X] T018 [P] [US2] Add unit tests for soft-deletion mixin in `tests/unit/domain/mixins/test_soft_deletion.py` (SoftDeletionMixin is_deleted, update validator blocking non-soft-delete updates when deleted)

**Checkpoint**: `pytest tests/unit/domain` passes; domain layer behavior is covered by unit tests.

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Validation and docs so the feature is complete and repeatable.

- [ ] T019 Run full base and domain suite and document result: `pytest tests/unit/base tests/unit/domain -v` completes in under 5 seconds on a typical dev machine; note any flaky or slow tests in `specs/002-base-domain-tests/quickstart.md` if needed
- [ ] T020 [P] Update `specs/002-base-domain-tests/quickstart.md` with exact pytest commands and any environment notes (e.g., Python path, optional env vars) so a new contributor can run the same commands

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — create package layout first.
- **Phase 2 (Foundational)**: Depends on Phase 1 — conftest/fixtures if needed.
- **Phase 3 (US1 – Base)**: Depends on Phase 2 — all base tests can be written in parallel (different files).
- **Phase 4 (US2 – Domain)**: Depends on Phase 2; independent of Phase 3 (domain tests do not require base tests to pass, but both are required for the feature).
- **Phase 5 (Polish)**: Depends on Phase 3 and Phase 4 complete.

### User Story Dependencies

- **US1 (Base tests)**: Can start after Phase 2. No dependency on US2.
- **US2 (Domain tests)**: Can start after Phase 2. No dependency on US1.

### Parallel Opportunities

- Phase 1: T002 and T003 can run in parallel after T001.
- Phase 3: T005–T012 are all [P] — different files; can be implemented in parallel.
- Phase 4: T013, T015, T016, T017, T018 are [P]; T014 depends on domain models (no file conflict with others).
- Phase 5: T019 and T020 can be done in parallel after Phase 4.

---

## Parallel Example: User Story 1

```bash
# Example: implement several base test files in parallel
# tests/unit/base/test_errors.py
# tests/unit/base/test_files.py
# tests/unit/base/primitives/test_datetime.py
# tests/unit/base/primitives/test_runtime.py
# tests/unit/base/primitives/test_string.py
# tests/unit/base/primitives/test_uuid.py
# tests/unit/base/serialization/test_diff.py
# tests/unit/base/serialization/test_pydantic.py
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational
3. Complete Phase 3: All base layer unit tests (T005–T012)
4. **STOP and VALIDATE**: Run `pytest tests/unit/base`; confirm deterministic and fast
5. Feature MVP: base layer is covered by unit tests

### Incremental Delivery

1. Setup + Foundational → test layout and discovery ready
2. Add US1 (base tests) → validate with `pytest tests/unit/base` → MVP
3. Add US2 (domain tests) → validate with `pytest tests/unit/domain`
4. Polish → quickstart and timing check

### Parallel Team Strategy

- After Phase 2: one developer can own all of US1 (or split T005–T012 across people by file); another can own US2 (T013–T018) in parallel.

---

## Notes

- [P] = different files, no dependencies on other tasks in same phase
- [US1]/[US2] = task belongs to that user story for traceability
- Each test file should be self-contained; use fixtures or conftest only where it avoids duplication and stays deterministic
- Commit after each task or logical group (e.g., all base primitives)
- Run `pytest tests/unit/base tests/unit/domain` before marking Phase 5 done
