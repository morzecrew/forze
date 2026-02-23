# Tasks: Postgres Query Builder Refactor

**Input**: Design documents from `/specs/001-postgres-query-builder-refactor/`  
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Unit tests requested in spec and plan; tasks include test-first where applicable.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story (US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `src/forze/`, `tests/` at repository root
- Builder: `src/forze/infra/providers/postgres/builder/filters.py`
- Tests: `tests/unit/infra/providers/postgres/builder/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Confirm existing test layout and tooling before adjustments

- [x] T001 Ensure test directory and init files exist at `tests/unit/infra/providers/postgres/builder/`
- [x] T002 [P] Run existing builder unit tests from repo root (`uv run pytest tests/unit/infra/providers/postgres/builder/ -v`) to capture the current behavior baseline

**Checkpoint**: Baseline tests pass and capture current behavior (pre‑adjustment)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Align documentation and research with new canonical public names before changing code

- [x] T003 Update `specs/001-postgres-query-builder-refactor/contracts/filter-input.md` so the canonical operator table uses `in` and `or` as the public names for membership and disjunction, matching the updated research and plan

**Checkpoint**: Public contract documents `in` and `or` as canonical operator names

---

## Phase 3: User Story 1 – Single canonical operator names (Priority: P1) 🎯 MVP

**Goal**: Builder accepts exactly one canonical **public** name per operator, with `in` and `or` as the public names for membership and disjunction; previously supported aliases remain rejected with clear errors.

**Independent Test**: Unit tests pass when using `in` and `or` as keys and fail for deprecated aliases (e.g. `==`, `ge`, `not in`).

### Tests for User Story 1

- [x] T004 [P] [US1] Update tests in `tests/unit/infra/providers/postgres/builder/test_filters.py` so filter inputs use `in` and `or` as the public canonical operator keys, and baseline expectations still hold
- [x] T005 [P] [US1] Extend alias rejection tests in `tests/unit/infra/providers/postgres/builder/test_filters.py` to cover any remaining deprecated aliases (e.g. `"in_"` as input, if treated as internal only), ensuring they raise `ValidationError`

### Implementation for User Story 1

- [x] T006 [US1] Update `parse_op()` in `src/forze/infra/providers/postgres/builder/filters.py` so it treats `in` and `or` as canonical public names and maps them to the correct internal `Op` members, while keeping a single public name per operator
- [x] T007 [US1] Ensure error messages from `parse_op()` still list the full set of canonical public names (including `in` and `or`) and remain actionable

**Checkpoint**: User Story 1 complete — `in` and `or` are the only accepted public names for membership and disjunction; aliases are rejected with clear errors

---

## Phase 4: User Story 2 – Preserved filter capabilities (Priority: P1)

**Goal**: Combined operators (AND on one field), OR chains, and all operator families behave as before after switching to `in`/`or` as public names.

**Independent Test**: Existing AND/OR and operator-family tests in `test_filters.py` still pass when using `in`/`or` keys.

### Tests for User Story 2

- [x] T008 [P] [US2] Re-run and, if needed, adjust combined-operator and OR-chain tests in `tests/unit/infra/providers/postgres/builder/test_filters.py` to ensure they use `in`/`or` where appropriate and still assert correct SQL/params

### Implementation for User Story 2

- [x] T009 [US2] Fix any regressions in `src/forze/infra/providers/postgres/builder/filters.py` uncovered by T008 so all operator and AND/OR tests pass without changing semantics

**Checkpoint**: User Story 2 complete — full feature parity preserved with `in`/`or` as public names

---

## Phase 5: User Story 3 – Easier to maintain and extend (Priority: P2)

**Goal**: Normalization and compilation logic are structured into focused helpers (and optionally a small builder class) so adding or adjusting operators is straightforward and localized.

**Independent Test**: Adding or changing an operator requires edits in a small, well-defined set of helpers/methods, and tests stay readable and targeted.

- [x] T010 [P] [US3] Extract operator-specific normalization helpers from `_normalize_field_expr` in `src/forze/infra/providers/postgres/builder/filters.py` (e.g. functions for scalar comparisons, membership, null/empty, ltree) to reduce branching in the main function
- [x] T011 [P] [US3] Extract operator-specific compile helpers from `_build_op_filter` in `src/forze/infra/providers/postgres/builder/filters.py` (e.g. functions for scalar comparisons, IN/NOT IN, array operators, ltree operators) so each operator family has a clear implementation point
- [x] T012 [US3] Optionally introduce a small `FilterBuilder` (or similarly named) class in `src/forze/infra/providers/postgres/builder/filters.py` that owns `types`, column, and value handling, with `build_filters()` delegating to it; keep public API and behavior unchanged *(deferred: no class added; `build_filters()` remains the single entry point)*

**Checkpoint**: User Story 3 complete — normalization/compilation structure is clear and localized for future changes

---

## Phase 6: User Story 4 – Clearer usage for API consumers (Priority: P2)

**Goal**: Public contract and documentation use `in` and `or` as canonical operator names, and error messages and examples match the contract.

**Independent Test**: Contract docs and quickstart examples list exactly one name per operator and match runtime behavior.

- [x] T013 [US4] Ensure all filter-related error messages in `src/forze/infra/providers/postgres/builder/filters.py` and `tests/unit/infra/providers/postgres/builder/test_filters.py` reference canonical public names where applicable (including `in` and `or`)
- [x] T014 [P] [US4] Update examples in `specs/001-postgres-query-builder-refactor/quickstart.md` and `specs/001-postgres-query-builder-refactor/contracts/filter-input.md` to use `in` and `or` in filter payloads and ensure operator tables match `parse_op()`

**Checkpoint**: User Story 4 complete — public docs and errors align on `in`/`or` and other canonical names

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, lint, and formatting

- [x] T015 [P] Run quickstart validation: `uv run pytest tests/unit/infra/providers/postgres/builder/ -v` from repo root and fix any failures
- [x] T016 Lint and format `src/forze/infra/providers/postgres/builder/filters.py` and `tests/unit/infra/providers/postgres/builder/test_filters.py` per project rules

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup — aligns docs before code
- **User Stories (Phase 3–6)**: Depend on Foundational completion
  - US1 (Phase 3) first — public canonical names
  - US2 (Phase 4) after US1 — parity verification
  - US3 (Phase 5) and US4 (Phase 6) can proceed after US2 (or in parallel where safe)
- **Polish (Phase 7)**: Depends on all desired user story phases complete

### User Story Dependencies

- **US1 (P1)**: After Foundational — defines canonical public names
- **US2 (P1)**: After US1 — validates no regressions with new names
- **US3 (P2)**: After US2 — refactors layout without changing semantics
- **US4 (P2)**: After US1; can run in parallel with US3 — docs and error messages

### Within Each User Story

- Tests updated/added before or alongside implementation changes
- Behavior and parity re-validated at each checkpoint

### Parallel Opportunities

- T002, T004, T005 can run in parallel with other [P] tasks in their phases
- T008 in parallel with some US3/US4 prep when safe
- T010, T011 can often be split across operators
- T014 [P] and T015 [P] can be run in parallel as documentation and test validation

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup  
2. Complete Phase 2: Foundational (contract doc update)  
3. Complete Phase 3: User Story 1 (canonical public names `in`/`or`)  
4. **STOP and VALIDATE**: Run builder unit tests; confirm `in` and `or` work and aliases are rejected  
5. Merge or proceed to US2

### Incremental Delivery

1. Setup + Foundational → baseline tests and docs aligned  
2. US1 → public canonical names in code and tests  
3. US2 → full operator and AND/OR parity with new names  
4. US3 + US4 → maintainability and documentation alignment  
5. Polish → quickstart validation and lint

### Parallel Team Strategy

- One developer: Phases 1 → 2 → 3 → 4 → 5 → 6 → 7 in order  
- After US2: one can drive US3 (refactor layout) while another drives US4 (docs and errors)

---

## Notes

- [P] = parallelizable; [USn] = maps to user story for traceability  
- Each user story has an independent test criterion and checkpoint  
- Commit after each task or logical group  
- File paths are absolute in descriptions where needed; repo root = forze
