# Research: Comprehensive Unit Tests for Forze Package

**Branch**: `003-forze-unit-tests` | **Phase**: 0

## 1. Testing Framework and Stub Strategy

**Decision**: Use pytest with in-memory stub implementations for ports.

**Rationale**:
- Project already uses pytest (pyproject.toml, justfile, .agent/rules/pytest-style.mdc).
- FR-003 requires tests runnable without external services; stubs satisfy this.
- User explicitly requested stub implementations for ports (in-memory or similar).
- `.agent/rules/pytest-style.mdc` mandates: unit tests use mocks; prefer `MagicMock(spec=RealClass)`; no I/O.

**Alternatives considered**:
- Integration tests with real services: Rejected for unit scope; FR-003 forbids external deps.
- Pure mocks only: Rejected; user asked for stub implementations (in-memory) for ports, which allows testing composition and Deps wiring without real I/O.

---

## 2. Ports to Test vs Exclude

**Decision**: Test all ports except `StreamPort` and `WorkflowPort`.

**Rationale**:
- User directive: "Don't test stream, and workflow ports."
- Ports under `forze/application/contracts/_ports/`:
  - **Include**: actor, counter, document, idempotency, outbox, storage, tenant, tx
  - **Exclude**: stream, workflow

**Alternatives considered**:
- Testing stream/workflow: Rejected per user request.
- Testing only document/storage: Rejected; spec requires coverage for all public modules.

---

## 3. Deps Container and Dependency Injection

**Decision**: Unit tests must exercise `Deps` (register, provide, exists, merge, without) and `ExecutionContext.dep()` with stub ports.

**Rationale**:
- User: "Pay attention to dependencies (deps container)."
- `Deps` is the central DI container; `ExecutionContext` wraps it for usecases.
- Integration packages (postgres, redis, s3, mongo) register dep factories; tests need stubs that conform to `DepPort` interfaces.

**Alternatives considered**:
- Skipping Deps tests: Rejected; user emphasized deps container.
- Using real adapters: Rejected; requires external services.

---

## 4. Stub Implementation Location and Reuse

**Decision**: Create `tests/unit/stubs/` with in-memory implementations of DocumentPort, StoragePort, TxManagerPort, DocumentCachePort, CounterPort, etc., shared across package tests.

**Rationale**:
- Avoid duplicating stubs per package.
- Stubs must conform to Protocol interfaces so `conforms_to` and type checks pass.
- Enables testing usecases, facades, and composition without I/O.

**Alternatives considered**:
- Per-package stubs: Rejected; duplication and inconsistency risk.
- Mock-only: Rejected; user asked for stub implementations.

---

## 5. Optional Dependencies and Skippable Tests

**Decision**: Use `pytest.importorskip` or `pytest.mark.skipif` for modules that require optional deps (postgres, redis, s3, mongo, temporal) when those deps are not installed.

**Rationale**:
- Spec edge case: "Tests for such modules should be skippable or mock-dependent when optional deps are absent."
- pyproject.optional-dependencies: fastapi, postgres, redis, temporal, s3, mongo.

**Alternatives considered**:
- Fail when optional deps missing: Rejected; breaks CI or minimal installs.
- Always install all deps: Rejected; increases install size and complexity.

---

## 6. Bug Reporting Policy

**Decision**: If bugs are found during test implementation, report them (e.g., in plan.md or a BUGS.md in specs) and do not fix in-place.

**Rationale**:
- User: "If you find any bugs - report, don't fix inplace."

**Alternatives considered**:
- Fix bugs as found: Rejected per user directive.
