# Data Model: Unit Test Artifacts

**Branch**: `003-forze-unit-tests` | **Phase**: 1

This document describes the conceptual entities for the unit testing feature. These are not domain entities but test infrastructure and coverage artifacts.

## 1. Test Module

| Field | Type | Description |
|-------|------|--------------|
| package | str | Source package (forze, forze_fastapi, forze_postgres, etc.) |
| path | str | Path under `tests/unit/test_<package>/` mirroring `src/<package>/` |
| scope | str | Package or submodule scope for scoped runs |
| primary_behavior | str | Main behavior exercised by at least one test |

**Relationships**:
- One test module per public source module (or logical grouping).
- Test module may depend on stubs from `tests/unit/stubs/`.

## 2. Stub (In-Memory Port Implementation)

| Field | Type | Description |
|-------|------|--------------|
| port_name | str | Protocol name (e.g., DocumentPort, StoragePort, TxManagerPort) |
| implementation | class | In-memory implementation conforming to Protocol |
| storage | dict/list | In-memory state (e.g., dict for documents, list for objects) |

**Validation**:
- Stub must pass `isinstance(stub, Protocol)` (runtime_checkable).
- Stub must not perform real I/O.

**Excluded stubs** (per user directive):
- StreamPort
- WorkflowPort

## 3. Port (Protocol)

| Field | Type | Description |
|-------|------|--------------|
| name | str | Protocol class name |
| methods | list[str] | Async/sync methods to implement |
| in_scope | bool | True if not StreamPort or WorkflowPort |

## 4. Deps Container Usage

| Operation | Purpose in Tests |
|-----------|------------------|
| `Deps.register(key, stub)` | Register stub for a DepKey |
| `Deps.provide(key)` | Resolve dependency in usecase/facade tests |
| `Deps.merge(*deps)` | Combine module deps for composition tests |
| `Deps.without(key)` | Exclude a dep for isolation tests |
| `ExecutionContext(deps=..., ...)` | Create execution context with stubbed deps |

## 5. Package-to-Test Mapping

| Package | Test Dir | Stubs Used | Optional Dep |
|---------|----------|------------|--------------|
| forze | test_forze | — | — |
| forze_fastapi | test_forze_fastapi | DocumentPort, StoragePort, etc. | fastapi |
| forze_postgres | test_forze_postgres | — (test adapters with mocked client) | postgres |
| forze_redis | test_forze_redis | — (test adapters with mocked client) | redis |
| forze_s3 | test_forze_s3 | — (test adapters with mocked client) | s3 |
| forze_mongo | test_forze_mongo | — (test adapters with mocked client) | mongo |
| forze_temporal | test_forze_temporal | — (exclude WorkflowPort) | temporal |

## 6. State Transitions

- **Stub**: Empty → Populated (after test writes) → Read/Verified.
- **Deps**: Empty → Registered → Provided → (optional) Without.
- **Test**: Pending → Pass | Fail | Skip (when optional dep absent).
