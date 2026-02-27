# Test Coverage Contract

**Branch**: `003-forze-unit-tests`

## Requirement

Every public module in the forze project MUST have at least one unit test that exercises its primary behavior (FR-001, FR-002, SC-002).

## Scope

| Package | Modules | Exclusions |
|---------|---------|------------|
| forze | application, domain, base, utils | Private `_*` modules; compatibility shims |
| forze_fastapi | routers, routing, constants | — |
| forze_postgres | adapters, kernel, dependencies | — |
| forze_redis | adapters, kernel, dependencies | Stream adapter (StreamPort excluded) |
| forze_s3 | adapters, kernel, dependencies | — |
| forze_mongo | adapters, kernel, dependencies | — |
| forze_temporal | — | Workflow adapter (WorkflowPort excluded) |

## Stub Conformance

Stubs in `tests/unit/stubs/` MUST:
1. Implement all methods of the target Protocol.
2. Perform no real I/O (network, filesystem, database).
3. Use in-memory storage (dict, list) for state.
4. Be usable with `Deps.register(key, stub)` and `ExecutionContext.dep(key)`.

## Deps Container

Tests that exercise usecases, facades, or composition MUST:
1. Build a `Deps` instance with stub ports registered under the appropriate `DepKey`s.
2. Create `ExecutionContext` with that `Deps`.
3. Verify `dep(key)` resolves to the stub.

## Optional Dependencies

Tests for packages with optional deps (postgres, redis, s3, mongo, temporal) MUST be skippable when the optional dep is not installed, e.g.:

```python
pytest.importorskip("psycopg")
# or
pytest.mark.skipif(not HAS_POSTGRES, reason="postgres optional dep not installed")
```
