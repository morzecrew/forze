---
name: forze-testing
description: Write and run tests for Forze framework code following project conventions. Use when creating unit tests, integration tests, or understanding test patterns.
---

# Forze Testing

## Test Structure

```text
tests/
├── unit/                  # no external I/O
│   └── test_<pkg>/        # mirrors src/<pkg>/
│       └── test_<module>.py
├── integration/           # requires Docker (testcontainers)
│   └── test_<pkg>/
└── perf/                  # performance benchmarks (Docker)
```

Mirror the `src/` layout:

```text
src/forze/domain/models/document.py  →  tests/unit/test_forze/domain/models/test_document.py
src/forze_postgres/adapters/document.py  →  tests/unit/test_forze_postgres/adapters/test_document.py
```

If filenames collide across packages, use prefixes:

```text
src/forze/foo/bar.py  →  tests/unit/test_forze/foo/test_bar.py
src/forze/baz/bar.py  →  tests/unit/test_forze/baz/test_baz_bar.py
```

## Naming Conventions

- Test files: `test_*.py`
- Test classes: `Test*` (one per tested type)
- Test functions: `test_*`

## Running Tests

```bash
# All tests (excluding perf)
just test-fast

# Unit tests only
just test-fast tests/unit

# Specific test file
just test-fast tests/unit/test_forze/domain/models/test_document.py

# Specific test class
just test-fast tests/unit/test_forze/domain/models/test_document.py::TestDocumentUpdate

# Integration tests (requires Docker)
just test-fast tests/integration

# Performance benchmarks (requires Docker)
just test-perf
```

## Quality Checks

```bash
# All quality checks (lint, types, imports, dead code, deps, security)
just quality

# Strict mode (fail on any issue)
just quality -s
```

## Unit Tests

### Rules

- No external I/O (no network, no disk, no database)
- Use mocks for port dependencies
- Prefer `MagicMock(spec=RealClass)` for type-safe mocks
- One `TestX` class per tested type

### Domain Model Tests

```python
import pytest
from forze.base.errors import ValidationError
from my_module import MyDocument

class TestMyDocument:
    def test_default_fields(self) -> None:
        doc = MyDocument(name="test")
        assert isinstance(doc.id, UUID)
        assert doc.rev == 1

    def test_update_bumps_revision(self) -> None:
        doc = MyDocument(name="old")
        updated, diff = doc.update({"name": "new"})
        assert updated.name == "new"
        assert updated.rev == 2
        assert "name" in diff

    def test_update_validator_rejects_invalid(self) -> None:
        doc = MyDocument(name="test", status="archived")
        with pytest.raises(ValidationError):
            doc.update({"name": "changed"})
```

### Usecase Tests

```python
import pytest
from unittest.mock import MagicMock, AsyncMock

class TestGetDocument:
    @pytest.mark.asyncio
    async def test_returns_document(self) -> None:
        doc_port = MagicMock(spec=DocumentReadPort)
        doc_port.get = AsyncMock(return_value=expected_doc)

        ctx = MagicMock(spec=ExecutionContext)
        uc = GetDocument(ctx=ctx, doc=doc_port)

        result = await uc.main(doc_id)
        assert result == expected_doc
        doc_port.get.assert_called_once_with(doc_id)
```

### Middleware Tests

```python
class TestMyGuard:
    @pytest.mark.asyncio
    async def test_raises_on_unauthorized(self) -> None:
        guard = MyGuard(...)
        with pytest.raises(CoreError):
            await guard(args)

    @pytest.mark.asyncio
    async def test_passes_on_authorized(self) -> None:
        guard = MyGuard(...)
        await guard(args)  # should not raise
```

### Execution Runtime Tests

```python
class TestExecutionRuntime:
    @pytest.mark.asyncio
    async def test_scope_runs_lifecycle(self) -> None:
        order: list[str] = []
        step = LifecycleStep(
            name="test",
            startup=lambda ctx: order.append("start"),
            shutdown=lambda ctx: order.append("stop"),
        )
        rt = ExecutionRuntime(
            lifecycle=LifecyclePlan.from_steps(step),
        )
        async with rt.scope():
            assert order == ["start"]
        assert order == ["start", "stop"]
```

## Integration Tests

### Rules

- Use `testcontainers` for external services
- Fixtures defined in `tests/integration/conftest.py`
- One scenario per test
- Ensure test data isolation

### Available Testcontainers

| Service | Container |
|---------|-----------|
| PostgreSQL | `PostgresContainer` |
| Redis/Valkey | `ValKeyContainer` |
| MinIO (S3) | `MinioContainer` |
| MongoDB | `MongoDbContainer` |
| RabbitMQ | `RabbitmqContainer` |
| LocalStack (SQS) | `LocalStackContainer` |

### Integration Test Example

```python
import pytest

@pytest.mark.asyncio
@pytest.mark.integration
async def test_create_and_read(postgres_adapter) -> None:
    created = await postgres_adapter.create(CreateMyCmd(name="test"))
    assert created.name == "test"

    fetched = await postgres_adapter.get(created.id)
    assert fetched.id == created.id
```

## Pytest Markers

Markers must be registered in `pyproject.toml`:

| Marker | Purpose |
|--------|---------|
| `unit` | Unit tests |
| `integration` | Integration tests |
| `perf` | Performance benchmarks |
| `asyncio` | Async tests (via `pytest-asyncio`) |

## Async Tests

Use `@pytest.mark.asyncio` for async test functions:

```python
@pytest.mark.asyncio
async def test_async_operation() -> None:
    result = await some_async_function()
    assert result is not None
```

## Mock Adapters for Testing

The `forze_mock` package provides in-memory adapters:

```python
from forze_mock.adapters import MockDocumentAdapter, MockState

state = MockState()
adapter = MockDocumentAdapter(
    state=state,
    namespace="test_entity",
    read_model=MyReadDocument,
)

# Use in tests as a real adapter
created = await adapter.create(cmd)
fetched = await adapter.get(created.id)
```

`MockState` is shared across adapters for consistent in-memory state.

## Checklist

When writing tests:

1. Choose test type: unit (no I/O) or integration (with services)
2. Mirror the `src/` path structure in `tests/unit/test_<pkg>/`
3. One `TestX` class per type being tested
4. Use `MagicMock(spec=...)` for typed mocks in unit tests
5. Use `@pytest.mark.asyncio` for async tests
6. Run `just test-fast` to verify, `just quality` for full checks
7. Register new markers in `pyproject.toml` if needed
