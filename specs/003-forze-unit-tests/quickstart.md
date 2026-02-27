# Quickstart: Comprehensive Unit Tests for Forze Package

**Branch**: `003-forze-unit-tests`

## Running the tests

From the repository root:

```bash
# Full unit suite
uv run pytest tests/unit -v

# Or via just
just test tests/unit
```

### Scoped runs (single package)

```bash
# Core forze (base, domain, application)
uv run pytest tests/unit/test_forze -v

# FastAPI integration
uv run pytest tests/unit/test_forze_fastapi -v

# PostgreSQL integration (requires postgres optional dep)
uv run pytest tests/unit/test_forze_postgres -v

# Redis integration (requires redis optional dep)
uv run pytest tests/unit/test_forze_redis -v

# S3 integration (requires s3 optional dep)
uv run pytest tests/unit/test_forze_s3 -v

# Mongo integration (requires mongo optional dep)
uv run pytest tests/unit/test_forze_mongo -v

# Temporal integration (requires temporal optional dep; excludes WorkflowPort)
uv run pytest tests/unit/test_forze_temporal -v
```

### Coverage report

```bash
uv run pytest tests/unit --cov=src --cov-report=term-missing
```

## What this validates

- **forze.base**: Primitives, errors, serialization (existing tests).
- **forze.domain**: Models, mixins, validation (existing tests).
- **forze.application**: Execution context, Deps, usecases, facades, composition (with stub ports).
- **forze_fastapi**: Routing, forms, params, routers (with stubbed deps).
- **forze_postgres**: Query render, gateways, adapters (with mocked client).
- **forze_redis**: Cache, counter, idempotency adapters (with mocked client; stream excluded).
- **forze_s3**: Storage adapter (with mocked client).
- **forze_mongo**: Document adapter, query render (with mocked client).
- **forze_temporal**: (Workflow adapter excluded; other logic if any).

## Stub implementations

Stubs live in `tests/unit/stubs/` and provide in-memory implementations of:

- DocumentPort, DocumentCachePort
- StoragePort
- TxManagerPort
- CounterPort
- (Other ports as needed; StreamPort and WorkflowPort excluded)

Use them in tests via:

```python
from tests.unit.stubs import InMemoryDocumentPort, InMemoryStoragePort

deps = Deps().register(DocumentDepKey, InMemoryDocumentPort(...))
ctx = ExecutionContext(deps=deps, ...)
```

## Environment notes

- **Python path**: `src` is on `pythonpath` (pyproject.toml).
- **Optional deps**: Install with `uv sync --group dev` and optional groups, e.g. `uv sync --extra postgres --extra redis` for full coverage.
- **No external services**: Unit tests require no databases, caches, or object storage.
