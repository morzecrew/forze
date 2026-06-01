# Forze test suite

## Layout

```text
tests/
  unit/           # No external I/O; mocks and pure logic
  integration/    # Testcontainers / real services (Docker required)
  perf/           # Benchmarks (`-m perf`, excluded from default `just test`)
  support/        # Shared fixtures, factories, scenarios
```

Mirror `src/` under `tests/unit/` when possible (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

## Test tiers

| Tier | Location | Purpose |
|------|----------|---------|
| L0 | `tests/unit/` pure modules | Renderers, scrubbing, cursors, error mapping |
| L1 | `tests/unit/` + `forze_mock` | Lifecycle hooks, `ExecutionContext`, routed client wiring |
| L2 | `tests/integration/test_forze_*/` | One backend, smoke + adapter paths |
| L3 | Cross-package integration | e.g. Postgres + Redis snapshots, authn + authz + PG |

## Running subsets

```bash
just test tests/unit
just test tests/integration
just test -m integration
just test -m "not perf"
just test tests/unit/test_forze_redis/test_redis_lifecycle.py
```

Integration tests use shared Docker checks from [`integration/conftest.py`](integration/conftest.py) and [`support/docker.py`](support/docker.py).

## Integration smoke matrix (per package)

Each `forze_*` integration should cover:

1. Client lifecycle (startup / health / shutdown)
2. Routed client (tenant secrets, LRU eviction where applicable)
3. One `ExecutionContext` document or queue golden path
4. One search or analytics path (if supported)
5. One failure path (not found, validation, infra error)

Contract coverage tables in [integration docs](../pages/docs/integrations/) map features to ports; add `# covers: Port.method` in the matching test module when extending coverage.

## Shared support

- [`support/execution_context.py`](support/execution_context.py) — build `ExecutionContext` from `Deps`
- [`support/factories.py`](support/factories.py) — Polyfactory document models for integration tests
- [`support/secrets_fixtures.py`](support/secrets_fixtures.py) — in-memory secrets for routed-client tests
- [`support/scenarios/`](support/scenarios/) — cross-backend document/query scenarios
