# Forze test suite

## Layout

```text
tests/
  unit/           # No external I/O; mocks and pure logic (CI default)
  integration/    # Testcontainers / real services (Docker required; CI default)
  perf/           # Performance benchmarks (`-m perf`; excluded from default `just test`)
  support/        # Shared fixtures, factories, scenarios
```

**CI default** (`just test`): `unit` + `integration` only (`-m "not perf"`).

**Perf tier** (`just perf`): throughput/latency benchmarks. Most modules under `tests/perf/` use
Docker testcontainers; some (e.g. codec decode) are in-process only. Perf is about measuring
overall performance, not requiring Docker for every benchmark.

Mirror `src/` under `tests/unit/` when possible (see [CONTRIBUTING.md](../CONTRIBUTING.md)).

## Test tiers

| Tier | Location | Purpose |
|------|----------|---------|
| L0 | `tests/unit/` pure modules | Renderers, scrubbing, cursors, error mapping |
| L1 | `tests/unit/` + `forze_mock` | Lifecycle hooks, `ExecutionContext`, routed client wiring |
| L2 | `tests/integration/test_forze_*/` | One backend, smoke + adapter paths |
| L3 | Cross-package integration | e.g. Postgres + Redis snapshots, authn + authz + PG |
| Perf | `tests/perf/` | Benchmarks; optional Docker per package conftest |

## Running subsets

```bash
just test tests/unit
just test tests/integration
just test -m integration
just test -m "not perf"
just test tests/unit/test_forze_redis/test_redis_lifecycle.py
just perf                              # all @pytest.mark.perf benchmarks
just perf tests/perf/test_forze_codec_perf.py   # pydantic + msgspec tiers
just perf tests/perf/test_forze_codec_perf.py -k pydantic_strict
just perf tests/perf/test_forze_codec_perf.py -k msgspec
just perf tests/perf/test_forze_codec_perf.py -k "simple"
just perf tests/perf/test_forze_codec_perf.py --benchmark-compare
```

Integration tests use shared Docker checks from [`integration/conftest.py`](integration/conftest.py) and [`support/docker.py`](support/docker.py). Perf subpackages that start containers use the same pattern in their local `conftest.py`; perf tests without a container fixture run without Docker.

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
