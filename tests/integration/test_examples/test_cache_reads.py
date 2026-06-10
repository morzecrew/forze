"""Cache-reads recipe end to end on real Postgres + Redis (Docker required).

Proves read-through: a document carrying a ``CacheSpec`` populates Redis on a
cache miss and refreshes it after a write. Mirrors the recipe example at
``examples/recipes/cache_reads/app.py``.
"""

from __future__ import annotations

from uuid import uuid4

from forze.application.execution import DepsRegistry, ExecutionContext
from forze_postgres import PostgresDepsModule
from forze_redis import RedisCacheConfig, RedisDepsModule

from examples.recipes.cache_reads.app import (
    PRODUCT_PG,
    SCHEMA,
    cache_scenario,
)


async def test_cache_reads_read_through(pg_client, redis_client) -> None:
    await pg_client.execute(SCHEMA)

    deps = (
        DepsRegistry.from_modules(
            PostgresDepsModule(
                client=pg_client,
                rw_documents={"products": PRODUCT_PG},
                tx={"products"},
            ),
            RedisDepsModule(
                client=redis_client,
                caches={"products": RedisCacheConfig(namespace=f"it:products:{uuid4().hex[:8]}")},
            ),
        )
        .freeze()
        .resolve()
    )
    ctx = ExecutionContext(deps=deps)

    # cache_scenario asserts the cache is populated after a read and refreshed
    # after an update; here we confirm the final value it returns.
    result = await cache_scenario(ctx)

    assert result.price == 12
