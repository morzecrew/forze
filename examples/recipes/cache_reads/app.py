"""Recipe: read-through caching of a document — Postgres-backed, Redis cache.

A ``Product`` aggregate carries a ``CacheSpec``: reads serve from Redis on a hit
and populate it on a miss; writes invalidate it. The handlers never change —
caching is wired entirely through the deps.

Run it against the recipe's ``compose.yaml``:

    just run            # from examples/recipes/cache_reads/

The read-through behaviour is exercised by
``tests/integration/test_examples/test_cache_reads.py``.
"""

from __future__ import annotations

import asyncio
import os

import structlog

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    LifecyclePlan,
)
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentIdDTO,
    DocumentUpdateDTO,
    build_document_registry,
)
from forze_postgres import (
    PostgresClient,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresLifecycleModule,
)
from forze_redis import (
    RedisCacheConfig,
    RedisClient,
    RedisDepsModule,
    redis_lifecycle_step,
)

_LOGGER_NAME = "cache_reads"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly (and filter trace/debug),
    # **only when run as a script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# ----------------------- #
# Domain


# --8<-- [start:domain]
class Product(Document):
    name: str
    price: int


class ProductCreate(CreateDocumentCmd):
    name: str
    price: int


class ProductUpdate(BaseDTO):
    name: str | None = None
    price: int | None = None


class ProductRead(ReadDocument):
    name: str
    price: int


# --8<-- [end:domain]


# --8<-- [start:spec]
product_spec = DocumentSpec(
    name="products",
    read=ProductRead,
    write=DocumentWriteTypes(
        domain=Product, create_cmd=ProductCreate, update_cmd=ProductUpdate
    ),
    cache=CacheSpec(name="products"),  # reads cached, writes invalidate
)
# --8<-- [end:spec]


PRODUCT_PG = PostgresDocumentConfig(
    read=("public", "products"),
    write=("public", "products"),
    bookkeeping_strategy="application",
)

# A demo creates its own table; real apps own their schema via migrations.
SCHEMA = """
CREATE TABLE IF NOT EXISTS public.products (
    id             uuid PRIMARY KEY,
    rev            bigint      NOT NULL DEFAULT 1,
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_update_at timestamptz NOT NULL DEFAULT now(),
    name           text        NOT NULL,
    price          integer     NOT NULL
)
"""

# ----------------------- #
# Wiring — Postgres stores documents, Redis caches reads.


# --8<-- [start:wiring]
def build_runtime(
    pg: PostgresClient,
    redis: RedisClient,
    *,
    pg_dsn: str,
    redis_dsn: str,
) -> ExecutionRuntime:
    deps = DepsRegistry.from_modules(
        PostgresDepsModule(
            client=pg,
            rw_documents={"products": PRODUCT_PG},
            tx={"products"},
        ),
        # caches keyed by CacheSpec.name — this is the whole "cache reads" step
        RedisDepsModule(
            client=redis,
            caches={"products": RedisCacheConfig(namespace="app:products")},
        ),
    )
    lifecycle = LifecyclePlan.from_modules(
        PostgresLifecycleModule(client=pg, dsn=pg_dsn),
    ).with_steps(redis_lifecycle_step(dsn=redis_dsn))

    return ExecutionRuntime(deps=deps.freeze(), lifecycle=lifecycle.freeze())


# --8<-- [end:wiring]


# ----------------------- #
# The cache scenario — handlers untouched; reads go through the cache.


# --8<-- [start:read-through]
registry = build_document_registry(product_spec).freeze()


def products(
    ctx: ExecutionContext,
) -> DocumentFacade[ProductRead, ProductCreate, ProductUpdate]:
    return DocumentFacade(
        ctx=ctx,
        registry=registry,
        namespace=product_spec.default_namespace,
    )


async def cache_scenario(ctx: ExecutionContext) -> ProductRead:
    facade = products(ctx)
    cache = ctx.cache(product_spec.cache)  # pyright: ignore[reportArgumentType]

    product = await facade.create(ProductCreate(name="Widget", price=10))

    await facade.get(DocumentIdDTO(id=product.id))  # miss → fills the cache
    if await cache.get(str(product.id)) is None:  # cached now
        raise RuntimeError("expected the read to fill the cache")

    await facade.update(
        DocumentUpdateDTO(
            id=product.id,
            rev=product.rev,
            dto=ProductUpdate(price=12),
        )
    )
    fresh = await facade.get(DocumentIdDTO(id=product.id))  # repopulates, new value
    if fresh.price != 12:
        raise RuntimeError("expected the cached read to see the new price")

    return fresh


# --8<-- [end:read-through]


async def main() -> None:
    pg_dsn = os.environ.get(
        "POSTGRES_DSN", "postgresql://forze:forze@localhost:5432/forze"
    )
    redis_dsn = os.environ.get("REDIS_DSN", "redis://localhost:6379/0")

    pg = PostgresClient()
    redis = RedisClient()
    runtime = build_runtime(pg, redis, pg_dsn=pg_dsn, redis_dsn=redis_dsn)

    async with runtime.scope():
        await pg.execute(SCHEMA)  # demo bootstrap (real apps migrate instead)
        result = await cache_scenario(runtime.get_context())
        log.info("product cached and refreshed", price=result.price)


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
