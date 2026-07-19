"""Recipe: a typed read-only document API over Postgres (``write=None``).

The spec has no write side, so no command port is registered — the API only
queries. Data is owned and written elsewhere; here we seed a few rows directly
(through the client, not a document port) to keep the example self-contained.

Run it:  just run   (from examples/recipes/read_only/)
Exercised by tests/integration/test_examples/test_read_only.py (real Postgres).
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    LifecyclePlan,
)
from forze.base.primitives import RuntimeVar
from forze.domain.models import BaseDTO, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentIdDTO,
    ListRequestDTO,
    build_document_registry,
)
from forze_kits.dto import Paginated
from forze_postgres import (
    PostgresClient,
    PostgresConfig,
    PostgresDepsModule,
    PostgresLifecycleModule,
    PostgresReadOnlyDocumentConfig,
)


# --8<-- [start:spec]
class ArticleRead(ReadDocument):  # inherits id, rev, created_at, last_update_at
    title: str
    body: str


article_spec = DocumentSpec(name="articles", read=ArticleRead, write=None)
# --8<-- [end:spec]


# --8<-- [start:wiring]
# Read-only config: just the read relation — no write side, no bookkeeping.
ARTICLE_PG = PostgresReadOnlyDocumentConfig(read=("public", "articles"))


def build_runtime(pg: PostgresClient, *, dsn: str) -> ExecutionRuntime:
    deps = DepsRegistry.from_modules(
        PostgresDepsModule(client=pg, ro_documents={"articles": ARTICLE_PG}),
    )
    lifecycle = LifecyclePlan.from_modules(
        PostgresLifecycleModule(client=pg, dsn=dsn, config=PostgresConfig()),
    )
    return ExecutionRuntime(deps=deps.freeze(), lifecycle=lifecycle.freeze())
# --8<-- [end:wiring]


SCHEMA = """
CREATE TABLE IF NOT EXISTS public.articles (
    id             uuid PRIMARY KEY,
    rev            bigint      NOT NULL DEFAULT 1,
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_update_at timestamptz NOT NULL DEFAULT now(),
    title          text        NOT NULL,
    body           text        NOT NULL
)
"""

SEED = [
    (UUID("00000000-0000-0000-0000-000000000001"), "Hexagonal architecture", "Ports and adapters."),
    (UUID("00000000-0000-0000-0000-000000000002"), "Domain-driven design", "Aggregates and invariants."),
]


async def seed(pg: PostgresClient) -> None:
    # No write port on a read-only doc — seed straight through the client.
    for row in SEED:
        await pg.execute(
            "INSERT INTO public.articles (id, title, body) VALUES (%s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING",
            list(row),
        )


_rt = RuntimeVar[ExecutionRuntime]("rt")


def ctx() -> ExecutionContext:
    return _rt.get().get_context()


# --8<-- [start:routes]
# A read-only spec (write=None) derives a read-only DTO mapping.
registry = build_document_registry(article_spec).freeze()


@asynccontextmanager
async def lifespan(app: FastAPI):
    pg = PostgresClient()
    dsn = os.environ.get("POSTGRES_DSN", "postgresql://forze:forze@localhost:5432/forze")
    _rt.set_once(build_runtime(pg, dsn=dsn))
    async with _rt.get().scope():
        await pg.execute(SCHEMA)
        await seed(pg)
        yield


app = FastAPI(title="Articles API (read-only)", lifespan=lifespan)
register_exception_handlers(app)


def articles() -> DocumentFacade[ArticleRead, BaseDTO, BaseDTO]:
    return DocumentFacade(
        ctx=ctx(),
        registry=registry,
        namespace=article_spec.default_namespace,
    )


@app.get("/articles/{article_id}")
async def get_article(article_id: UUID) -> ArticleRead:
    # `get` raises not_found (→ 404) on a miss.
    return await articles().get(DocumentIdDTO(id=article_id))


@app.get("/articles")
async def list_articles(page: int = 1, size: int = 20) -> Paginated[ArticleRead]:
    return await articles().list(ListRequestDTO(page=page, size=size))
# --8<-- [end:routes]
