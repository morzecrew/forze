"""Recipe: an HTTP CRUD service for a ``Product`` aggregate over Postgres.

A `DocumentSpec` plus a `PostgresDepsModule` is the whole persistence story; the
FastAPI routes use a `DocumentFacade` and never touch persistence ports or SQL.
Optimistic concurrency comes for free — updates carry the document's ``rev``.

Run it against the recipe's ``compose.yaml``:

    just run            # from examples/recipes/crud_fastapi/

The routes are exercised end to end (real Postgres) by
``tests/integration/test_examples/test_crud_fastapi.py``.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    LifecyclePlan,
)
from forze.base.primitives import RuntimeVar
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentIdDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
    build_document_registry,
)
from forze_postgres import (
    PostgresClient,
    PostgresConfig,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresLifecycleModule,
)

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


# --8<-- [start:wiring]
def build_runtime(pg: PostgresClient, *, dsn: str) -> ExecutionRuntime:
    deps = DepsRegistry.from_modules(
        PostgresDepsModule(
            client=pg, rw_documents={"products": PRODUCT_PG}, tx={"products"}
        ),
    )
    lifecycle = LifecyclePlan.from_modules(
        PostgresLifecycleModule(client=pg, dsn=dsn, config=PostgresConfig()),
    )
    return ExecutionRuntime(deps=deps.freeze(), lifecycle=lifecycle.freeze())
# --8<-- [end:wiring]


_rt = RuntimeVar[ExecutionRuntime]("rt")


def ctx() -> ExecutionContext:
    return _rt.get().get_context()


# --8<-- [start:routes]
# DTOs are derived from the spec's read + write models.
registry = build_document_registry(product_spec).freeze()


@asynccontextmanager
async def lifespan(app: FastAPI):
    pg = PostgresClient()
    dsn = os.environ.get("POSTGRES_DSN", "postgresql://forze:forze@localhost:5432/forze")
    _rt.set_once(build_runtime(pg, dsn=dsn))
    async with _rt.get().scope():
        await pg.execute(SCHEMA)  # demo bootstrap (real apps migrate instead)
        yield


app = FastAPI(title="Products API", lifespan=lifespan)
register_exception_handlers(app)  # CoreException → HTTP (not_found → 404, conflict → 409)


def products() -> DocumentFacade[ProductRead, ProductCreate, ProductUpdate]:
    return DocumentFacade(
        ctx=ctx(),
        registry=registry,
        namespace=product_spec.default_namespace,
    )


@app.post("/products")
async def create_product(cmd: ProductCreate) -> ProductRead:
    return await products().create(cmd)


@app.get("/products/{product_id}")
async def get_product(product_id: UUID) -> ProductRead:
    return await products().get(DocumentIdDTO(id=product_id))


@app.get("/products")
async def list_products() -> list[ProductRead]:
    page = await products().list(ListRequestDTO())
    return list(page.hits)


@app.put("/products/{product_id}")
async def update_product(product_id: UUID, rev: int, patch: ProductUpdate) -> ProductRead:
    result = await products().update(
        DocumentUpdateDTO(id=product_id, rev=rev, dto=patch)
    )
    return result.data


@app.delete("/products/{product_id}", status_code=204)
async def delete_product(product_id: UUID) -> None:
    await products().kill(DocumentIdDTO(id=product_id))
# --8<-- [end:routes]
