"""Recipe: serve a real forze app on in-memory backends — a stateful mock server.

The app below is an ordinary one: a `DocumentSpec`, generated CRUD routes, API-key authn.
What makes it a *mock server* is one line of wiring — `MockDepsModule` in place of the
backend modules. Nothing else changes: the same generated routes, the same handlers, the
same identity plane, the same error mapping. A frontend gets a stateful API with `create`
→ `list` coherence, real pagination cursors and real `rev`/OCC conflicts, with no Postgres,
no Redis, no container.

The property worth the whole recipe: **contract drift is structurally impossible**. The
routes are generated from the same frozen registry production serves, so there is no mock
to keep in sync — a schema faker over OpenAPI cannot say that, and cannot honor a cursor
or a `rev` either.

Run it:  ``just run``   (from examples/recipes/mock_server/) — then:

    curl -sS -X POST localhost:8000/products/list \
        -H 'X-API-Key: dev-key' -H 'content-type: application/json' -d '{}'

Exercised by ``tests/unit/test_examples/test_mock_server.py``.

**Not for production.** `MockDepsModule` keeps everything in memory and enforces none of
the durability, isolation or capability limits a real backend does. Serve it on a laptop
or in CI; never behind a deployment.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.routes import attach_document_routes
from forze_fastapi.security import AuthnRequirement, HeaderApiKeyAuthn
from forze_identity.authz import policy_principal_spec
from forze_identity.authz.domain import CreatePolicyPrincipalCmd
from forze_identity.builtin.local import LocalIdentityConfig, from_json_path, local_identity_deps
from forze_kits.aggregates.document import build_document_registry
from forze_mock import MockDepsModule, MockState

_LOGGER_NAME = "examples.mock_server"

type Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]

KEY_FILE = Path(__file__).with_name("identity.local.json")

# The principal the dev key maps to (identity.local.json). The app authenticates it with
# its *own* identity plane — the mock server has no way to mint a principal, by design.
DEV_PRINCIPAL = UUID("550e8400-e29b-41d4-a716-446655440000")


def _setup_logging(level: LogLevel) -> None:
    # Only when run as a script — global logging stays untouched for imports/tests.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


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


product_spec = DocumentSpec(
    name="products",
    read=ProductRead,
    write=DocumentWriteTypes(domain=Product, create_cmd=ProductCreate, update_cmd=ProductUpdate),
)
# --8<-- [end:domain]


AUTHN = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))


# --8<-- [start:wiring]
def build_runtime(identity: LocalIdentityConfig) -> ExecutionRuntime:
    """The only thing a mock server changes: which modules answer the ports.

    `MockDepsModule` registers every port as a *fallback*, so the app's real
    identity wiring composes on top of it in one registry — the local API-key verifier and
    tenant resolver win their routes, the mock keeps the other ~45 planes. Before that,
    this pairing raised at freeze and a mock context could not carry real identity at all.
    """

    deps = DepsRegistry.from_modules(MockDepsModule(state=MockState())).with_deps(
        local_identity_deps(identity, authn_route=AUTHN.name, tenancy_route=AUTHN.name),
    )

    return ExecutionRuntime(deps=deps.freeze())


# --8<-- [end:wiring]


# --8<-- [start:app]
registry = build_document_registry(product_spec).freeze()


def build_app(
    runtime: ExecutionRuntime,
    *,
    lifespan: Lifespan | None = None,
) -> FastAPI:
    """The app's own factory — production writes exactly this and passes a real runtime.

    Keeping the factory parameterized by runtime is what makes the swap total: routes,
    middleware, exception handlers and identity ingress are shared verbatim between the
    served mock and the deployed service. *lifespan* defaults to the production one
    (`runtime_lifespan`); the mock server passes a variant that seeds once the scope is
    open, which is the only seam it needs — the routes below never learn about it.
    """

    ctx = runtime.get_context

    router = APIRouter(prefix="/products")
    # Generated from the operation catalog — one route per registered kernel operation,
    # `operation_id` equal to the operation key (`products.get`, `products.list`, …).
    attach_document_routes(
        router,
        registry=registry,
        ns=product_spec.default_namespace,
        ctx_dep=ctx,
        style="rest",
    )

    app = FastAPI(
        title="Products API (mock)",
        lifespan=lifespan or runtime_lifespan(runtime),
    )
    app.include_router(router)
    register_exception_handlers(app)  # CoreException → HTTP: not_found → 404, conflict → 409
    app.add_middleware(InvocationMetadataMiddleware, ctx_dep=ctx)
    app.add_middleware(
        SecurityContextMiddleware,
        ctx_dep=ctx,
        authn=AuthnRequirement(
            ingress=(HeaderApiKeyAuthn(authn_spec=AUTHN, header_name="X-API-Key", required=True),),
        ),
        when_multiple_credentials="first_in_order",
    )

    return app


# --8<-- [end:app]


# --8<-- [start:seed]
_CATALOG = (("Espresso", 250), ("Cortado", 320), ("Filter", 280))


async def seed(ctx: ExecutionContext) -> None:
    """Fill the empty store — an API that answers `[]` is one no frontend can build against.

    Seeds go through the **write path**, never into `MockState` directly, so `rev`,
    timestamps, materialized fields and field encryption are produced by the same code
    that serves the reads.
    """

    # The app's own eligibility gate reads a `policy_principal` document, so the dev key
    # needs one or authentication fails with "Principal not found". It has to be this
    # document write: the mock's `PrincipalRegistryPort.ensure_principal` is a no-op that
    # returns a ref without storing anything the real gate can then read.
    await ctx.doc.command(policy_principal_spec).create(
        CreatePolicyPrincipalCmd(kind="user"),
        id=DEV_PRINCIPAL,
    )

    for name, price in _CATALOG:
        await ctx.doc.command(product_spec).create(ProductCreate(name=name, price=price))


# --8<-- [end:seed]


def seeding_lifespan(runtime: ExecutionRuntime) -> Lifespan:
    """The production lifespan, plus a seed once the runtime scope is open.

    The one seam a mock server needs. `MockApp` formalizes this pair —
    `build_app` and `seed` stay separate so the served app is the production one.
    """

    production = runtime_lifespan(runtime)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        async with production(app):
            await seed(runtime.get_context())
            yield

    return lifespan


def build_server() -> FastAPI:
    """Compose runtime + seed + app — the whole mock server."""

    runtime = build_runtime(from_json_path(KEY_FILE))

    return build_app(runtime, lifespan=seeding_lifespan(runtime))


if __name__ == "__main__":
    import uvicorn

    _setup_logging("info")
    uvicorn.run(build_server(), host="127.0.0.1", port=8000)
