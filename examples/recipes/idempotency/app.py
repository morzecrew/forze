"""Recipe: make a retried mutation a no-op that returns the first result.

An idempotency key (the ``Idempotency-Key`` header over HTTP) lets the engine
wrap a mutating operation: the first call runs the handler and stores its result;
a replay with the same key returns the stored result and skips the handler — and
its transaction — entirely. Wiring is one wrap on the operation registry plus an
idempotency adapter (here the in-memory mock; swap in Redis for production).

Run it:  uv run python -m examples.recipes.idempotency.app   (no infra — mock store)
Exercised by tests/unit/test_examples/test_idempotency.py.
"""

from __future__ import annotations

import asyncio

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentFacade,
    build_document_registry,
)
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_mock import MockDepsModule

# --8<-- [start:domain]
class Order(Document):
    item: str


class CreateOrder(CreateDocumentCmd):
    item: str


class ReadOrder(ReadDocument):
    item: str
# --8<-- [end:domain]


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=ReadOrder,
    write={"domain": Order, "create_cmd": CreateOrder},
)


# --8<-- [start:wrap]
# The idempotency spec carries the TTL and names the adapter route.
IDEM = IdempotencySpec(name="orders")

# "orders.create" — the namespaced key of the registry's create operation.
CREATE = ORDER_SPEC.default_namespace.key(DocumentKernelOp.CREATE)

# Wrap that operation with idempotency. The wrap sits outermost, so a replay
# skips the handler (and its transaction); `before` hooks (authn/authz) still run.
REGISTRY = (
    build_document_registry(ORDER_SPEC, DocumentDTOs(read=ReadOrder, create=CreateOrder))
    .bind(CREATE)
    .bind_outer()
    .wrap(IdempotencyWrap(op=CREATE, spec=IDEM, result_type=ReadOrder).to_step())
    .finish(deep=True)
    .freeze()
)
# --8<-- [end:wrap]


def orders(ctx: ExecutionContext) -> DocumentFacade[ReadOrder, CreateOrder, BaseDTO]:
    return DocumentFacade(ctx=ctx, registry=REGISTRY, namespace=ORDER_SPEC.default_namespace)


# --8<-- [start:scenario]
async def idempotent_create(ctx: ExecutionContext) -> tuple[ReadOrder, ReadOrder]:
    facade = orders(ctx)
    cmd = CreateOrder(item="widget")

    # Over HTTP the FastAPI InvocationMetadataMiddleware binds the
    # Idempotency-Key header for you; here we bind the key explicitly.
    with ctx.inv_ctx.bind_idempotency("order-001"):
        first = await facade.create(cmd)
        second = await facade.create(cmd)  # replay — handler skipped, stored result

    assert first.id == second.id  # created once, returned twice
    return first, second
# --8<-- [end:scenario]


async def main() -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        first, second = await idempotent_create(runtime.get_context())
        print(f"created once, returned twice: {first.id == second.id}")


if __name__ == "__main__":
    asyncio.run(main())
