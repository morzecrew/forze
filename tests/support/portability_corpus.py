"""A governed corpus for the RFC 0017 portability tests — one document spec, one blob route.

Shared by the ``migrate`` tests and the ``run_export_import_roundtrip`` conformance tests so they
exercise the *same* shapes the file round-trip does. The document carries the types that break the
mock horizon (``UUID`` + ``datetime`` + ``Decimal``); the create model mixes in
:class:`ImportTimestamps`, the app-side requirement for faithful timestamps on import (RFC §7).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.dto import ImportTimestamps
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #


class OrderDoc(Document):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class OrderRead(ReadDocument):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class OrderCreate(ImportTimestamps):
    ref: UUID
    placed_at: datetime
    total: Decimal
    label: str


class OrderUpdate(BaseDTO):
    label: str | None = None


ORDER_SPEC: DocumentSpec[OrderRead, OrderDoc, OrderCreate, OrderUpdate] = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=OrderDoc, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)

ATTACHMENTS = StorageSpec(name="attachments")

# Whole-second timestamps keep the round-trip about type fidelity, not the sub-millisecond
# precision the ``datetime-subsecond-precision`` divergence catalogs (BSON is ms-precision).
_CREATED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
_UPDATED = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)


# ....................... #


def order_registry(*, with_blobs: bool = False) -> SpecRegistry:
    registry = SpecRegistry().register(ORDER_SPEC)

    return registry.register(ATTACHMENTS) if with_blobs else registry


def mock_runtime(state: MockState, *, with_blobs: bool = False) -> ExecutionRuntime:
    # allow_unregistered: the mock wires every plane generically, far more than this corpus, so the
    # bound-but-not-catalogued direction is a warning here, not an error.
    return build_runtime(
        MockDepsModule(state=state),
        specs=order_registry(with_blobs=with_blobs),
        allow_unregistered=True,
    )


def order_corpus(count: int) -> list[tuple[UUID, OrderCreate]]:
    return [
        (
            uuid4(),
            OrderCreate(
                ref=uuid4(),
                placed_at=datetime(2026, 6, index + 1, 12, 0, 0, tzinfo=UTC),
                total=Decimal(f"{index + 1}.99"),
                label=f"order-{index}",
                created_at=_CREATED,
                last_update_at=_UPDATED,
            ),
        )
        for index in range(count)
    ]


async def seed_orders(
    ctx: ExecutionContext,
    corpus: list[tuple[UUID, OrderCreate]],
    *,
    tenant: UUID | None = None,
) -> dict[UUID, OrderRead]:
    """Write *corpus* into *ctx*, optionally inside a tenant binding, returning the read models."""

    seeded: dict[UUID, OrderRead] = {}

    with _maybe_tenant(ctx, tenant):
        command = ctx.document.command(ORDER_SPEC)

        for order_id, create in corpus:
            seeded[order_id] = await command.ensure(order_id, create)

    return seeded


async def read_orders(
    ctx: ExecutionContext,
    ids: list[UUID],
    *,
    tenant: UUID | None = None,
) -> dict[UUID, OrderRead]:
    with _maybe_tenant(ctx, tenant):
        found = await ctx.document.query(ORDER_SPEC).get_many(ids)

    return {doc.id: doc for doc in found}


async def seed_attachments(
    ctx: ExecutionContext,
    blobs: list[tuple[bytes, dict[str, str]]],
) -> dict[str, tuple[bytes, dict[str, str]]]:
    seeded: dict[str, tuple[bytes, dict[str, str]]] = {}
    command = ctx.storage.command(ATTACHMENTS)

    for content, tags in blobs:
        obj = await command.upload_stream(
            _one_chunk(content), filename="f.bin", tags=tags, content_type="application/pdf"
        )
        seeded[obj.key] = (content, tags)

    return seeded


async def download_attachment(ctx: ExecutionContext, key: str) -> bytes:
    streamed = await ctx.storage.query(ATTACHMENTS).download_stream(key)

    return b"".join([chunk async for chunk in streamed.chunks])


def assert_orders_faithful(
    restored: dict[UUID, OrderRead], original: dict[UUID, OrderRead]
) -> None:
    assert set(restored) == set(original)

    for doc_id, want in original.items():
        got = restored[doc_id]
        assert got.ref == want.ref, "UUID field must survive"
        assert got.placed_at == want.placed_at, "datetime field must survive"
        assert got.total == want.total, "Decimal field must survive exactly"
        assert got.label == want.label
        assert got.created_at == want.created_at, "created_at preserved via ImportTimestamps"
        assert got.last_update_at == want.last_update_at
        assert got.rev == 1  # optimistic-concurrency lineage resets by design (RFC §7)


# ....................... #


def _maybe_tenant(ctx: ExecutionContext, tenant: UUID | None) -> AbstractContextManager[Any]:
    if tenant is None:
        return nullcontext()

    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


async def _one_chunk(data: bytes) -> AsyncIterator[bytes]:
    yield data
