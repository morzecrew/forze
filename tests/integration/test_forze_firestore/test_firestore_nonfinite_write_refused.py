"""Firestore writes refuse non-finite and double-overflowing numerics end to end.

The decimal→double write coercion shares one seam with filter values
(``coerce_firestore_value``). These exercise it through the write gateway against the
emulator: a ``Decimal`` that overflows the double range (accepted by a typed model —
pydantic only refuses literal ``NaN``/``Infinity``) and a non-finite value smuggled
through an untyped dict field both raise ``precondition`` and persist nothing, instead
of writing ``inf``/``nan`` into the system of record. In-range precision loss remains
the documented cost of the double representation.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest
from pydantic import Field

from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.integration.test_forze_firestore._fixtures import client_context

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class DecimalDoc(Document):
    total: Decimal = Decimal("0")
    meta: dict[str, Any] = Field(default_factory=dict)


class DecimalCreate(CreateDocumentCmd):
    total: Decimal = Decimal("0")
    meta: dict[str, Any] = Field(default_factory=dict)


class DecimalUpdate(BaseDTO):
    total: Decimal | None = None


async def _assert_absent(read: Any, doc_id: UUID) -> None:
    with pytest.raises(CoreException) as ei:
        await read.get(doc_id)
    assert ei.value.kind is ExceptionKind.NOT_FOUND


def _gateways(ctx: ExecutionContext, collection: str) -> tuple[Any, Any]:
    write = doc_write_gw(
        ctx,
        write_types={
            "domain": DecimalDoc,
            "create_cmd": DecimalCreate,
            "update_cmd": DecimalUpdate,
        },
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )
    read = read_gw(
        ctx,
        read_type=DecimalDoc,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )
    return write, read


async def test_create_refuses_decimal_overflowing_double(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``Decimal("1e400")`` passes a typed model but must not persist as ``inf``."""

    collection = f"gw_nonfinite_{unique_collection}"
    ctx = client_context(firestore_client)
    write, read = _gateways(ctx, collection)

    doc_id = UUID("70000000-0000-0000-0000-000000000001")
    with pytest.raises(CoreException) as ei:
        await write.create(DecimalCreate(total=Decimal("1e400")), id=doc_id)
    assert ei.value.kind is ExceptionKind.PRECONDITION

    await _assert_absent(read, doc_id)  # nothing was persisted


async def test_create_refuses_non_finite_in_untyped_dict_field(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """A ``Decimal("NaN")`` inside a ``dict[str, Any]`` field skips pydantic's
    non-finite refusal; the write seam is the last guard before the SoR."""

    collection = f"gw_nonfinite_dict_{unique_collection}"
    ctx = client_context(firestore_client)
    write, read = _gateways(ctx, collection)

    doc_id = UUID("70000000-0000-0000-0000-000000000002")
    with pytest.raises(CoreException) as ei:
        await write.create(
            DecimalCreate(meta={"score": Decimal("NaN")}), id=doc_id
        )
    assert ei.value.kind is ExceptionKind.PRECONDITION

    await _assert_absent(read, doc_id)


async def test_finite_decimal_round_trips(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """The guard does not narrow the supported range: a finite Decimal still writes
    and reads back at double precision."""

    collection = f"gw_nonfinite_ok_{unique_collection}"
    ctx = client_context(firestore_client)
    write, read = _gateways(ctx, collection)

    created = await write.create(DecimalCreate(total=Decimal("19.99")))
    loaded = await read.get(created.id)
    assert loaded.total == Decimal("19.99")
