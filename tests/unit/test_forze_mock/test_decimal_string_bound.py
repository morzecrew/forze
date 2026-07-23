"""A JSON string range bound on a Decimal field filters numerically, on every backend.

A JSON number cannot carry an exact Decimal, so an HTTP caller's only faithful way to send
a money range bound is a string. The shared ``coerce_query_ord_operands`` seam casts that
string to the field's scalar family once (keyed by the read model), so the in-memory matcher
and the real backends agree — a string compared as a string would filter lexically ("9" > "10")
where a number filters numerically. Ordering on a genuine *text* field stays refused.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import OPERATOR_TYPE_MISMATCH_CODE
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockState

pytestmark = pytest.mark.unit


class _Fields(BaseModel):
    name: str
    price: Decimal = Decimal("0")


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="t", read=_Read, write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create)
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="t",
        read_model=_Read,
        domain_model=_Doc,
    )


async def _seed(adapter: MockDocumentAdapter[Any, Any, Any, Any], prices: list[str]) -> None:
    for p in prices:
        await adapter.create(_Create(id=uuid4(), name=f"item-{p}", price=Decimal(p)))


@pytest.mark.asyncio
async def test_string_gt_bound_on_decimal_filters_numerically() -> None:
    adapter = _mock()
    await _seed(adapter, ["9", "10", "100.5", "2"])

    # A string bound: "10" as a string would sort after "100.5" lexically; numerically it
    # is below it. The coercion casts "10" to Decimal against the price field.
    page = await adapter.find_many({"$values": {"price": {"$gt": "10"}}})
    got = sorted(Decimal(str(h.price)) for h in page.hits)

    assert got == [Decimal("100.5")]  # only 100.5 > 10; "9" and "2" excluded numerically


@pytest.mark.asyncio
async def test_exact_high_precision_string_bound_is_not_float_rounded() -> None:
    adapter = _mock()
    just_below = "123.456789012345678900"
    exact = "123.456789012345678901"
    just_above = "123.456789012345678902"
    await _seed(adapter, [just_below, exact, just_above])

    # A bound that differs only in the 18th fractional digit — a float would collapse all
    # three together; the exact Decimal cast keeps them apart.
    page = await adapter.find_many({"$values": {"price": {"$gt": exact}}})
    got = [Decimal(str(h.price)) for h in page.hits]

    assert got == [Decimal(just_above)]


@pytest.mark.asyncio
async def test_string_ord_bound_on_a_text_field_is_refused() -> None:
    adapter = _mock()
    await _seed(adapter, ["1"])

    # The field-type gate still rejects an ordering op on a text field — admitting str to
    # the Numeric union never legalizes ``name $gt "x"``.
    with pytest.raises(CoreException) as caught:
        await adapter.find_many({"$values": {"name": {"$gt": "abc"}}})

    assert caught.value.code == OPERATOR_TYPE_MISMATCH_CODE


@pytest.mark.asyncio
async def test_unparseable_string_bound_on_decimal_is_refused() -> None:
    adapter = _mock()
    await _seed(adapter, ["1"])

    # A string that is not a number is refused by the cast, not silently matched to nothing.
    with pytest.raises(CoreException):
        await adapter.find_many({"$values": {"price": {"$gt": "not-a-number"}}})


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", ["NaN", "nan", "sNaN", "Infinity", "-inf"])
async def test_non_finite_string_bound_on_decimal_is_refused(bound: str) -> None:
    adapter = _mock()
    await _seed(adapter, ["1", "2"])

    # "NaN" parses as Decimal but is not a range bound: Postgres sorts 'NaN'::numeric
    # above every number, so a `$lt "NaN"` money filter would fail open and match every
    # row, while the in-memory Decimal comparison raises. Refused once at the cast.
    with pytest.raises(CoreException, match="Non-finite"):
        await adapter.find_many({"$values": {"price": {"$lt": bound}}})


@pytest.mark.asyncio
async def test_non_finite_native_decimal_bound_is_refused() -> None:
    adapter = _mock()
    await _seed(adapter, ["1", "2"])

    # A native Decimal("NaN") skips the string cast; the coercion seam still refuses it,
    # keeping the mock aligned with Postgres (whose render cast raises the same way).
    with pytest.raises(CoreException, match="Non-finite"):
        await adapter.find_many({"$values": {"price": {"$lt": Decimal("NaN")}}})
