"""Decimal fields index as JSON numbers so Meilisearch can filter and sort numerically.

A ``mode="json"`` dump stringifies a ``Decimal``; the gateway re-numbers those leaves
(guided by the live model's values), leaving genuine string fields and sealed roots alone.
"""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search.base import (
    MeilisearchSearchGateway,
    _model_may_hold_decimal,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

# ----------------------- #


class _Nested(BaseModel):
    price: Decimal
    label: str


class _Item(BaseModel):
    id: str
    title: str = ""
    price: Decimal = Decimal("0")
    maybe: Decimal | None = None
    nested: _Nested | None = None
    prices: list[Decimal] = []
    by_key: dict[str, Decimal] = {}
    fake_number: str = "10.5"


class _PlainItem(BaseModel):
    id: str
    title: str = ""
    count: int = 0


def _gateway(
    model_type: type[BaseModel] = _Item,
    *,
    encryption: FieldEncryption | None = None,
) -> MeilisearchSearchGateway[Any]:
    return MeilisearchSearchGateway(
        spec=SearchSpec(
            name="items",
            model_type=model_type,
            fields=["title"],
            encryption=encryption,
        ),
        config=MeilisearchSearchConfig(index_uid="items"),
    )


# ....................... #


def test_decimal_fields_index_as_numbers() -> None:
    gw = _gateway()
    doc = gw.to_index_document(
        _Item(
            id="a",
            title="t",
            price=Decimal("10.50"),
            maybe=Decimal("1.5"),
            nested=_Nested(price=Decimal("9.5"), label="x"),
            prices=[Decimal("1.5"), Decimal("2.5")],
            by_key={"k": Decimal("3.5")},
        ),
    )

    assert doc["price"] == 10.5 and isinstance(doc["price"], float)
    assert doc["maybe"] == 1.5 and isinstance(doc["maybe"], float)
    assert doc["nested"] == {"price": 9.5, "label": "x"}
    assert doc["prices"] == [1.5, 2.5]
    assert doc["by_key"] == {"k": 3.5}


def test_string_field_that_looks_numeric_is_untouched() -> None:
    """The conversion is value-driven — a ``str`` field is never coerced."""

    gw = _gateway()
    doc = gw.to_index_document(_Item(id="a", fake_number="10.5"))

    assert doc["fake_number"] == "10.5" and isinstance(doc["fake_number"], str)


def test_none_decimal_stays_null() -> None:
    gw = _gateway()
    doc = gw.to_index_document(_Item(id="a", maybe=None))

    assert doc["maybe"] is None


def test_sealed_root_is_left_untouched() -> None:
    """A field-encrypted root's dumped value is a ciphertext envelope — never coerced."""

    gw = _gateway(encryption=FieldEncryption(encrypted=frozenset({"price"})))
    doc = gw.to_index_document(_Item(id="a", price=Decimal("10.50")))

    # The plain (non-wrapped) codec dumps the plaintext string; what matters is that
    # the sealed root bypassed the numeric conversion entirely.
    assert doc["price"] == "10.50" and isinstance(doc["price"], str)


def test_model_without_decimal_skips_conversion_entirely() -> None:
    assert _model_may_hold_decimal(_Item) is True
    assert _model_may_hold_decimal(_PlainItem) is False

    gw = _gateway(_PlainItem)
    doc = gw.to_index_document(_PlainItem(id="a", title="t", count=3))

    assert doc == {"id": "a", "title": "t", "count": 3}


def test_may_hold_decimal_is_conservative_for_unknowable_annotations() -> None:
    class _AnyPayload(BaseModel):
        id: str
        payload: dict[str, Any] = {}

    assert _model_may_hold_decimal(_AnyPayload) is True

    gw = _gateway(_AnyPayload)
    doc = gw.to_index_document(_AnyPayload(id="a", payload={"amount": Decimal("2.5")}))

    assert doc["payload"] == {"amount": 2.5}
