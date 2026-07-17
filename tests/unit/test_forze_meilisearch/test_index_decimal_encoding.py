"""Decimal fields index as JSON numbers so Meilisearch can filter and sort numerically.

A ``mode="json"`` dump stringifies a ``Decimal``; the gateway re-numbers those leaves
(guided by the live model's values), leaving genuine string fields and sealed roots alone.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search.base import (
    MeilisearchSearchGateway,
    _canonicalize_leaves,
    _model_may_hold_canonical_leaf,
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


def test_model_without_canonical_leaves_skips_conversion_entirely() -> None:
    assert _model_may_hold_canonical_leaf(_Item) is True
    assert _model_may_hold_canonical_leaf(_PlainItem) is False

    gw = _gateway(_PlainItem)
    doc = gw.to_index_document(_PlainItem(id="a", title="t", count=3))

    assert doc == {"id": "a", "title": "t", "count": 3}


def test_may_hold_leaf_scan_is_conservative_for_unknowable_annotations() -> None:
    class _AnyPayload(BaseModel):
        id: str
        payload: dict[str, Any] = {}

    assert _model_may_hold_canonical_leaf(_AnyPayload) is True

    gw = _gateway(_AnyPayload)
    doc = gw.to_index_document(_AnyPayload(id="a", payload={"amount": Decimal("2.5")}))

    assert doc["payload"] == {"amount": 2.5}


def test_may_hold_leaf_scan_annotation_shapes() -> None:
    """The scanner sees through Annotated / unions / containers, terminates on
    recursive models, and stays conservative on unknown generics."""

    from collections.abc import Sequence

    class _AnnotatedDec(BaseModel):
        v: Annotated[Decimal, "meta"]

    class _TupleInts(BaseModel):
        v: tuple[int, ...] = ()

    class _Recursive(BaseModel):
        children: list["_Recursive"] = []

    class _UnknownGeneric(BaseModel):
        v: Sequence[int] = ()

    class _Stamped(BaseModel):
        at: datetime

    assert _model_may_hold_canonical_leaf(_AnnotatedDec) is True
    assert _model_may_hold_canonical_leaf(_TupleInts) is False
    assert _model_may_hold_canonical_leaf(_Recursive) is False
    assert _model_may_hold_canonical_leaf(_UnknownGeneric) is True
    assert _model_may_hold_canonical_leaf(_Stamped) is True


def test_unrepresentable_decimal_keeps_string_form() -> None:
    """A finite Decimal whose magnitude overflows f64 must not poison the upsert with an
    ``inf`` JSON number — it stays a string. (Explicit NaN/Infinity never get this far:
    pydantic validation rejects them as non-finite; the walk guards them anyway.)"""

    class _Wide(BaseModel):
        id: str
        huge: Decimal

    gw = _gateway(_Wide)
    doc = gw.to_index_document(_Wide(id="a", huge=Decimal("1e1000")))

    assert doc["huge"] == "1E+1000" and isinstance(doc["huge"], str)

    # Defense-in-depth on the walk itself for values that bypass validation.
    assert _canonicalize_leaves(Decimal("NaN"), "NaN") == "NaN"
    assert _canonicalize_leaves(Decimal("Infinity"), "Infinity") == "Infinity"


def test_aware_datetime_normalizes_to_utc_z() -> None:
    """A non-UTC offset indexes as the UTC-``Z`` text filter literals render, so an
    ``$eq`` operand for the same instant matches; naive timestamps stay as dumped."""

    class _Stamped(BaseModel):
        id: str
        at: datetime
        naive: datetime

    gw = _gateway(_Stamped)
    doc = gw.to_index_document(
        _Stamped(
            id="a",
            at=datetime(2024, 1, 2, 6, 4, 5, tzinfo=timezone(timedelta(hours=3))),
            naive=datetime(2024, 1, 2, 3, 4, 5),
        ),
    )

    assert doc["at"] == "2024-01-02T03:04:05Z"
    assert doc["naive"] == "2024-01-02T03:04:05"


def test_utc_datetime_representation_is_unchanged() -> None:
    class _Stamped(BaseModel):
        id: str
        at: datetime

    gw = _gateway(_Stamped)
    doc = gw.to_index_document(
        _Stamped(id="a", at=datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)),
    )

    assert doc["at"] == "2024-01-02T03:04:05Z"


def test_set_members_convert_by_value_not_position() -> None:
    """Set iteration order cannot pair positionally against the dumped list — each
    Decimal claims exactly the string it serialized to, and a plain-``str`` member
    that *equals* a Decimal's text is not converted twice."""

    prices = {Decimal("9.5"), Decimal("10.5")}
    dumped = ["10.5", "9.5"]  # deliberately reversed vs any particular iteration
    out = _canonicalize_leaves(prices, dumped)
    assert out == [10.5, 9.5]

    mixed: set[Any] = {Decimal("9.5"), "hello"}
    out = _canonicalize_leaves(mixed, ["hello", "9.5"])
    assert out == ["hello", 9.5]

    twin: set[Any] = {Decimal("9.5"), "9.5"}
    out = _canonicalize_leaves(twin, ["9.5", "9.5"])
    assert sorted(out, key=str) == sorted([9.5, "9.5"], key=str)

    stamps = {datetime(2024, 1, 2, 6, 4, 5, tzinfo=timezone(timedelta(hours=3)))}
    out = _canonicalize_leaves(stamps, ["2024-01-02T06:04:05+03:00"])
    assert out == ["2024-01-02T03:04:05Z"]


def test_structural_mismatch_keeps_dumped_value() -> None:
    assert _canonicalize_leaves([Decimal("1.5")], ["1.5", "2.5"]) == ["1.5", "2.5"]
    assert _canonicalize_leaves(None, "x") == "x"
