"""`canonical_sort_key`: every value kind has its own place, and sorting never raises."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

from forze.application.contracts.querying import QueryFilterExpressionParser
from forze.application.contracts.querying.internal.canonical import canonical_sort_key

pytestmark = pytest.mark.unit


class _Color(Enum):
    RED = "red"
    BLUE = "blue"


_UID = UUID("12345678-1234-5678-1234-567812345678")


def _order(*values: object) -> list[object]:
    return sorted(values, key=canonical_sort_key)


class TestKinds:
    def test_none_sorts_first(self) -> None:
        assert _order("a", 1, None) == [None, 1, "a"]

    def test_a_bool_is_not_an_int(self) -> None:
        # True == 1 in Python; the kind tag keeps them apart.
        assert canonical_sort_key(True) != canonical_sort_key(1)
        assert _order(2, True, False, 1) == [False, True, 1, 2]

    def test_numbers_of_every_type_share_one_order(self) -> None:
        assert _order(Decimal("2.5"), 3, 1.5, -1) == [-1, 1.5, Decimal("2.5"), 3]

    def test_text_and_bytes_are_their_own_kinds(self) -> None:
        assert _order(b"a", "b", "a", 1) == [1, "a", "b", b"a"]

    def test_a_date_does_not_tie_its_iso_string(self) -> None:
        day = date(2024, 1, 1)

        assert canonical_sort_key(day) != canonical_sort_key("2024-01-01")
        assert _order(day, "2024-01-01") == ["2024-01-01", day]

    def test_datetimes_dates_and_times_are_kinds_of_their_own(self) -> None:
        moment = datetime(2024, 1, 1, 12, 0)
        day = date(2024, 1, 2)
        clock = time(9, 30)

        assert _order(clock, day, moment) == [moment, day, clock]
        assert _order(date(2024, 3, 1), date(2024, 1, 1)) == [date(2024, 1, 1), date(2024, 3, 1)]

    def test_a_uuid_orders_by_its_text(self) -> None:
        other = UUID("02345678-1234-5678-1234-567812345678")

        assert _order(_UID, other) == [other, _UID]

    def test_an_enum_orders_by_its_type_then_value(self) -> None:
        assert _order(_Color.RED, _Color.BLUE) == [_Color.BLUE, _Color.RED]

    def test_sequences_and_sets_key_by_their_elements(self) -> None:
        assert canonical_sort_key((1, "a")) == canonical_sort_key([1, "a"])
        assert canonical_sort_key(frozenset({"b", "a"})) == canonical_sort_key({"a", "b"})
        assert _order(frozenset({"b"}), frozenset({"a", "c"}), ("z",)) == [
            ("z",),
            frozenset({"a", "c"}),
            frozenset({"b"}),
        ]

    def test_a_mapping_keys_by_its_sorted_items(self) -> None:
        assert canonical_sort_key({"b": 1, "a": 2}) == canonical_sort_key({"a": 2, "b": 1})

    def test_anything_else_keys_by_its_type_and_repr(self) -> None:
        class _Thing:
            def __repr__(self) -> str:
                return "thing"

        key = canonical_sort_key(_Thing())

        assert (key[0], key[1].endswith("._Thing"), key[2]) == (13, True, "thing")


class TestNonFiniteNumbers:
    """NaN compares with nothing and a Decimal NaN raises on comparison; neither may reach `<`."""

    @pytest.mark.parametrize(
        "values",
        [
            (Decimal("NaN"), Decimal(1), Decimal("-Infinity"), Decimal("Infinity"), Decimal("sNaN")),
            (float("nan"), 1.0, float("-inf"), float("inf")),
            (Decimal("NaN"), float("nan"), 1, Decimal("-Infinity")),
        ],
        ids=["decimal", "float", "mixed"],
    )
    def test_sorting_never_raises_and_is_one_order(self, values: tuple[object, ...]) -> None:
        orders = {repr(_order(*values)), repr(_order(*reversed(values)))}

        assert len(orders) == 1

    def test_infinities_bracket_the_finite_values(self) -> None:
        assert _order(float("inf"), 1.0, float("-inf")) == [float("-inf"), 1.0, float("inf")]
        assert _order(Decimal("Infinity"), Decimal(1), Decimal("-Infinity")) == [
            Decimal("-Infinity"),
            Decimal(1),
            Decimal("Infinity"),
        ]

    @pytest.mark.parametrize(
        "operand",
        [
            {Decimal("NaN"), Decimal(1)},
            {Decimal("-Infinity"), Decimal(1)},
            {float("nan"), 1.0},
            frozenset({float("inf"), 2.0}),
            [Decimal("NaN"), 1],
            (float("nan"), 1),
        ],
        ids=["decimal-nan-set", "decimal-inf-set", "float-nan-set", "float-inf-frozenset", "nan-list", "nan-tuple"],
    )
    def test_the_parser_takes_a_non_finite_operand_as_it_takes_one_in_eq(self, operand: object) -> None:
        # The parser leaves a non-finite bound to the backend's caster, in `$eq` as in `$in`.
        QueryFilterExpressionParser.parse({"$values": {"x": {"$eq": Decimal("NaN")}}})

        parsed = QueryFilterExpressionParser.parse({"$values": {"x": {"$in": operand}}})  # type: ignore[dict-item]

        assert len(parsed.items[0].value) == 2  # type: ignore[union-attr, arg-type]
