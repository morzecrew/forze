"""SystemInvariant contract — the value ``holds`` receives, and the paired-law declaration."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.invariants import (
    CountAll,
    Measure,
    ReadSet,
    SumOf,
    SystemInvariant,
    aggregate_value,
)
from forze.base.exceptions import exc
from forze.domain.models import ReadDocument

# ----------------------- #


class _Money(BaseModel):
    amount: Decimal


class _Read(ReadDocument):
    group: str = "g"
    price: Decimal = Decimal(0)
    maybe_price: Decimal | None = None
    quantity: int = 0
    weight: float = 0.0
    money: _Money | None = None


SPEC = DocumentSpec(name="contract_invariants", read=_Read)
READ_SET = ReadSet(spec=SPEC, scope_keys=("group",))


# ----------------------- #


class TestAggregateValue:
    def test_a_count_is_an_int(self) -> None:
        value = aggregate_value(READ_SET, CountAll(), 3)

        assert value == 3
        assert type(value) is int

    def test_an_empty_count_is_zero(self) -> None:
        assert aggregate_value(READ_SET, CountAll(), None) == 0

    def test_a_decimal_field_sums_to_an_exact_decimal(self) -> None:
        value = aggregate_value(READ_SET, SumOf("price"), Decimal("0.3"))

        assert value == Decimal("0.3")
        assert isinstance(value, Decimal)

    @pytest.mark.parametrize("field", ["price", "maybe_price", "money.amount"])
    def test_an_int_or_empty_total_of_a_decimal_field_stays_a_decimal(self, field: str) -> None:
        # Postgres sums no rows to NULL and an int column to an int; neither may become a float.
        assert aggregate_value(READ_SET, SumOf(field), None) == Decimal(0)
        assert isinstance(aggregate_value(READ_SET, SumOf(field), 7), Decimal)

    @pytest.mark.parametrize("field", ["quantity", "weight", "not_declared"])
    def test_any_other_field_sums_to_a_float_as_before(self, field: str) -> None:
        # A bigint SUM arrives from Postgres as a Decimal; an int field's law still sees a float.
        value = aggregate_value(READ_SET, SumOf(field), Decimal("30"))

        assert value == 30.0
        assert type(value) is float


class TestPairedDeclaration:
    def test_scope_keys_of_different_length_are_refused(self) -> None:
        with pytest.raises(exc, match="pair by position") as caught:
            SystemInvariant(
                name="mismatch",
                read_set=READ_SET,
                aggregate=CountAll(),
                against=Measure(read_set=ReadSet(spec=SPEC), aggregate=CountAll()),
                holds=lambda own, other: own <= other,
            )

        assert caught.value.code == "system_invariant_scope_mismatch"

    def test_holds_for_passes_both_sides_to_a_paired_law(self) -> None:
        law = SystemInvariant(
            name="bounded",
            read_set=READ_SET,
            aggregate=SumOf("price"),
            against=Measure(read_set=READ_SET, aggregate=SumOf("price")),
            holds=lambda own, other: own <= other,
        )

        assert law.holds_for(Decimal("0.3"), Decimal("0.3"))
        assert not law.holds_for(Decimal("0.4"), Decimal("0.3"))

    def test_holds_for_passes_one_value_to_a_single_law(self) -> None:
        law = SystemInvariant(
            name="single",
            read_set=READ_SET,
            aggregate=CountAll(),
            holds=lambda n: n <= 1,
        )

        assert law.holds_for(1)
        assert not law.holds_for(2)
