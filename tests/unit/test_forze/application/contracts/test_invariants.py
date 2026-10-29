"""SystemInvariant contract — the value ``holds`` receives, and the paired-law declaration."""

from __future__ import annotations

import inspect
from datetime import date, datetime
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Annotated, Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel, Field

from forze.application.contracts import invariants
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
    capped_price: Annotated[Decimal, Field(ge=0)] | None = None
    quantity: int = 0
    weight: float = 0.0
    money: _Money | None = None


SPEC = DocumentSpec(name="contract_invariants", read=_Read)
READ_SET = ReadSet(spec=SPEC, scope_keys=("group",))


class _PlainStr(Enum):
    OPEN = "open"


class _PlainInt(Enum):
    ONE = 1


class _Str(StrEnum):
    OPEN = "open"


class _Int(IntEnum):
    ONE = 1


class _Keys(ReadDocument):
    """One scope field of each type a paired law can be keyed by."""

    ref: str = ""
    number: int = 0
    plain_str: _PlainStr = _PlainStr.OPEN
    plain_int: _PlainInt = _PlainInt.ONE
    str_enum: _Str = _Str.OPEN
    int_enum: _Int = _Int.ONE
    day: date = date(2026, 1, 1)
    at: datetime = datetime(2026, 1, 1)
    anything: Any = None


KEYS = DocumentSpec(name="contract_invariant_keys", read=_Keys)


def _pairing(own: str, other: str) -> SystemInvariant:
    return SystemInvariant(
        name=f"{own}_against_{other}",
        read_set=ReadSet(spec=KEYS, scope_keys=(own,)),
        aggregate=CountAll(),
        against=Measure(read_set=ReadSet(spec=KEYS, scope_keys=(other,)), aggregate=CountAll()),
        holds=lambda mine, theirs: mine <= theirs,
    )


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

    @pytest.mark.parametrize("field", ["price", "maybe_price", "capped_price", "money.amount"])
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

    @pytest.mark.parametrize(
        ("against", "holds"),
        [
            (None, lambda own, other: own <= other),  # one side, two arguments
            (Measure(read_set=READ_SET, aggregate=CountAll()), lambda n: n <= 1),  # paired, one
        ],
    )
    def test_a_holds_that_cannot_take_its_arguments_is_refused(
        self, against: Measure | None, holds: Any
    ) -> None:
        with pytest.raises(exc) as caught:
            SystemInvariant(
                name="arity", read_set=READ_SET, aggregate=CountAll(), against=against, holds=holds
            )

        assert caught.value.code == "system_invariant_arity_mismatch"

    def test_a_holds_without_an_inspectable_signature_is_accepted(self) -> None:
        with pytest.raises(ValueError):
            inspect.signature(max)

        law = SystemInvariant(name="builtin", read_set=READ_SET, aggregate=CountAll(), holds=max)

        assert law.holds is max

    @pytest.mark.parametrize(
        ("own", "other"),
        [
            ("ref", "id"),  # a UUID by its string
            ("ref", "str_enum"),
            ("ref", "plain_str"),  # a plain Enum by its value
            ("number", "int_enum"),
            ("plain_int", "plain_int"),
            ("day", "day"),
            ("ref", "anything"),  # an undeclared type is not second-guessed
        ],
    )
    def test_scope_fields_matched_in_one_form_pair(self, own: str, other: str) -> None:
        _pairing(own, other)
        _pairing(other, own)

    @pytest.mark.parametrize(
        ("own", "other"),
        [
            ("number", "ref"),
            ("number", "id"),
            ("plain_int", "ref"),  # 1 never meets "1": no evaluator would pair them
            ("int_enum", "ref"),
            ("plain_int", "number"),  # Postgres compares a plain Enum's JSON path as text
            ("plain_int", "id"),
            ("day", "at"),  # date(2026, 1, 1) != datetime(2026, 1, 1)
        ],
    )
    def test_scope_fields_that_never_meet_are_refused(self, own: str, other: str) -> None:
        for pair in ((own, other), (other, own)):
            with pytest.raises(exc) as caught:
                _pairing(*pair)

            assert caught.value.code == "system_invariant_scope_type_mismatch"

    def test_summing_a_path_the_read_model_does_not_declare_warns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        spy = MagicMock()
        monkeypatch.setattr(invariants, "logger", spy)

        SystemInvariant(
            name="undeclared",
            read_set=READ_SET,
            aggregate=SumOf("money.cents"),
            holds=lambda total: total >= 0,
        )

        spy.warning.assert_called_once()
        assert "money.cents" in spy.warning.call_args.args

    def test_holds_for_passes_one_value_to_a_single_law(self) -> None:
        law = SystemInvariant(
            name="single",
            read_set=READ_SET,
            aggregate=CountAll(),
            holds=lambda n: n <= 1,
        )

        assert law.holds_for(1)
        assert not law.holds_for(2)
