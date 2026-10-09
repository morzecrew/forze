import pytest

from forze.domain.models import BaseDTO, CoreModel


def test_core_model_uses_common_config() -> None:
    class Sample(CoreModel):
        value: int

    s = Sample(value=1)
    dumped = s.model_dump()
    # config uses attribute docstrings and stable encoders; basic behavior should work
    assert dumped == {"value": 1}

def test_base_dto_is_frozen() -> None:
    class SampleDTO(BaseDTO):
        value: int

    dto = SampleDTO(value=1)
    assert dto.value == 1

    # frozen DTO should not allow attribute reassignment and must raise ValidationError
    with pytest.raises(Exception):
        dto.value = 2  # type: ignore[misc]


def test_a_decimal_is_written_in_fixed_point_in_json() -> None:
    from decimal import Decimal

    from pydantic import Field

    class Amounts(BaseDTO):
        zero: Decimal
        many: list[Decimal]
        maybe: Decimal | None = None
        odd: Decimal = Field(default=Decimal("NaN"), allow_inf_nan=True)

    dto = Amounts(
        zero=Decimal("0.00000000"),
        many=[Decimal("1E+3"), Decimal("-0E-8"), Decimal("1.5")],
        maybe=Decimal("2.5E-7"),
    )
    expected = {
        "zero": "0.00000000",
        "many": ["1000", "-0.00000000", "1.5"],
        "maybe": "0.00000025",
        "odd": "NaN",
    }

    assert dto.model_dump(mode="json") == expected
    assert Amounts.model_validate_json(dto.model_dump_json()).zero == dto.zero
    # Python mode keeps the Decimal itself, for the stores.
    assert dto.model_dump()["zero"] == Decimal("0.00000000")


def test_a_decimal_with_a_huge_exponent_stays_scientific() -> None:
    from decimal import Decimal

    class Amount(BaseDTO):
        value: Decimal

    # Written out, these would run to a billion characters.
    for text in ("1E+999999999", "1E-999999999", "1E+101", "1E-101"):
        assert Amount(value=Decimal(text)).model_dump(mode="json") == {"value": text}

    # An exponent of 100 either way is still written out.
    assert Amount(value=Decimal("1E+100")).model_dump(mode="json") == {"value": "1" + "0" * 100}
    assert Amount(value=Decimal("1E-100")).model_dump(mode="json") == {"value": "0." + "0" * 99 + "1"}
