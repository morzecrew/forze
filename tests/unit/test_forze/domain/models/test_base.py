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


_SETS_SCRIPT = """
from forze.domain.models import BaseDTO
from forze.base.serialization.pydantic import pydantic_model_hash

class Tags(BaseDTO):
    s: set[str]
    f: frozenset[str]
    m: set[object]
    n: set[frozenset[str]]

words = {"alpha", "beta", "gamma", "delta", "echo", "foxtrot"}
tags = Tags(
    s=words,
    f=frozenset(words),
    m={1, "a", 2.5, "b"},
    n={frozenset({w}) for w in words},  # disjoint: never less than one another
)
print(tags.model_dump_json(), pydantic_model_hash(tags))
"""


def test_a_set_is_written_in_one_order_in_every_process() -> None:
    """A set, a frozenset, a set of mixed types and a set of sets dump to the same JSON and
    hash in any interpreter: string hashing is seeded per process, so iteration order is not
    an order, and neither is ``<`` between sets, which means "subset"."""

    import os
    import subprocess
    import sys

    outputs = {
        seed: subprocess.run(
            [sys.executable, "-c", _SETS_SCRIPT],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for seed in ("0", "1", "7", "12345")
    }

    assert len(set(outputs.values())) == 1, outputs


def test_a_set_of_one_type_is_written_sorted() -> None:
    class Numbers(BaseDTO):
        s: set[int]
        f: frozenset[str]
        pairs: set[tuple[int, int]]

    dumped = Numbers(
        s={10, 2, 1}, f=frozenset({"b", "a"}), pairs={(10, 1), (2, 1)}
    ).model_dump(mode="json")

    # Natural order, where canonical JSON text would put "[10,1]" first.
    assert dumped == {"s": [1, 2, 10], "f": ["a", "b"], "pairs": [[2, 1], [10, 1]]}


def test_a_model_hash_reads_a_set_in_its_one_order() -> None:
    from forze.base.serialization.pydantic import pydantic_model_hash

    class Tags(BaseDTO):
        s: set[str]

    assert pydantic_model_hash(Tags(s={"b", "a"})) == pydantic_model_hash(Tags(s={"a", "b"}))
    assert pydantic_model_hash(Tags(s={"a"})) != pydantic_model_hash(Tags(s={"b"}))


def test_a_set_with_nan_is_written_in_one_order() -> None:
    """NaN is not ordered, so a set holding one goes by canonical JSON text: a Decimal NaN
    would raise on comparison, and a float NaN has no place a sort can rely on."""

    from decimal import Decimal

    from forze.base.serialization import sorted_set_items

    decimals = {Decimal("NaN"), Decimal("1"), Decimal("0.5")}  # a signalling NaN cannot be hashed
    floats = {float("nan"), 1.0, 0.5}

    assert sorted_set_items(decimals) == sorted_set_items(set(reversed(list(decimals))))
    assert [str(x) for x in sorted_set_items(floats)] == [
        str(x) for x in sorted_set_items(set(reversed(list(floats))))
    ]
