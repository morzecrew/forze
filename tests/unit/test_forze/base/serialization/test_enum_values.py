"""``enum_values``: Enum members as the values a store holds."""

from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from forze.base.serialization import enum_values


class _Plain(Enum):
    OPEN = "open"
    NONE = None


class _Text(StrEnum):
    OPEN = "open"


class _Number(IntEnum):
    ONE = 1


def test_a_member_becomes_its_value_at_any_depth() -> None:
    out = enum_values({"s": _Plain.OPEN, "l": [_Plain.OPEN, 1], "t": (_Plain.OPEN,), "n": None})

    assert out == {"s": "open", "l": ["open", 1], "t": ("open",), "n": None}
    assert enum_values(_Plain.NONE) is None


def test_a_strenum_or_intenum_member_becomes_the_same_text_or_number() -> None:
    assert type(enum_values(_Text.OPEN)) is str and enum_values(_Text.OPEN) == str(_Text.OPEN)
    assert type(enum_values(_Number.ONE)) is int and enum_values(_Number.ONE) == 1


def test_a_container_holding_no_member_is_returned_as_it_is() -> None:
    for value in (("a", 1), ["a"], {"k": ["a"]}):
        assert enum_values(value) is value
