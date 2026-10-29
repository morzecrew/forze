"""``enum_values``: Enum members as the values a store holds."""

from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

import pytest

from forze.base.exceptions import CoreException
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


class _Outer(Enum):
    MEMBER = _Plain.OPEN
    LIST = [_Plain.OPEN]  # noqa: RUF012 -- an enum member, not a class attribute


def test_a_member_whose_value_holds_a_member_becomes_the_inner_value() -> None:
    assert enum_values(_Outer.MEMBER) == "open"
    assert enum_values([_Outer.LIST]) == [["open"]]


def test_a_member_used_as_a_mapping_key_becomes_its_value() -> None:
    assert enum_values({_Plain.OPEN: [_Plain.OPEN], "k": 1}) == {"open": ["open"], "k": 1}
    assert type(next(iter(enum_values({_Text.OPEN: 1})))) is str


def test_two_keys_that_share_a_value_are_refused() -> None:
    """Last-wins, as a JSON encoder would do, would drop one entry with nothing said."""

    with pytest.raises(CoreException, match="share"):
        enum_values({_Plain.OPEN: 1, "open": 2})
