"""Tests for :class:`~forze.base.validators.NoneValidator`."""

from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import given, settings

from forze.base.validators import NoneValidator


@settings(max_examples=30, deadline=None)
@given(
    values=st.lists(
        st.one_of(st.none(), st.integers(), st.text(max_size=8)),
        min_size=0,
        max_size=8,
    ),
)
def test_exactly_one_property(values: list[object]) -> None:
    non_null = sum(1 for v in values if v is not None)
    assert NoneValidator.exactly_one(*values) == (non_null == 1)


@settings(max_examples=30, deadline=None)
@given(
    values=st.lists(
        st.one_of(st.none(), st.booleans()),
        min_size=0,
        max_size=8,
    ),
)
def test_at_least_one_property(values: list[object]) -> None:
    assert NoneValidator.at_least_one(*values) == any(v is not None for v in values)


def test_all_or_none_empty() -> None:
    assert NoneValidator.all_or_none() is True


def test_all_or_none_mixed_fails() -> None:
    assert NoneValidator.all_or_none(None, 1) is False
    assert NoneValidator.all_or_none(1, 2) is True
    assert NoneValidator.all_or_none(None, None) is True


def test_one_or_none_allows_all_none() -> None:
    assert NoneValidator.one_or_none(None, None) is True
    assert NoneValidator.one_or_none(1, None) is True
    assert NoneValidator.one_or_none(1, 2) is False
