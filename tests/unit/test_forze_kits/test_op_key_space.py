"""Tests for :class:`~forze.base.primitives.StrKeyNamespace`."""

import pytest

from forze.base.exceptions import CoreException
from forze.base.primitives import StrKeyNamespace


def test_key_joins_prefix_and_segment() -> None:
    s = StrKeyNamespace(prefix="document")
    assert s.key("get") == "document.get"


def test_key_accepts_str_enum_value() -> None:
    from enum import StrEnum

    class K(StrEnum):
        X = "x"

    s = StrKeyNamespace(prefix="bc")
    assert s.key(K.X) == "bc.x"


def test_segment_with_dot_raises() -> None:
    s = StrKeyNamespace(prefix="document")
    with pytest.raises(CoreException, match=r"must not contain"):
        s.key("bad.kernel")


def test_empty_prefix_raises() -> None:
    with pytest.raises(CoreException, match="must be non-empty"):
        StrKeyNamespace(prefix="")


def test_multi_segment_key_builds_dotted_operation_id() -> None:
    s = StrKeyNamespace(prefix="orders")
    assert s.key("document", "create") == "orders.document.create"
