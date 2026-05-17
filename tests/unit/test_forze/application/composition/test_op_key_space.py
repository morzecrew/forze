"""Tests for :class:`~forze.application.execution.OperationNamespace`."""

import pytest

from forze.application.execution import OperationNamespace
from forze.base.errors import CoreError


def test_op_joins_prefix_and_kernel() -> None:
    s = OperationNamespace(prefix="document")
    assert s.op("get") == "document.get"


def test_op_accepts_str_enum_value() -> None:
    from enum import StrEnum

    class K(StrEnum):
        X = "x"

    s = OperationNamespace(prefix="bc")
    assert s.op(K.X) == "bc.x"


def test_kernel_with_dot_raises() -> None:
    s = OperationNamespace(prefix="document")
    with pytest.raises(CoreError, match=r"must not contain"):
        s.op("bad.kernel")


def test_empty_prefix_raises() -> None:
    with pytest.raises(CoreError, match="prefix must be non-empty"):
        OperationNamespace(prefix="   ")


def test_prefix_strips_whitespace_and_outer_dots() -> None:
    s = OperationNamespace(prefix="  orders.document.  ")
    assert s.prefix == "orders.document"
    assert s.op("create") == "orders.document.create"
