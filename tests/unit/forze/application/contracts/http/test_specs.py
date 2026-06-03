"""Tests for HTTP service specs."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.http import HttpOperationSpec, HttpServiceSpec
from forze.base.exceptions import CoreException

# ----------------------- #


class _Args(BaseModel):
    order_id: str


class _Out(BaseModel):
    ok: bool


def test_path_placeholder_requires_args_type() -> None:
    with pytest.raises(CoreException):
        HttpOperationSpec(
            name="get",
            method="GET",
            path="/orders/{order_id}",
            args_type=None,
            return_type=_Out,
        )


def test_duplicate_operation_names_rejected() -> None:
    op = HttpOperationSpec(
        name="dup",
        method="GET",
        path="/",
        args_type=None,
        return_type=_Out,
    )

    with pytest.raises(CoreException):
        HttpServiceSpec(
            name="svc",
            operations={"a": op, "b": op},
        )


def test_path_placeholder_fields_validated() -> None:
    class _WrongArgs(BaseModel):
        other: str

    with pytest.raises(CoreException):
        HttpOperationSpec(
            name="get",
            method="GET",
            path="/orders/{order_id}",
            args_type=_WrongArgs,
            return_type=_Out,
        )
