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


def test_operation_key_name_mismatch_rejected() -> None:
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
            operations={"a": op},
        )


def test_query_from_validated_for_non_get() -> None:
    class _Body(BaseModel):
        name: str

    with pytest.raises(CoreException):
        HttpOperationSpec(
            name="create",
            method="POST",
            path="/items",
            args_type=_Body,
            return_type=_Out,
            query_from=frozenset({"unknown_field"}),
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
