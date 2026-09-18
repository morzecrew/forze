"""Tests for HTTP service specs."""

from enum import StrEnum

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


# ....................... #


class TestOperationResolution:
    """`HttpServiceSpec.operation`, which both adapters route their `invoke` through."""

    def _service(self) -> HttpServiceSpec:
        op = HttpOperationSpec(
            name="get",
            method="GET",
            path="/orders",
            args_type=None,
            return_type=_Out,
        )

        return HttpServiceSpec(name="orders", operations={"get": op})

    def test_a_key_resolves(self) -> None:
        service = self._service()

        assert service.operation("get") is service.operations["get"]

    def test_a_str_enum_key_resolves(self) -> None:
        class _Op(StrEnum):
            GET = "get"

        service = self._service()

        assert service.operation(_Op.GET) is service.operations["get"]

    def test_the_declared_spec_resolves_to_itself(self) -> None:
        service = self._service()
        declared = service.operations["get"]

        assert service.operation(declared) is declared

    def test_an_undeclared_key_is_refused(self) -> None:
        with pytest.raises(CoreException, match="Unknown HTTP operation 'missing'"):
            self._service().operation("missing")

    def test_a_spec_this_service_does_not_declare_is_refused(self) -> None:
        # Same name, different return_type: resolving by name alone would validate the
        # response against `_Args` and either fail confusingly or, on an overlapping
        # payload, succeed with the wrong model.
        foreign = HttpOperationSpec(
            name="get",
            method="GET",
            path="/orders",
            args_type=None,
            return_type=_Args,
        )

        with pytest.raises(CoreException, match="is not the one 'orders' declares"):
            self._service().operation(foreign)

    def test_an_equal_copy_of_the_declared_spec_resolves(self) -> None:
        # Equality, not identity: a spec rebuilt from the same declaration names the same
        # operation, and refusing it would make the typed form unusable from a second module.
        service = self._service()
        copy = HttpOperationSpec(
            name="get",
            method="GET",
            path="/orders",
            args_type=None,
            return_type=_Out,
        )

        assert service.operation(copy) is service.operations["get"]
