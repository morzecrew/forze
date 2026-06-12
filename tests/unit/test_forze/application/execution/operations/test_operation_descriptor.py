"""Tests for operation catalog descriptors and the frozen-registry catalog view."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.execution.operations import (
    OperationDescriptor,
    OperationKind,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace

# ----------------------- #


class _In(BaseModel):
    x: int


class _Out(BaseModel):
    y: str


# ....................... #


class TestOperationDescriptor:
    def test_schemas_for_typed_descriptor(self) -> None:
        d = OperationDescriptor(input_type=_In, output_type=_Out, description="d")

        assert d.input_schema() is not None
        assert "x" in (d.input_schema() or {}).get("properties", {})
        assert "y" in (d.output_schema() or {}).get("properties", {})

    def test_schemas_none_when_types_absent(self) -> None:
        d = OperationDescriptor(description="void op")

        assert d.input_schema() is None
        assert d.output_schema() is None


# ....................... #


class TestRegistryDescriptorMutations:
    def test_set_descriptor_conflict_without_override(self) -> None:
        reg = OperationRegistry(handlers={"op": lambda _c: None}).set_descriptor(
            "op", OperationDescriptor(description="a")
        )

        with pytest.raises(exc, match="already set"):
            reg.set_descriptor("op", OperationDescriptor(description="b"))

    def test_set_descriptor_with_override(self) -> None:
        reg = (
            OperationRegistry(handlers={"op": lambda _c: None})
            .set_descriptor("op", OperationDescriptor(description="a"))
            .set_descriptor("op", OperationDescriptor(description="b"), override=True)
        )

        assert reg.get_descriptors()["op"].description == "b"

    def test_set_descriptors_batch_with_namespace(self) -> None:
        ns = StrKeyNamespace(prefix="docs")
        reg = OperationRegistry().set_descriptors(
            {"get": OperationDescriptor(description="g")},
            namespace=ns,
        )

        assert ns.key("get") in reg.get_descriptors()


# ....................... #


class TestFrozenCatalog:
    def _registry(self) -> OperationRegistry:
        reg = OperationRegistry(
            handlers={"read": lambda _c: None, "write": lambda _c: None}
        )
        reg = reg.set_descriptor(
            "read", OperationDescriptor(input_type=_In, output_type=_Out, description="r")
        )
        return reg.bind("read").as_query().finish()

    def test_freeze_carries_descriptors(self) -> None:
        frozen = self._registry().freeze()

        assert "read" in frozen.descriptors
        assert frozen.descriptors["read"].description == "r"

    def test_catalog_joins_kind_and_descriptor(self) -> None:
        cat = self._registry().freeze().catalog()

        assert cat["read"].kind is OperationKind.QUERY
        assert cat["read"].is_read_only is True
        assert cat["read"].descriptor is not None

    def test_catalog_includes_ops_without_descriptor(self) -> None:
        cat = self._registry().freeze().catalog()

        # 'write' has no descriptor and defaults to COMMAND.
        assert cat["write"].descriptor is None
        assert cat["write"].kind is OperationKind.COMMAND
        assert cat["write"].is_read_only is False


# ....................... #


class TestDescriptorMerge:
    def test_merge_combines_descriptors(self) -> None:
        a = OperationRegistry(handlers={"a": lambda _c: None}).set_descriptor(
            "a", OperationDescriptor(description="a")
        )
        b = OperationRegistry(handlers={"b": lambda _c: None}).set_descriptor(
            "b", OperationDescriptor(description="b")
        )

        merged = OperationRegistry.merge(a, b)

        assert set(merged.get_descriptors()) == {"a", "b"}

    def test_merge_conflicting_descriptors_raises_naming_keys(self) -> None:
        a = OperationRegistry(handlers={"a": lambda _c: None}).set_descriptor(
            "a", OperationDescriptor(description="a")
        )
        b = OperationRegistry(handlers={"a2": lambda _c: None}).set_descriptor(
            "a", OperationDescriptor(description="b")
        )

        with pytest.raises(exc, match=r"duplicate operation descriptors.*'a'"):
            OperationRegistry.merge(a, b)

    def test_merge_conflicting_descriptors_override_last_wins(self) -> None:
        a = OperationRegistry(handlers={"a": lambda _c: None}).set_descriptor(
            "a", OperationDescriptor(description="a")
        )
        b = OperationRegistry(handlers={"a2": lambda _c: None}).set_descriptor(
            "a", OperationDescriptor(description="b")
        )

        merged = OperationRegistry.merge(a, b, override=True)

        assert merged.get_descriptors()["a"].description == "b"
