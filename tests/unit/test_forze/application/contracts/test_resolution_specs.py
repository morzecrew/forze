"""Tests for :mod:`forze.application.contracts.resolution.specs`."""

from __future__ import annotations

import pytest

from forze.application.contracts.resolution.specs import (
    coerce_named_resource_spec,
    coerce_relation_spec,
    is_static_relation,
    require_static_named_resource,
    require_static_relation,
)
from forze.base.exceptions import exc


class TestCoerceRelationSpec:
    def test_accepts_tuple(self) -> None:
        assert coerce_relation_spec(("ns", "name")) == ("ns", "name")

    def test_accepts_callable(self) -> None:
        def resolver(_tenant_id):
            return ("ns", "name")

        assert coerce_relation_spec(resolver) is resolver

    def test_rejects_invalid(self) -> None:
        with pytest.raises(exc, match="Relation must"):
            coerce_relation_spec("bad")


class TestCoerceNamedResourceSpec:
    def test_accepts_str(self) -> None:
        assert coerce_named_resource_spec("bucket") == "bucket"

    def test_accepts_enum_value(self) -> None:
        class _E:
            value = "idx"

        assert coerce_named_resource_spec(_E()) == "idx"

    def test_rejects_invalid(self) -> None:
        with pytest.raises(exc, match="Named resource must"):
            coerce_named_resource_spec(42)


class TestStaticGuards:
    def test_is_static_relation(self) -> None:
        assert is_static_relation(("a", "b"))
        assert not is_static_relation(lambda _: ("a", "b"))

    def test_require_static_relation(self) -> None:
        assert require_static_relation(("db", "tbl"), route_name="r", field="write") == (
            "db",
            "tbl",
        )

        with pytest.raises(exc, match="dynamic RelationSpec"):
            require_static_relation(
                lambda _: ("db", "tbl"),
                route_name="r",
                field="write",
            )

    def test_require_static_named_resource(self) -> None:
        assert require_static_named_resource("idx", route_name="r", field="index") == "idx"

        with pytest.raises(exc, match="dynamic NamedResourceSpec"):
            require_static_named_resource(
                lambda _: "idx",
                route_name="r",
                field="index",
            )
