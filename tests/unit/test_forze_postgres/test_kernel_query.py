"""Unit tests for forze_postgres.kernel.query."""

from datetime import date, datetime, timezone

import pytest

from forze.base.errors import CoreError
from forze_postgres.kernel.introspect import PostgresType
from forze_postgres.kernel.query.render import PsycopgValueCoercer

# ----------------------- #


class TestPsycopgValueCoercer:
    """Tests for PsycopgValueCoercer."""

    def test_scalar_none_returns_none(self) -> None:
        coercer = PsycopgValueCoercer()
        assert coercer.scalar(None, t=None) is None

    def test_scalar_uuid_type(self) -> None:
        from uuid import UUID

        from forze_postgres.kernel.introspect import PostgresType

        coercer = PsycopgValueCoercer()
        t = PostgresType(base="uuid", is_array=False, not_null=False)
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert coercer.scalar(str(u), t=t) == u

    def test_bool_flag(self) -> None:
        coercer = PsycopgValueCoercer()
        assert coercer.bool_flag(True) is True
        assert coercer.bool_flag("true") is True

    def test_scalar_pass_through_when_t_is_none(self) -> None:
        coercer = PsycopgValueCoercer()
        assert coercer.scalar("val", t=None) == "val"
        assert coercer.scalar(123, t=None) == 123

    def test_scalar_array_type_raises(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="text", is_array=True, not_null=False)
        with pytest.raises(CoreError, match="Array type not supported"):
            coercer.scalar("val", t=t)

    def test_scalar_text_types(self) -> None:
        coercer = PsycopgValueCoercer()
        for base in ["text", "varchar", "char", "citext"]:
            t = PostgresType(base=base, is_array=False, not_null=False)
            assert coercer.scalar(123, t=t) == "123"

    def test_scalar_bool_type(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="bool", is_array=False, not_null=False)
        assert coercer.scalar("true", t=t) is True
        assert coercer.scalar(0, t=t) is False

    def test_scalar_int_types(self) -> None:
        coercer = PsycopgValueCoercer()
        for base in ["int2", "int4", "int8"]:
            t = PostgresType(base=base, is_array=False, not_null=False)
            assert coercer.scalar("123", t=t) == 123
            assert coercer.scalar(456.0, t=t) == 456

    def test_scalar_float_types(self) -> None:
        coercer = PsycopgValueCoercer()
        for base in ["float4", "float8", "numeric"]:
            t = PostgresType(base=base, is_array=False, not_null=False)
            assert coercer.scalar("123.45", t=t) == 123.45
            assert coercer.scalar(456, t=t) == 456.0

    def test_scalar_date_type(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="date", is_array=False, not_null=False)
        assert coercer.scalar("2023-01-01", t=t) == date(2023, 1, 1)
        assert coercer.scalar(datetime(2023, 1, 1), t=t) == date(2023, 1, 1)

    def test_scalar_datetime_types(self) -> None:
        coercer = PsycopgValueCoercer()
        tz_type = PostgresType(base="timestamptz", is_array=False, not_null=False)
        dt_type = PostgresType(base="timestamp", is_array=False, not_null=False)

        # ISO string with Z
        assert coercer.scalar("2023-01-01T12:00:00Z", t=tz_type) == datetime(
            2023, 1, 1, 12, 0, tzinfo=timezone.utc
        )
        # force_tz=False strips tzinfo
        assert coercer.scalar("2023-01-01T12:00:00Z", t=dt_type) == datetime(
            2023, 1, 1, 12, 0
        )

    def test_scalar_unknown_type(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="unknown", is_array=False, not_null=False)
        assert coercer.scalar("val", t=t) == "val"

    def test_array_none_returns_empty_list(self) -> None:
        coercer = PsycopgValueCoercer()
        assert coercer.array(None, t=None) == []

    def test_array_scalar_value_raises(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="text", is_array=True, not_null=False)
        with pytest.raises(CoreError, match="Scalar value not supported"):
            coercer.array("not an array", t=t)
        with pytest.raises(CoreError, match="Scalar value not supported"):
            coercer.array(123, t=t)
        with pytest.raises(CoreError, match="Scalar value not supported"):
            coercer.array(True, t=t)

    def test_array_t_none_maps_scalar_t_none(self) -> None:
        coercer = PsycopgValueCoercer()
        assert coercer.array(["val", 123], t=None) == ["val", 123]

    def test_array_t_not_array_raise_on_scalar_t_true_raises(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="text", is_array=False, not_null=False)

        with pytest.raises(CoreError, match="Expected array column, got scalar"):
            coercer.array(["val"], t=t, raise_on_scalar_t=True)

    def test_array_t_not_array_raise_on_scalar_t_false_passes(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="text", is_array=False, not_null=False)
        assert coercer.array(["val"], t=t, raise_on_scalar_t=False) == ["val"]

    def test_array_valid_t_maps_scalar(self) -> None:
        coercer = PsycopgValueCoercer()
        t = PostgresType(base="int4", is_array=True, not_null=False)
        assert coercer.array(["123", 456.0], t=t) == [123, 456]
