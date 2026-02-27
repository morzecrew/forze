"""Unit tests for forze_postgres.kernel.query."""

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
