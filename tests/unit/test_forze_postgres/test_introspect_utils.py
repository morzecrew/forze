"""Unit tests for :mod:`forze_postgres.kernel.introspect.utils`."""

import pytest

from forze_postgres.kernel.introspect.utils import (
    extract_index_expr_from_indexdef,
    normalize_pg_type,
)


class TestNormalizePgType:
    """Tests for :func:`normalize_pg_type`."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("timestamp with time zone", "timestamptz"),
            ("timestamp without time zone", "timestamp"),
            ("character varying(255)", "varchar"),
            ("character varying", "varchar"),
            ("character", "char"),
            ("double precision", "float8"),
            ("real", "float4"),
            ("smallint", "int2"),
            ("integer", "int4"),
            ("bigint", "int8"),
            ("boolean", "bool"),
            ("  TEXT  ", "text"),
            ("custom_enum", "custom_enum"),
        ],
    )
    def test_known_mappings(self, raw: str, expected: str) -> None:
        assert normalize_pg_type(raw) == expected

    def test_lru_cache_same_result(self) -> None:
        """Repeated calls return the same string object (cache hit)."""
        a = normalize_pg_type("integer")
        b = normalize_pg_type("integer")
        assert a == b == "int4"


class TestExtractIndexExprFromIndexdef:
    """Tests for :func:`extract_index_expr_from_indexdef`."""

    def test_extracts_expression_inside_using_gin(self) -> None:
        """Typical GIN index definition yields the indexed expression."""
        indexdef = (
            "CREATE INDEX idx ON public.docs USING gin (to_tsvector('english', body))"
        )
        expr = extract_index_expr_from_indexdef(indexdef)
        assert expr == "to_tsvector('english', body)"

    def test_no_match_returns_none(self) -> None:
        """Malformed or unexpected definitions return None."""
        assert extract_index_expr_from_indexdef("CREATE TABLE x (a int)") is None

    def test_empty_inner_expression_returns_none(self) -> None:
        """Empty capture group becomes None."""
        # Unlikely real DDL, but exercises ``expr or None``.
        assert extract_index_expr_from_indexdef("CREATE INDEX i ON t USING btree ()") is None
