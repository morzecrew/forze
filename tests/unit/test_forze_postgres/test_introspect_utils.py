"""Unit tests for :mod:`forze_postgres.kernel.catalog.introspect.utils`."""

import pytest

from forze_postgres.kernel.catalog.introspect.utils import (
    extract_index_expr_from_indexdef,
    index_expr_uses_to_tsvector,
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

    @pytest.mark.parametrize(
        ("indexdef", "expected"),
        [
            # Regression: trailing clauses must not be swallowed by a greedy
            # capture that runs to the last ')'. Balanced-paren extraction
            # stops at the matching close of the USING (...) group.
            (
                "CREATE INDEX i ON t USING gin (tsv_col) WITH (fastupdate = off)",
                "tsv_col",
            ),
            (
                "CREATE INDEX i ON t USING pgroonga (name) "
                "WITH (tokenizer = 'TokenBigram')",
                "name",
            ),
            (
                "CREATE INDEX i ON t USING gin "
                "(to_tsvector('english', body)) WHERE active",
                "to_tsvector('english', body)",
            ),
            ("CREATE INDEX i ON t USING btree (a) INCLUDE (b)", "a"),
            # A ')' inside a string literal must not close the group early.
            (
                "CREATE INDEX i ON t USING gin (to_tsvector('english', label)) "
                "WITH (tokenizer = 'a)b')",
                "to_tsvector('english', label)",
            ),
        ],
    )
    def test_balanced_extraction_ignores_trailing_clauses(
        self, indexdef: str, expected: str
    ) -> None:
        assert extract_index_expr_from_indexdef(indexdef) == expected

    def test_unbalanced_parens_returns_none(self) -> None:
        """A definition whose USING group never closes yields None."""
        assert (
            extract_index_expr_from_indexdef("CREATE INDEX i ON t USING gin (a, b")
            is None
        )


class TestIndexExprUsesToTsvector:
    """Tests for :func:`index_expr_uses_to_tsvector` (GIN -> FTS classification)."""

    @pytest.mark.parametrize(
        "expr",
        [
            "to_tsvector('english'::regconfig, (title || ' ' || body))",
            "TO_TSVECTOR('english', body)",
            "to_tsvector ('simple', col)",  # whitespace before paren
        ],
    )
    def test_detects_to_tsvector_call(self, expr: str) -> None:
        assert index_expr_uses_to_tsvector(expr) is True

    @pytest.mark.parametrize(
        "expr",
        [
            None,
            # Bare "tsvector" substring must NOT classify as FTS: a JSON key,
            # a column name, or a literal default that merely mentions the word.
            "(data ->> 'tsvector'::text)",
            "COALESCE(tsvector_meta, ''::text)",
            "(my_tsvector_col)",
            "to_tsvector",  # name without a call paren
        ],
    )
    def test_does_not_misfire_on_substring(self, expr: str | None) -> None:
        assert index_expr_uses_to_tsvector(expr) is False
