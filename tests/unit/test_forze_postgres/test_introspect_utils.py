"""Unit tests for :mod:`forze_postgres.kernel.catalog.introspect.utils`."""

import pytest

from forze_postgres.kernel.catalog.introspect.utils import (
    extract_index_expr_from_indexdef,
    find_balanced_span,
    index_expr_uses_to_tsvector,
    mask_sql_literals,
    normalize_pg_type,
)


class TestMaskSqlLiterals:
    """Tests for :func:`mask_sql_literals` (length-preserving literal blanking)."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("f(a, 'x,y') + b", "f(a, xxxxx) + b"),
            # Doubled '' escape stays inside one literal.
            ("'a''b'", "xxxxxx"),
            # Double-quoted identifier (with structural chars) is blanked.
            ('f("a,b)c", x)', 'f(xxxxxxx, x)'),
            ('"a""b"', "xxxxxx"),
            # Dollar-quoted body (with structural chars) is blanked.
            ("f($$a,(b)$$, c)", "f(" + "x" * 9 + ", c)"),
            ("f($tag$x)y$tag$)", "f(" + "x" * 13 + ")"),
            # A lone '$' (positional param) is not a literal.
            ("$1 + col", "$1 + col"),
        ],
    )
    def test_masks_literals_preserving_length(
        self, text: str, expected: str
    ) -> None:
        masked = mask_sql_literals(text)
        assert masked == expected
        assert len(masked) == len(text)


class TestFindBalancedSpan:
    """Tests for the shared :func:`find_balanced_span` delimiter matcher."""

    @pytest.mark.parametrize(
        ("text", "open_idx", "expected_inner"),
        [
            ("(a, b)", 0, "a, b"),
            ("[a, b]", 0, "a, b"),
            # Nested () and [] are tracked as one depth counter.
            ("(f(x), g[1])", 0, "f(x), g[1]"),
            # A delimiter inside a single-quoted literal does not close early.
            ("(coalesce(c, ')'))", 0, "coalesce(c, ')')"),
            # Doubled '' is an escaped quote, not a literal boundary.
            ("('a''b)c')", 0, "'a''b)c'"),
            # Opener not at index 0.
            ("ARRAY[x, y]", 5, "x, y"),
            # Parens inside a dollar-quoted body do not affect depth.
            ("(f($$a (b) c$$))", 0, "f($$a (b) c$$)"),
            ("(f($tag$x)y$tag$))", 0, "f($tag$x)y$tag$)"),
            # A ')' inside a double-quoted identifier does not close early.
            ('("a)b")', 0, '"a)b"'),
        ],
    )
    def test_matches_balanced_group(
        self, text: str, open_idx: int, expected_inner: str
    ) -> None:
        close = find_balanced_span(text, open_idx)
        assert close is not None
        assert text[open_idx + 1 : close] == expected_inner

    def test_unbalanced_returns_none(self) -> None:
        assert find_balanced_span("(a, b", 0) is None


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
            # Parens inside a dollar-quoted literal must not close early either.
            (
                "CREATE INDEX i ON t USING gin (f(col, $$a (b)$$)) WITH (x = 1)",
                "f(col, $$a (b)$$)",
            ),
            # A ')' inside a double-quoted identifier must not close early.
            (
                'CREATE INDEX i ON t USING gin ("a)b") WITH (fastupdate = off)',
                '"a)b"',
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

    def test_word_boundary_ignores_using_inside_identifier(self) -> None:
        """``using`` embedded in an identifier must not start the match.

        The word boundary anchors on the ``USING`` keyword. Without it, a token
        ending in ``using`` immediately followed by ``<word>(`` (here the
        crafted ``reusing data(...)``) hijacks extraction onto the wrong group;
        with it, the real ``USING gin (...)`` clause is matched.
        """
        indexdef = "CREATE INDEX i ON reusing data(col) USING gin (real_expr)"
        assert extract_index_expr_from_indexdef(indexdef) == "real_expr"


class TestIndexExprUsesToTsvector:
    """Tests for :func:`index_expr_uses_to_tsvector` (GIN -> FTS classification)."""

    @pytest.mark.parametrize(
        "expr",
        [
            "to_tsvector('english'::regconfig, (title || ' ' || body))",
            "TO_TSVECTOR('english', body)",
            "to_tsvector ('simple', col)",  # whitespace before paren
            # Weighted FTS nests the call -- a *contains* check still detects it.
            "setweight(to_tsvector('english', title), 'A') || "
            "setweight(to_tsvector('english', body), 'B')",
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
            # A to_tsvector( call inside a quoted literal/JSON key is not FTS.
            "(data ->> 'to_tsvector(x)'::text)",
            "COALESCE(notes, 'to_tsvector(x)'::text)",
        ],
    )
    def test_does_not_misfire_on_substring(self, expr: str | None) -> None:
        assert index_expr_uses_to_tsvector(expr) is False
