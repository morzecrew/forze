"""A guarantee's column is matched against an index predicate as an identifier.

`pg_get_expr` hands back deparsed SQL, and the question asked of it is which columns the index
restricts on. A substring test answers that wrongly in both directions that matter: it accepts
`(deleted_at IS NULL)` for a guarantee filtered on `deleted`, and it accepts a predicate that
only mentions the name inside a string literal. Both are silent acceptances of an index
restricted to the wrong rows — the failure the comparison was added to catch.

What it deliberately does not do is decide whether the predicate *means* the same as the
declared filter. That is expression equivalence, which is the database's judgement; this is a
floor under a predicate over the wrong column.
"""

from __future__ import annotations

import pytest

from forze_postgres.kernel.catalog.validation.validate_schema import (
    _conjuncts,  # pyright: ignore[reportPrivateUsage]
    _excludes_only_nulls,  # pyright: ignore[reportPrivateUsage]
    _forced_non_null,  # pyright: ignore[reportPrivateUsage]
    _null_exclusions,  # pyright: ignore[reportPrivateUsage]
    _predicate_names,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #


class TestAnIdentifierIsNotASubstring:
    @pytest.mark.parametrize(
        "predicate",
        [
            "(deleted_at IS NULL)",
            "(NOT undeleted)",
            "(deleted_by IS NULL)",
        ],
    )
    def test_a_longer_identifier_containing_it_does_not_count(self, predicate: str) -> None:
        assert not _predicate_names(predicate, "deleted")

    @pytest.mark.parametrize(
        "predicate",
        [
            "((label)::text <> 'deleted'::text)",
            "((label)::text = 'deleted''s'::text)",
        ],
    )
    def test_a_string_literal_holding_the_name_does_not_count(self, predicate: str) -> None:
        # A predicate comparing *against* the word is not a predicate restricting *on* the
        # column, and taking it for one accepts an index over unrelated rows.
        assert not _predicate_names(predicate, "deleted")

    @pytest.mark.parametrize(
        "predicate",
        [
            "(deleted IS FALSE)",
            "(NOT deleted)",
            "((NOT deleted) AND (tenant_id IS NOT NULL))",
            "(deleted = false)",
        ],
    )
    def test_the_column_itself_counts_however_it_is_spelled(self, predicate: str) -> None:
        # The contrast that keeps this from being a check that refuses everything: Postgres
        # deparses the same condition several ways and every one of them names the column.
        assert _predicate_names(predicate, "deleted")

    def test_a_quoted_identifier_counts(self) -> None:
        assert _predicate_names('("is_current" IS TRUE)', "is_current")


# ....................... #


class TestAFilterThatRulesOutNulls:
    """The fields a guarantee's `where` holds non-null, which then need no NULLS NOT DISTINCT."""

    @pytest.mark.parametrize(
        "where",
        [
            {"$values": {"code": {"$null": False}}},
            {"$values": {"code": "a"}},
            {"$values": {"code": {"$in": ["a", "b"]}}},
            {"$values": {"code": {"$gte": "a"}}},
            {"$and": [{"$values": {"is_current": True}}, {"$values": {"code": {"$lt": "z"}}}]},
        ],
    )
    def test_a_conjunct_no_null_satisfies_counts(self, where: dict[str, object]) -> None:
        assert "code" in _forced_non_null(where)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "where",
        [
            {"$values": {"code": {"$null": True}}},
            {"$values": {"code": {"$neq": "a"}}},
            {"$values": {"code": {"$in": ["a", None]}}},
            {"$or": [{"$values": {"code": "a"}}, {"$values": {"is_current": True}}]},
            {"$not": {"$values": {"code": {"$null": True}}}},
        ],
    )
    def test_anything_a_null_can_pass_does_not(self, where: dict[str, object]) -> None:
        # A field this cannot prove non-null keeps the stricter reading: refusing a correct
        # index is a migration to explain, accepting a wrong one is a parity break.
        assert "code" not in _forced_non_null(where)  # type: ignore[arg-type]

    def test_no_filter_rules_out_nothing(self) -> None:
        assert _forced_non_null(None) == frozenset()


# ....................... #

TUPLE = frozenset({"parent_id", "code"})


class TestTheNullExclusionIsReadStructurally:
    """An unfiltered `skip_null` admits one predicate shape: `IS NOT NULL` conjuncts over the
    tuple, covering each nullable field. Naming the field is not enough."""

    @pytest.mark.parametrize(
        "predicate",
        [
            "(code IS NOT NULL)",
            "((parent_id IS NOT NULL) AND (code IS NOT NULL))",
            '("code" IS NOT NULL)',
        ],
    )
    def test_the_null_exclusion_counts(self, predicate: str) -> None:
        assert _excludes_only_nulls(predicate, TUPLE, ["code"])

    @pytest.mark.parametrize(
        "predicate",
        [
            "(code = 'x'::text)",
            "(code IS NULL)",
            "((code IS NOT NULL) OR is_current)",
            "((code IS NOT NULL) AND is_current)",
            "(NOT (code IS NULL))",
            "((code IS NOT NULL)",
            "(parent_id IS NOT NULL)",
            "(other IS NOT NULL)",
        ],
    )
    def test_anything_else_does_not(self, predicate: str) -> None:
        assert not _excludes_only_nulls(predicate, TUPLE, ["code"])

    def test_the_exclusions_are_read_beside_other_conjuncts(self) -> None:
        # A filtered guarantee's predicate carries its own conditions too.
        predicate = "(is_current AND (code IS NOT NULL) AND (label <> ' AND x IS NOT NULL'))"

        assert _null_exclusions(predicate) == frozenset({"code"})
        assert _null_exclusions("((code IS NULL) OR (code IS NOT NULL))") == frozenset()
        assert _null_exclusions(None) == frozenset()


class TestTheConjunctSplit:
    def test_top_level_operands_come_back_unwrapped(self) -> None:
        assert _conjuncts("((a IS NOT NULL) AND (b = 1))") == ["a IS NOT NULL", "b = 1"]

    @pytest.mark.parametrize("predicate", ["a AND (b IS NOT NULL", "(a)) AND ((b"])
    def test_an_unbalanced_predicate_is_declined(self, predicate: str) -> None:
        assert _conjuncts(predicate) is None
