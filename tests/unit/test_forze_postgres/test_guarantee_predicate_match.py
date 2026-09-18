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
