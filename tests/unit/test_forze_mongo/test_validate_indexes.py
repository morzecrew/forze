"""Unit tests for Mongo document index validation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.application.contracts.guarantees import UniqueTogether
from forze.base.exceptions import CoreException
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.introspect import MongoIndexInfo, MongoIntrospector
from forze_mongo.kernel.validate_indexes import (
    MongoDocumentIndexSpec,
    _equalities,  # pyright: ignore[reportPrivateUsage]
    _guarantee_equalities,  # pyright: ignore[reportPrivateUsage]
    _require_guarantee_indexes,  # pyright: ignore[reportPrivateUsage]
    validate_mongo_document_indexes,
)


@pytest.mark.asyncio
async def test_validate_warns_on_secondary_unique() -> None:
    client = MagicMock(spec=MongoClient)
    client.list_indexes = AsyncMock(
        return_value=[
            {"name": "_id_", "key": {"_id": 1}},
            {"name": "email_1", "key": {"email": 1}, "unique": True},
        ],
    )
    intro = MongoIntrospector(client=client)

    mock_logger = MagicMock()
    with patch("forze_mongo.kernel.validate_indexes.logger", mock_logger):
        await validate_mongo_document_indexes(
            intro,
            [
                MongoDocumentIndexSpec(
                    name="projects",
                    write_relation=("app", "projects"),
                ),
            ],
        )

    mock_logger.warning.assert_called_once()
    assert "secondary unique" in str(mock_logger.warning.call_args)


# ....................... #


class TestWhatMongoDeclaresItCanKeep:
    """The declaration is checked against what the mechanisms actually do.

    A capability that reconciles and then does not enforce is the failure the whole convention
    exists to prevent, and it is invisible until a deployment writes the row that should have
    been refused — so the claim is pinned here rather than read off the docstring.
    """

    def test_skip_null_is_not_claimed(self) -> None:
        # Mongo has no mechanism for it. A sparse index skips a document only when *every*
        # indexed field is missing and still indexes an explicit null, so a tuple holding one
        # stays in the index and conflicts — the opposite of the exemption. A
        # `partialFilterExpression` cannot say "not null" either.
        assert MongoDocumentAdapter.storage_guarantees.unique_together_skip_null is False

    def test_a_skip_null_guarantee_is_refused_by_name(self) -> None:
        unmet = MongoDocumentAdapter.storage_guarantees.unmet(
            UniqueTogether(fields=("supersedes_id",), skip_null=True)
        )

        assert unmet == ("exempting rows whose tuple holds a null (`skip_null`)",)

    def test_the_filtered_form_is_claimed(self) -> None:
        # The contrast: the refusal above is about `skip_null`, not about Mongo being unable to
        # restrict a guarantee at all.
        assert (
            MongoDocumentAdapter.storage_guarantees.unmet(
                UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
            )
            == ()
        )


# ....................... #


class TestTheFilterIsComparedBySelectedDocuments:
    """A `partialFilterExpression` counts when it selects the documents the guarantee covers.

    Two weaker readings were tried first and both accept an index that does not keep the
    guarantee: matching the filter's *text* takes `status_code` for `status`, and matching its
    *field names* takes `{is_current: false}` for a guarantee about current documents. What has
    to agree is the document set, so the two filters are reduced to their equality constraints
    and compared — and anything that does not reduce that way is refused rather than guessed at,
    because whether it covers the guarantee's documents is an implication between predicates.
    """

    @staticmethod
    def _spec(where: dict[str, object]) -> MongoDocumentIndexSpec:
        return MongoDocumentIndexSpec(
            name="fact",
            write_relation=("db", "coll"),
            guarantees=(UniqueTogether(fields=("root_id",), where={"$values": where}),),
        )

    @staticmethod
    def _index(partial_filter: dict[str, object] | None) -> MongoIndexInfo:
        return MongoIndexInfo(
            name="ix",
            keys=(("root_id", 1),),
            unique=True,
            partial_filter=partial_filter,
        )

    def _validate(self, where: dict[str, object], partial_filter: dict[str, object]) -> None:
        _require_guarantee_indexes(
            self._spec(where),
            [self._index(partial_filter)],
            database="db",
            collection="coll",
        )

    def test_a_longer_field_name_containing_the_declared_one_is_refused(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"status": "current"}, {"status_code": "current"})

    def test_the_declared_field_itself_counts(self) -> None:
        self._validate({"status": "current"}, {"status": "current"})

    def test_a_wrong_value_on_the_right_field_is_refused(self) -> None:
        # The failure the field-name reading could not see: the index restricts exactly the
        # column the guarantee filters on, to the opposite value, so every document the
        # guarantee covers is outside the index and duplicates are insertable.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"is_current": True}, {"is_current": False})

    def test_the_operator_spelling_of_the_same_equality_counts(self) -> None:
        # `{f: v}` and `{f: {$eq: v}}` are one constraint; refusing the second would fail a
        # correct migration over a spelling the server treats as identical.
        self._validate({"status": "current"}, {"$and": [{"status": {"$eq": "current"}}]})

    def test_an_extra_restriction_is_refused(self) -> None:
        # A document the guarantee covers but the index skips — one with no `tenant_id` — can be
        # duplicated, so an index narrower than the declaration does not keep it.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate(
                {"status": "current"},
                {"$and": [{"status": "current"}, {"tenant_id": {"$exists": True}}]},
            )

    def test_a_broader_restriction_is_refused_too(self) -> None:
        # Sound for the guarantee and wrong for the aggregate: an index over current *and* draft
        # documents refuses a second draft, which the declaration deliberately allows. The same
        # reason a plain unique index does not satisfy a filtered guarantee.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate(
                {"status": "current"},
                {"$or": [{"status": "current"}, {"status": "draft"}]},
            )

    def test_a_dotted_path_is_not_the_field_it_starts_with(self) -> None:
        # `meta.kind == "x"` selects different documents from `meta == "x"`.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"meta": "x"}, {"meta.kind": "x"})

    def test_an_operator_name_is_not_read_as_a_field(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"exists": True}, {"root_id": {"$exists": True}})

    def test_a_field_inside_a_matched_document_is_not_a_restricted_field(self) -> None:
        # `{metadata: {deleted: true}}` matches documents whose `metadata` equals that whole
        # document. It says nothing about a top-level `deleted`, and reading it as if it did
        # accepts an index that does not restrict the documents the guarantee covers.
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"deleted": True}, {"metadata": {"deleted": True}})

    def test_a_document_under_an_operator_is_not_descended_into_either(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"deleted": True}, {"metadata": {"$eq": {"deleted": True}}})

    def test_an_index_over_other_fields_does_not_count(self) -> None:
        with pytest.raises(CoreException, match="no unique index"):
            _require_guarantee_indexes(
                self._spec({"status": "current"}),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("tenant_id", 1),),
                        unique=True,
                        partial_filter={"status": "current"},
                    )
                ],
                database="db",
                collection="coll",
            )

    def test_a_special_index_type_does_not_count(self) -> None:
        # A `text` or `hashed` index is a different structure over a different value; taking one
        # for a unique tuple index would accept a collection with no such constraint on it.
        with pytest.raises(CoreException, match="no unique index"):
            _require_guarantee_indexes(
                self._spec({"status": "current"}),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("root_id", "text"),),
                        unique=True,
                        partial_filter={"status": "current"},
                    )
                ],
                database="db",
                collection="coll",
            )

    @pytest.mark.parametrize(
        "expression",
        [
            "not-an-expression",
            {"$or": [{"a": 1}]},
            {"a": {"$gt": 1}},
            {"$and": "not-a-list"},
        ],
    )
    def test_what_does_not_reduce_to_equalities_says_so(self, expression: object) -> None:
        # `None`, not an empty map: "no constraints" and "constraints I cannot compare" are
        # different answers, and only the first would ever match a guarantee's own filter.
        assert _equalities(expression) is None

    def test_a_matched_document_is_a_value_like_any_other(self) -> None:
        assert _equalities({"metadata": {"deleted": True}}) is None

    @pytest.mark.parametrize(
        "where",
        [
            {"$or": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]},
            {"$values": {"a": {"$gt": 1}}},
        ],
    )
    def test_a_guarantee_filter_that_is_not_plain_equalities_says_so(
        self,
        where: dict[str, object],
    ) -> None:
        # The same answer from the declaration's side: a guarantee whose own filter is a
        # disjunction or a range has no equality map to compare, so no index can be proven to
        # cover it and every one of them is refused.
        assert _guarantee_equalities(where) is None

    def test_a_guarantee_filter_of_plain_equalities_reduces(self) -> None:
        assert _guarantee_equalities({"$values": {"a": 1, "b": "x"}}) == {"a": 1, "b": "x"}

    def test_a_guarantee_whose_filter_cannot_be_compared_is_refused(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            _require_guarantee_indexes(
                MongoDocumentIndexSpec(
                    name="fact",
                    write_relation=("db", "coll"),
                    guarantees=(
                        UniqueTogether(
                            fields=("root_id",),
                            where={"$values": {"status": {"$gt": "a"}}},
                        ),
                    ),
                ),
                [
                    MongoIndexInfo(
                        name="ix",
                        keys=(("root_id", 1),),
                        unique=True,
                        partial_filter={"status": {"$gt": "a"}},
                    )
                ],
                database="db",
                collection="coll",
            )
