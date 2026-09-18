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


class TestTheFilterIsComparedByField:
    """A `partialFilterExpression` is matched on the fields it names, not on its text.

    The text reading is the one that fails quietly: it accepts an index restricted to the wrong
    documents whenever the wrong field's name happens to contain the right one, which is common
    enough in real schemas (`status` / `status_code`, `deleted` / `deleted_at`) to be the
    default outcome rather than an edge case.
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

    def test_a_field_under_an_operator_counts(self) -> None:
        # `$and` is a Mongo operator, not a field; its branches carry the fields.
        self._validate(
            {"status": "current"},
            {"$and": [{"status": {"$eq": "current"}}, {"tenant_id": {"$exists": True}}]},
        )

    def test_a_dotted_path_contributes_its_root(self) -> None:
        self._validate({"meta": "x"}, {"meta.kind": "x"})

    def test_an_operator_name_is_not_read_as_a_field(self) -> None:
        with pytest.raises(CoreException, match="partialFilterExpression"):
            self._validate({"exists": True}, {"root_id": {"$exists": True}})
