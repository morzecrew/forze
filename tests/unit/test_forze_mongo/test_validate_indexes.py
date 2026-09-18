"""Unit tests for Mongo document index validation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.application.contracts.guarantees import UniqueTogether
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.introspect import MongoIntrospector
from forze_mongo.kernel.validate_indexes import (
    MongoDocumentIndexSpec,
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
