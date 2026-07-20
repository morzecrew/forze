"""Unit tests for Mongo document index validation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
