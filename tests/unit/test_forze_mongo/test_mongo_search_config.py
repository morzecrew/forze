"""Unit tests for :class:`~forze_mongo.execution.deps.configs.MongoSearchConfig`."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps.configs import (
    MongoAtlasEngine,
    MongoSearchConfig,
    MongoVectorEngine,
)


class _Read(BaseModel):
    id: str
    title: str = ""


def _spec() -> SearchSpec[_Read]:
    return SearchSpec(name="items", model_type=_Read, fields=("title",))


def test_validate_text_engine_minimal() -> None:
    MongoSearchConfig(read=("app", "items"), engine="text").validate_against_spec(
        _spec()
    )


def test_validate_atlas_requires_index_name() -> None:
    with pytest.raises(CoreException, match="index_name is required"):
        MongoAtlasEngine(index_name="")


def test_validate_vector_requires_embedding_fields() -> None:
    with pytest.raises(CoreException, match="vector_path is required"):
        MongoVectorEngine(
            index_name="ix",
            vector_path="",
            embeddings_name="m",
            dimensions=8,
        )


def test_field_map_key_must_be_in_spec_fields() -> None:
    config = MongoSearchConfig(
        read=("app", "items"),
        engine="text",
        field_map={"unknown": "x"},
    )
    with pytest.raises(CoreException):
        config.validate_against_spec(_spec())
