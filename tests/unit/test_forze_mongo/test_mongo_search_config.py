"""Unit tests for :class:`~forze_mongo.execution.deps.configs.MongoSearchConfig`."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps.configs import MongoSearchConfig


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
    with pytest.raises(CoreException):
        MongoSearchConfig(read=("app", "items"), engine="atlas")


def test_validate_vector_requires_embedding_fields() -> None:
    with pytest.raises(CoreException):
        MongoSearchConfig(
            read=("app", "items"),
            engine="vector",
            vector_path="embedding",
        )


def test_field_map_key_must_be_in_spec_fields() -> None:
    config = MongoSearchConfig(
        read=("app", "items"),
        engine="text",
        field_map={"unknown": "x"},
    )
    with pytest.raises(CoreException):
        config.validate_against_spec(_spec())
