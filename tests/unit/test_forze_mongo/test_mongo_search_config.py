"""Unit tests for :func:`~forze_mongo.execution.deps.configs.validate_mongo_search_conf`."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps.configs import validate_mongo_search_conf


class _Read(BaseModel):
    id: str
    title: str = ""


def _spec() -> SearchSpec[_Read]:
    return SearchSpec(name="items", model_type=_Read, fields=("title",))


def test_validate_text_engine_minimal() -> None:
    validate_mongo_search_conf(
        {"read": ("app", "items"), "engine": "text"},
        _spec(),
    )


def test_validate_atlas_requires_index_name() -> None:
    with pytest.raises(CoreException):
        validate_mongo_search_conf(
            {"read": ("app", "items"), "engine": "atlas"},
            _spec(),
        )


def test_validate_vector_requires_embedding_fields() -> None:
    with pytest.raises(CoreException):
        validate_mongo_search_conf(
            {
                "read": ("app", "items"),
                "engine": "vector",
                "vector_path": "embedding",
            },
            _spec(),
        )


def test_field_map_key_must_be_in_spec_fields() -> None:
    with pytest.raises(CoreException):
        validate_mongo_search_conf(
            {
                "read": ("app", "items"),
                "engine": "text",
                "field_map": {"unknown": "x"},
            },
            _spec(),
        )
