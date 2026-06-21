"""Behavior tests for the ``MongoSearchConfig`` engine value objects.

Covers the public construction surface (engine variants), the bare-string shorthand,
the flat read-shims that internal factories/module rely on, and per-variant validation.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_mongo.execution.deps.configs import (
    MongoAtlasEngine,
    MongoSearchConfig,
    MongoTextEngine,
    MongoVectorEngine,
)

# ----------------------- #


def _cfg(engine: object) -> MongoSearchConfig:
    return MongoSearchConfig(engine=engine, read=("app", "items"))  # type: ignore[arg-type]


# ....................... #


class TestEngineResolution:
    def test_text_resolves_kind(self) -> None:
        assert _cfg(MongoTextEngine()).engine == "text"

    def test_atlas_resolves_kind(self) -> None:
        assert _cfg(MongoAtlasEngine(index_name="ix")).engine == "atlas"

    def test_vector_resolves_kind(self) -> None:
        cfg = _cfg(
            MongoVectorEngine(
                index_name="ix", vector_path="emb", embeddings_name="m", dimensions=3
            )
        )
        assert cfg.engine == "vector"

    def test_bare_text_string_is_shorthand(self) -> None:
        cfg = _cfg("text")
        assert cfg.engine == "text"
        assert isinstance(cfg.engine_spec, MongoTextEngine)

    def test_bare_atlas_string_rejected_with_pointer(self) -> None:
        with pytest.raises(CoreException, match="MongoAtlasEngine"):
            _cfg("atlas")

    def test_bare_vector_string_rejected_with_pointer(self) -> None:
        with pytest.raises(CoreException, match="MongoVectorEngine"):
            _cfg("vector")


class TestFlatReadShims:
    def test_text_shims(self) -> None:
        cfg = _cfg(MongoTextEngine(default_language="english"))
        assert cfg.default_language == "english"
        assert cfg.index_name is None
        assert cfg.vector_path is None

    def test_atlas_shims(self) -> None:
        cfg = _cfg(MongoAtlasEngine(index_name="ix"))
        assert str(cfg.index_name) == "ix"
        assert cfg.vector_path is None
        assert cfg.default_language is None

    def test_vector_shims(self) -> None:
        cfg = _cfg(
            MongoVectorEngine(
                index_name="ix", vector_path="emb", embeddings_name="m", dimensions=3
            )
        )
        assert str(cfg.index_name) == "ix"
        assert cfg.vector_path == "emb"
        assert cfg.embeddings_name == "m"
        assert cfg.embedding_dimensions == 3


class TestPerVariantValidation:
    def test_atlas_empty_index_rejected(self) -> None:
        with pytest.raises(CoreException, match="index_name is required"):
            MongoAtlasEngine(index_name="")

    def test_vector_empty_path_rejected(self) -> None:
        with pytest.raises(CoreException, match="vector_path is required"):
            MongoVectorEngine(
                index_name="ix", vector_path="", embeddings_name="m", dimensions=3
            )

    def test_vector_bad_dimensions_rejected(self) -> None:
        with pytest.raises(CoreException, match="embedding_dimensions"):
            MongoVectorEngine(
                index_name="ix", vector_path="emb", embeddings_name="m", dimensions=0
            )
