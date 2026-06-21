"""Behavior tests for the ``PostgresSearchConfig`` engine value objects.

Covers the public construction surface (engine variants), the bare-string shorthand,
the flat read-shims that internal factories/adapters rely on, and the per-variant
validation that replaced the old flat ``__attrs_post_init__`` match.
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaAuto,
    PgroongaEngine,
    PostgresSearchConfig,
    VectorEngine,
)

pytest.importorskip("psycopg")

# ----------------------- #


def _cfg(engine: object) -> PostgresSearchConfig:
    return PostgresSearchConfig(
        engine=engine,  # type: ignore[arg-type]
        index=("public", "ix"),
        read=("public", "src"),
    )


# ....................... #


class TestEngineResolution:
    def test_pgroonga_engine_resolves_kind(self) -> None:
        assert _cfg(PgroongaEngine()).engine == "pgroonga"

    def test_fts_engine_resolves_kind(self) -> None:
        assert _cfg(FtsEngine(groups={"A": ("title",)})).engine == "fts"

    def test_vector_engine_resolves_kind(self) -> None:
        cfg = _cfg(VectorEngine(column="emb", dimensions=3, embeddings_name="e"))
        assert cfg.engine == "vector"

    def test_bare_pgroonga_string_is_shorthand(self) -> None:
        cfg = _cfg("pgroonga")
        assert cfg.engine == "pgroonga"
        assert isinstance(cfg.engine_spec, PgroongaEngine)

    def test_bare_fts_string_rejected_with_pointer(self) -> None:
        with pytest.raises(CoreException, match="FtsEngine"):
            _cfg("fts")

    def test_bare_vector_string_rejected_with_pointer(self) -> None:
        with pytest.raises(CoreException, match="VectorEngine"):
            _cfg("vector")


class TestFlatReadShims:
    """The internal factories/adapters read engine knobs by their historical flat names."""

    def test_pgroonga_shims(self) -> None:
        cfg = _cfg(
            PgroongaEngine(
                score_version="v1",
                plan="auto",
                index_first_filter_margin=2.0,
                auto=PgroongaAuto(
                    index_first_min_rows=42,
                    use_exact_count=True,
                    with_filters=False,
                    filter_first_max_rows=7,
                ),
            )
        )

        assert cfg.pgroonga_score_version == "v1"
        assert cfg.pgroonga_plan == "auto"
        assert cfg.pgroonga_index_first_filter_margin == 2.0
        assert cfg.pgroonga_auto_index_first_min_rows == 42
        assert cfg.pgroonga_auto_use_exact_count is True
        assert cfg.pgroonga_auto_with_filters is False
        assert cfg.pgroonga_auto_filter_first_max_rows == 7

    def test_vector_shims(self) -> None:
        cfg = _cfg(
            VectorEngine(column="emb", dimensions=3, embeddings_name="e", distance="cosine")
        )

        assert cfg.vector_column == "emb"
        assert cfg.embedding_dimensions == 3
        assert cfg.embeddings_name == "e"
        assert cfg.vector_distance == "cosine"
        # non-active variant fields fall back to the prior defaults
        assert cfg.fts_groups is None
        assert cfg.pgroonga_score_version == "v2"

    def test_fts_shims(self) -> None:
        cfg = _cfg(FtsEngine(groups={"A": ("title",)}))

        assert cfg.fts_groups == {"A": ("title",)}
        assert cfg.vector_column is None

    def test_candidate_limit_is_shared_across_engines(self) -> None:
        # ``pgroonga_candidate_limit`` is the historical name of the shared ranked-heap cap.
        for engine in (
            PgroongaEngine(),
            FtsEngine(groups={"A": ("title",)}),
            VectorEngine(column="emb", dimensions=3, embeddings_name="e"),
        ):
            cfg = PostgresSearchConfig(
                engine=engine,  # type: ignore[arg-type]
                index=("public", "ix"),
                read=("public", "src"),
                candidate_limit=123,
            )
            assert cfg.candidate_limit == 123
            assert cfg.pgroonga_candidate_limit == 123


class TestPerVariantValidation:
    def test_fts_empty_groups_rejected(self) -> None:
        with pytest.raises(CoreException, match="FTS groups are required"):
            FtsEngine(groups={})

    def test_fts_duplicate_fields_rejected(self) -> None:
        with pytest.raises(CoreException, match="duplicate"):
            FtsEngine(groups={"A": ("x",), "B": ("x",)})

    def test_vector_missing_column_rejected(self) -> None:
        with pytest.raises(CoreException, match="vector_column"):
            VectorEngine(column="", dimensions=3, embeddings_name="e")

    def test_vector_bad_dimensions_rejected(self) -> None:
        with pytest.raises(CoreException, match="embedding_dimensions"):
            VectorEngine(column="emb", dimensions=0, embeddings_name="e")

    def test_pgroonga_bad_score_version_rejected(self) -> None:
        with pytest.raises(CoreException, match="pgroonga_score_version"):
            PgroongaEngine(score_version="v3")  # type: ignore[arg-type]

    def test_pgroonga_bad_margin_rejected(self) -> None:
        with pytest.raises(CoreException, match="index_first_filter_margin"):
            PgroongaEngine(index_first_filter_margin=0.5)

    def test_candidate_limit_floor(self) -> None:
        with pytest.raises(CoreException, match="candidate_limit"):
            _cfg_with_candidate_limit(0)


def _cfg_with_candidate_limit(limit: int) -> PostgresSearchConfig:
    return PostgresSearchConfig(
        engine=PgroongaEngine(),
        index=("public", "ix"),
        read=("public", "src"),
        candidate_limit=limit,
    )
