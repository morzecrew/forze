"""Unit tests for hub leg runtime builder."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_postgres.adapters.search.hub.runtime import HubLegRuntime
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresSearchConfig,
    VectorEngine,
)
from forze_postgres.execution.deps.factories.hub_builder import build_hub_leg_runtimes

pytest.importorskip("psycopg")


class _HubRow(BaseModel):
    id: int
    title: str


class _LegHit(BaseModel):
    label: str


def _hub_spec() -> HubSearchSpec[_HubRow]:
    return HubSearchSpec(
        name="hub_test",
        model_type=_HubRow,
        members=(
            SearchSpec(name="leg_a", model_type=_LegHit, fields=["label"]),
            SearchSpec(name="leg_b", model_type=_LegHit, fields=["label"]),
        ),
    )


def _pgroonga_config() -> PostgresHubSearchConfig:
    return PostgresHubSearchConfig(
        hub=("public", "hub_tbl"),
        members={
            "leg_a": PostgresHubSearchMemberConfig(
                index=("public", "idx_a"),
                read=("public", "heap_a"),
                engine="pgroonga",
                hub_fk="hub_id",
            ),
            "leg_b": PostgresHubSearchMemberConfig(
                index=("public", "idx_b"),
                read=("public", "heap_b"),
                engine="pgroonga",
                hub_fk="other_id",
            ),
        },
    )


def _context_with_embedder() -> ExecutionContext:
    ctx = MagicMock(spec=ExecutionContext)
    embedder = MagicMock(spec=EmbeddingsProviderPort)
    ctx.embeddings.provider.return_value = embedder
    return ctx


def test_build_hub_leg_runtimes_pgroonga() -> None:
    members, embedders = build_hub_leg_runtimes(
        _context_with_embedder(),
        _hub_spec(),
        _pgroonga_config(),
    )
    assert len(members) == 2
    assert all(isinstance(m, HubLegRuntime) for m in members)
    assert members[0].engine == "pgroonga"
    assert embedders == {}


def test_build_hub_leg_runtimes_missing_member_config() -> None:
    cfg = PostgresHubSearchConfig(
        hub=("public", "h"),
        members={
            "leg_a": PostgresHubSearchMemberConfig(
                index=("public", "i"),
                read=("public", "r"),
                engine="pgroonga",
                hub_fk="fk",
            ),
        },
    )
    with pytest.raises(CoreException, match="leg_b"):
        build_hub_leg_runtimes(_context_with_embedder(), _hub_spec(), cfg)


def test_build_hub_leg_runtimes_vector_resolves_embedder() -> None:
    spec = HubSearchSpec(
        name="hv",
        model_type=_HubRow,
        members=(SearchSpec(name="vleg", model_type=_LegHit, fields=["label"]),),
    )
    cfg = PostgresHubSearchConfig(
        hub=("public", "hub_tbl"),
        members={
            "vleg": PostgresHubSearchMemberConfig(
                index=("public", "idx_v"),
                read=("public", "heap_v"),
                engine=VectorEngine(column="emb", dimensions=8, embeddings_name="openai"),
                hub_fk="hub_id",
            ),
        },
    )
    ctx = _context_with_embedder()
    members, embedders = build_hub_leg_runtimes(ctx, spec, cfg)
    assert members[0].engine == "vector"
    assert 0 in embedders
    ctx.embeddings.provider.assert_called_once()


def test_build_hub_leg_runtimes_fts_requires_groups() -> None:
    spec = HubSearchSpec(
        name="hf",
        model_type=_HubRow,
        members=(SearchSpec(name="fleg", model_type=_LegHit, fields=["label"]),),
    )
    with pytest.raises(CoreException, match="FTS groups are required"):
        PostgresHubSearchConfig(
            hub=("public", "hub_tbl"),
            members={
                "fleg": PostgresHubSearchMemberConfig(
                    index=("public", "idx_f"),
                    read=("public", "heap_f"),
                    engine=FtsEngine(groups={}),
                    hub_fk="hub_id",
                ),
            },
        )
    _ = spec


def test_build_hub_leg_runtimes_same_heap_field_must_be_on_hub_model() -> None:
    spec = HubSearchSpec(
        name="hs",
        model_type=_HubRow,
        members=(SearchSpec(name="leg_a", model_type=_LegHit, fields=["missing_field"]),),
    )
    cfg = PostgresHubSearchConfig(
        hub=("public", "hub_tbl"),
        members={
            "leg_a": PostgresHubSearchMemberConfig(
                index=("public", "idx"),
                read=("public", "hub_tbl"),
                heap=("public", "hub_tbl"),
                engine="pgroonga",
                hub_fk="id",
                same_heap_as_hub=True,
            ),
        },
    )
    with pytest.raises(CoreException, match="same_heap_as_hub"):
        build_hub_leg_runtimes(_context_with_embedder(), spec, cfg)


class TestMemberFromSearchConfig:
    """PostgresHubSearchMemberConfig.from_search_config carries every parent field + hub wiring."""

    @staticmethod
    def _base() -> PostgresSearchConfig:
        return PostgresSearchConfig(
            index=("public", "docs_idx"),
            read=("public", "docs"),
            engine="pgroonga",
            tenant_aware=True,
            candidate_limit=1234,
            field_map={"title": "title_col"},
        )

    def test_carries_parent_fields_and_engine_alias(self) -> None:
        base = self._base()

        member = PostgresHubSearchMemberConfig.from_search_config(base, hub_fk="doc_id")

        assert isinstance(member, PostgresSearchConfig)  # still a subclass instance
        assert member.index == base.index
        assert member.read == base.read
        assert member.engine == "pgroonga"  # engine_spec→engine alias preserved
        assert member.engine_spec == base.engine_spec
        assert member.tenant_aware is True
        assert member.candidate_limit == 1234
        assert member.field_map == {"title": "title_col"}
        # Hub extras default when unset.
        assert member.hub_fk == "doc_id"
        assert member.heap_pk == "id"
        assert member.same_heap_as_hub is False

    def test_applies_hub_overrides_and_sequence_fk(self) -> None:
        member = PostgresHubSearchMemberConfig.from_search_config(
            self._base(),
            hub_fk=["a", "b"],
            heap_pk="pk",
            same_heap_as_hub=True,
        )

        assert member.hub_fk == ["a", "b"]
        assert member.heap_pk == "pk"
        assert member.same_heap_as_hub is True

    def test_result_is_usable_as_a_hub_leg(self) -> None:
        member = PostgresHubSearchMemberConfig.from_search_config(
            self._base(), hub_fk="hub_id"
        )
        # Constructs a valid hub config (its validate() accepts the derived leg).
        cfg = PostgresHubSearchConfig(hub=("public", "hub_tbl"), members={"leg": member})
        assert cfg.members["leg"].hub_fk == "hub_id"
