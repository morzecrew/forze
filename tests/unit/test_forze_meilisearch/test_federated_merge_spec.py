"""Behavior tests for the ``MeilisearchFederatedSearchConfig`` merge value objects."""

from __future__ import annotations

import pytest

from forze.application.contracts.search import Rrf
from forze.base.exceptions import CoreException
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederation,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)

# ----------------------- #


def _members() -> dict[str, MeilisearchSearchConfig]:
    return {
        "a": MeilisearchSearchConfig(index_uid="idx_a"),
        "b": MeilisearchSearchConfig(index_uid="idx_b"),
    }


def _cfg(merge: object) -> MeilisearchFederatedSearchConfig:
    return MeilisearchFederatedSearchConfig(members=_members(), merge=merge)  # type: ignore[arg-type]


# ....................... #


class TestMergeResolution:
    def test_default_is_federation(self) -> None:
        cfg = MeilisearchFederatedSearchConfig(members=_members())
        assert cfg.merge == "federation"
        assert isinstance(cfg.merge_spec, MeilisearchFederation)

    def test_federation_vo(self) -> None:
        assert _cfg(MeilisearchFederation()).merge == "federation"

    def test_rrf_vo(self) -> None:
        cfg = _cfg(Rrf(k=42, per_leg_limit=99))
        assert cfg.merge == "rrf"
        assert cfg.rrf_k == 42
        assert cfg.rrf_per_leg_limit == 99

    def test_bare_federation_string_shorthand(self) -> None:
        assert _cfg("federation").merge == "federation"

    def test_bare_rrf_string_shorthand_uses_defaults(self) -> None:
        cfg = _cfg("rrf")
        assert cfg.merge == "rrf"
        assert cfg.rrf_k == 60
        assert cfg.rrf_per_leg_limit == 5000

    def test_unknown_merge_string_rejected(self) -> None:
        with pytest.raises(CoreException, match="must be 'federation' or 'rrf'"):
            _cfg("bogus")


class TestFlatReadShims:
    def test_federation_shims_to_defaults(self) -> None:
        cfg = _cfg(MeilisearchFederation())
        # non-rrf merge falls back to the prior rrf defaults
        assert cfg.rrf_k == 60
        assert cfg.rrf_per_leg_limit == 5000


def test_requires_two_members() -> None:
    with pytest.raises(CoreException, match="at least two member"):
        MeilisearchFederatedSearchConfig(
            members={"a": MeilisearchSearchConfig(index_uid="idx_a")},
        )
