"""Behavior tests for the ``PostgresFederatedSearchConfig`` shared ``rrf`` value object."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from forze.application.contracts.search import Rrf
from forze.base.exceptions import CoreException
from forze_postgres.execution.deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLegSearch,
    PostgresSearchConfig,
)

# ----------------------- #


def _members() -> dict[str, PostgresFederatedSearchLegSearch]:
    return {
        name: PostgresFederatedSearchLegSearch(
            search=PostgresSearchConfig(
                index=("public", f"ix_{name}"),
                read=("public", f"t_{name}"),
                engine="pgroonga",
            )
        )
        for name in ("a", "b")
    }


# ....................... #


def test_default_rrf() -> None:
    cfg = PostgresFederatedSearchConfig(members=_members())
    assert cfg.rrf_k == 60
    assert cfg.rrf_per_leg_limit == 5000


def test_custom_rrf_shims() -> None:
    cfg = PostgresFederatedSearchConfig(
        members=_members(),
        rrf=Rrf(k=10, per_leg_limit=250),
    )
    assert cfg.rrf_k == 10
    assert cfg.rrf_per_leg_limit == 250


@pytest.mark.parametrize("kwargs", [{"k": 0}, {"k": -1}, {"per_leg_limit": 0}])
def test_rrf_rejects_invalid_settings(kwargs: dict[str, int]) -> None:
    with pytest.raises(CoreException):
        Rrf(**kwargs)
