"""Meilisearch highlight planning fails closed on nested (dotted) fields.

Meilisearch highlights map onto flat ``_formatted`` attribute names, so a nested
highlightable field would silently miss; the planner rejects it instead (a top-level
field, the mock, or the Postgres hub can highlight nested paths).
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.adapters.search._facets_highlights import plan_highlights
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig

# ----------------------- #


class _Sub(BaseModel):
    title: str


class _Doc(BaseModel):
    id: str
    name: str = ""
    contract: _Sub


def _gateway(highlightable: set[str]) -> MeilisearchSearchGateway[_Doc]:
    return MeilisearchSearchGateway(
        spec=SearchSpec(
            name="items",
            model_type=_Doc,
            fields=["name", "contract.title"],
            highlightable_fields=frozenset(highlightable),
        ),
        config=MeilisearchSearchConfig(index_uid="items"),
    )


def test_nested_highlight_field_rejected() -> None:
    gw = _gateway({"contract.title"})

    with pytest.raises(CoreException) as ei:
        plan_highlights(gw, gw.spec, {"highlight": {"fields": ["contract.title"]}})

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert "contract.title" in str(ei.value)


def test_flat_highlight_field_ok() -> None:
    gw = _gateway({"name"})

    plan = plan_highlights(gw, gw.spec, {"highlight": {"fields": ["name"]}})

    assert plan is not None
    assert "name" in plan.phys_to_logical.values()
