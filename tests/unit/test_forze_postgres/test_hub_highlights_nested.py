"""Postgres hub highlights resolve nested (dotted) fields in process.

The hub marks highlights on already-materialized hits, so a nested highlightable field
(``contract.title``) must resolve the same way the mock oracle and nested sort keys do —
whether the hit is a hydrated model or a projected ``JsonDict``.
"""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.search import (
    HubSearchSpec,
    SearchSpec,
    search_page_from_limit_offset,
)
from forze_postgres.adapters.search.hub._facets_highlights import attach_hub_highlights

# ----------------------- #


class _Sub(BaseModel):
    title: str


class _Doc(BaseModel):
    id: str
    contract: _Sub


def _hub() -> HubSearchSpec[_Doc]:
    leg = SearchSpec(name="a", model_type=_Doc, fields=["contract.title"])
    return HubSearchSpec(
        name="hub",
        model_type=_Doc,
        members=[leg],
        highlightable_fields=frozenset({"contract.title"}),
    )


def test_hub_highlights_nested_field_on_model_hits() -> None:
    page = search_page_from_limit_offset(
        [_Doc(id="1", contract=_Sub(title="alpha beta"))],
        {"limit": 10},
        total=1,
    )

    out = attach_hub_highlights(
        page, hub_spec=_hub(), query="alpha", options={"highlight": True}
    )

    assert out.highlights == [{"contract.title": ("<em>alpha</em> beta",)}]


def test_hub_highlights_nested_field_on_projected_dict_hits() -> None:
    page = search_page_from_limit_offset(
        [{"id": "1", "contract": {"title": "alpha beta"}}],
        {"limit": 10},
        total=1,
    )

    out = attach_hub_highlights(
        page,
        hub_spec=_hub(),
        query="alpha",
        options={"highlight": True},
        return_fields=["contract.title"],
    )

    assert out.highlights == [{"contract.title": ("<em>alpha</em> beta",)}]
