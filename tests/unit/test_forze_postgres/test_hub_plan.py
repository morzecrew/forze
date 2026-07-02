"""Unit tests for hub search plan builder."""

from __future__ import annotations

from typing import Any, Literal
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze_postgres.adapters.search.hub.plan import build_hub_search_plan
from forze_postgres.adapters.search.hub.runtime import HubLegRuntime


class _HubRead:
    id: str


def _make_host(*, execution: Literal["sql", "parallel"]) -> Any:
    leg_spec = SearchSpec(name="leg", model_type=_HubRead, fields=["title"])
    leg = HubLegRuntime(
        search=leg_spec,
        index_relation=("public", "idx"),
        index_heap_relation=("public", "heap"),
        hub_fk_columns="id",
        heap_pk_column="id",
        engine="pgroonga",
    )
    hub_spec = HubSearchSpec(
        name="hub",
        model_type=_HubRead,
        members=(leg_spec,),
    )
    host = MagicMock()
    host.hub_spec = hub_spec
    host.members = (leg,)
    host.vector_embedders = {}
    host.combine = "or"
    host.score_merge = "max"
    host.per_leg_limit = 5000
    host.combo_limit = None
    host.execution = execution
    host.read_fields = frozenset({"id"})
    return host


@pytest.mark.asyncio
async def test_build_hub_search_plan_parallel_offset_no_user_sorts() -> None:
    plan = await build_hub_search_plan(
        _make_host(execution="parallel"),
        query="alpha",
        options=None,
        sorts=None,
        pagination_or_cursor={"limit": 10},
        snapshot=None,
        result_snapshot=None,
        mode="offset",
    )
    assert plan.do_legs is True
    assert plan.use_parallel is True


@pytest.mark.asyncio
async def test_build_hub_search_plan_sql_when_user_sorts_on_offset() -> None:
    plan = await build_hub_search_plan(
        _make_host(execution="parallel"),
        query="alpha",
        options=None,
        sorts={"id": "desc"},  # type: ignore[arg-type]
        pagination_or_cursor={},
        snapshot=None,
        result_snapshot=None,
        mode="offset",
    )
    assert plan.use_parallel is False


@pytest.mark.asyncio
async def test_offset_caps_combo_but_cursor_walks_full_set() -> None:
    """The ``combo_top`` cap is an offset-page top-N bound; a cursor walk (and a hub stream
    export) must not be capped by it, or it truncates at ``combo_top`` instead of the whole
    ranked result set."""

    async def _plan(mode: str):
        return await build_hub_search_plan(
            _make_host(execution="sql"),
            query="alpha",
            options=None,
            sorts=None,
            pagination_or_cursor={"limit": 10},
            snapshot=None,
            result_snapshot=None,
            mode=mode,  # type: ignore[arg-type]
        )

    offset_plan = await _plan("offset")
    cursor_plan = await _plan("cursor")

    # Offset still bounds the candidate pool for the requested page.
    assert offset_plan.resolved_combo is not None
    # Cursor pagination walks the full set: no combo cap → no silent truncation.
    assert cursor_plan.resolved_combo is None
