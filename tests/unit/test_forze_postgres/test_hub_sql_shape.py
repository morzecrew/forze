"""Unit tests for hub SQL shape decisions: thin projection and the ORDER BY fallbacks."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import attrs
import pytest

pytest.importorskip("psycopg")

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze_postgres.adapters.search.hub.adapter import PostgresHubSearchAdapter
from forze_postgres.adapters.search.hub.constants import COMBO_ALIAS
from forze_postgres.adapters.search.hub.plan import HubSearchPlan, build_hub_search_plan
from forze_postgres.adapters.search.hub.runtime import HubLegRuntime
from forze_postgres.adapters.search.hub.sql import HubSearchSqlMixin
from forze_postgres.kernel.catalog.introspect.types import PostgresIndexInfo

# ----------------------- #


class _HubRow(BaseModel):
    id: str = ""
    fk_a: str = ""
    title: str = ""


# ....................... #


class _SqlHost(HubSearchSqlMixin[_HubRow]):
    """The mixin with only what its SQL-shape methods reach supplied."""

    def __init__(
        self,
        *,
        read_fields: frozenset[str],
        order_by: sql.Composable | None = None,
    ) -> None:
        self._read_fields = read_fields
        self._order_by = order_by

    @property
    def read_fields(self) -> frozenset[str]:
        return self._read_fields

    async def order_by_clause(
        self,
        sorts: Any,
        *,
        table_alias: str,
    ) -> sql.Composable | None:
        _ = sorts, table_alias
        return self._order_by


# ....................... #


def _leg() -> HubLegRuntime:
    return HubLegRuntime(
        search=SearchSpec(name="leg", model_type=_HubRow, fields=["title"]),
        index_relation=("public", "idx"),
        index_heap_relation=("public", "heap"),
        hub_fk_columns="fk_a",
        heap_pk_column="id",
        engine="pgroonga",
    )


# ....................... #


async def _plan(*, read_fields: frozenset[str], query: str) -> HubSearchPlan:
    """A plan as the builder produces it, for a hub with one leg over ``read_fields``."""

    leg = _leg()
    host = MagicMock()
    host.hub_spec = HubSearchSpec(name="hub", model_type=_HubRow, members=(leg.search,))
    host.members = (leg,)
    host.vector_embedders = {}
    host.combine = "or"
    host.score_merge = "max"
    host.per_leg_limit = 5000
    host.combo_limit = None
    host.execution = "sql"
    host.read_fields = read_fields

    return await build_hub_search_plan(
        host,
        query=query,
        options=None,
        sorts=None,
        pagination_or_cursor={"limit": 10},
        snapshot=None,
        result_snapshot=None,
        mode="cursor",
    )


# ----------------------- #


class TestHubThinProjection:
    """Late materialization is opt-out by shape: no id column means no thin phase."""

    @pytest.mark.asyncio
    async def test_projects_id_leg_fk_and_sort_roots(self) -> None:
        read_fields = frozenset({"id", "fk_a", "title"})
        plan = await _plan(read_fields=read_fields, query="alpha")
        host = _SqlHost(read_fields=read_fields)

        assert host._hub_thin_projection(plan) == ["fk_a", "id"]

    # ....................... #

    @pytest.mark.asyncio
    async def test_refuses_to_thin_without_the_id_column(self) -> None:
        """Hydration keys the page by id, so a read model without one cannot be thinned."""

        read_fields = frozenset({"fk_a", "title"})
        plan = await _plan(read_fields=read_fields, query="alpha")
        host = _SqlHost(read_fields=read_fields)

        assert host._hub_thin_projection(plan) is None

    # ....................... #

    @pytest.mark.asyncio
    async def test_refuses_to_thin_a_sort_over_an_unprojected_column(self) -> None:
        """A sort root the hub does not read cannot be carried through the thin pipeline."""

        read_fields = frozenset({"id", "fk_a", "title"})
        plan = await _plan(read_fields=read_fields, query="alpha")
        plan = attrs.evolve(plan, order_key_spec=(("missing", "asc"),))
        host = _SqlHost(read_fields=read_fields)

        assert host._hub_thin_projection(plan) is None


# ----------------------- #


class TestRenderHubOrderSql:
    """Browse (no legs, no sorts) must still emit a deterministic ORDER BY."""

    @pytest.mark.asyncio
    async def test_falls_back_to_id_when_nothing_is_sorted(self) -> None:
        read_fields = frozenset({"id", "fk_a", "title"})
        plan = await _plan(read_fields=read_fields, query="")
        host = _SqlHost(read_fields=read_fields)

        rendered = await host.render_hub_order_sql(plan)

        assert rendered.as_string(None) == f'"{COMBO_ALIAS}"."id" ASC'

    # ....................... #

    @pytest.mark.asyncio
    async def test_falls_back_to_the_first_read_field_without_an_id_column(self) -> None:
        """Without an id there is no natural tiebreak, so the lowest-named column orders."""

        read_fields = frozenset({"fk_a", "title"})
        plan = await _plan(read_fields=frozenset({"id", "fk_a", "title"}), query="")
        host = _SqlHost(read_fields=read_fields)

        rendered = await host.render_hub_order_sql(plan)

        assert rendered.as_string(None) == f'"{COMBO_ALIAS}"."fk_a" ASC'

    # ....................... #

    @pytest.mark.asyncio
    async def test_user_sort_wins_over_both_fallbacks(self) -> None:
        read_fields = frozenset({"id", "fk_a", "title"})
        plan = await _plan(read_fields=read_fields, query="")
        host = _SqlHost(
            read_fields=read_fields,
            order_by=sql.SQL("{} DESC").format(sql.Identifier(COMBO_ALIAS, "title")),
        )

        rendered = await host.render_hub_order_sql(plan)

        assert rendered.as_string(None) == f'"{COMBO_ALIAS}"."title" DESC'


# ----------------------- #


class _HubRowNoId(BaseModel):
    """A hub read model the thin pipeline cannot hydrate: no primary key to key it by."""

    fk_a: str = ""
    title: str = ""


# ....................... #


def _adapter(model: type[BaseModel]) -> tuple[Any, MagicMock]:
    """A hub adapter over one pgroonga leg, with the catalog and the client faked."""

    leg_spec = SearchSpec(name="leg", model_type=model, fields=["title"])
    leg = HubLegRuntime(
        search=leg_spec,
        index_relation=("public", "idx"),
        index_heap_relation=("public", "heap"),
        hub_fk_columns="fk_a",
        heap_pk_column="fk_a",
        engine="pgroonga",
    )
    hub_spec = HubSearchSpec(name="hub", model_type=model, members=(leg_spec,))

    introspector = MagicMock()
    introspector.get_column_types = AsyncMock(return_value={})
    introspector.get_index_info = AsyncMock(
        return_value=PostgresIndexInfo(
            schema="public",
            name="idx",
            amname="pgroonga",
            engine="pgroonga",
            indexdef="CREATE INDEX idx ON public.heap USING pgroonga (title)",
            columns=("title",),
        ),
    )

    client = MagicMock()
    client.fetch_all = AsyncMock(return_value=[])

    adapter = PostgresHubSearchAdapter(
        hub_spec=hub_spec,
        members=(leg,),
        relation=("public", "hub"),
        client=client,
        codec=hub_spec.resolved_read_codec,
        model_type=model,
        introspector=introspector,
        tenant_provider=None,
        tenant_aware=False,
        filter_table_alias="h",
    )
    return adapter, client


# ....................... #


async def _cursor_select(model: type[BaseModel]) -> str:
    adapter, client = _adapter(model)
    await adapter.search_cursor("alpha", cursor={"limit": 5})
    statement = client.fetch_all.await_args_list[-1].args[0]
    rendered = statement.as_string(None)

    return rendered[rendered.rindex("SELECT") :]


# ----------------------- #


class TestCursorSelectShape:
    """The cursor page selects thin key columns, or every read field when it cannot thin."""

    @pytest.mark.asyncio
    async def test_thin_page_selects_only_key_and_sort_columns(self) -> None:
        select = await _cursor_select(_HubRow)

        assert '"comb"."id"' in select
        assert '"comb"."title"' not in select

    # ....................... #

    @pytest.mark.asyncio
    async def test_unthinnable_shape_selects_the_full_read_projection(self) -> None:
        """No id column means no hydration by id, so the page carries the heavy columns."""

        select = await _cursor_select(_HubRowNoId)

        assert '"comb"."title"' in select
        assert '"comb"."fk_a"' in select
