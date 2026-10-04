"""Constructor validation for search adapters."""

import asyncio
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_postgres.adapters.search import (
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
)
from forze_postgres.adapters.search._leg_pgroonga import build_pgroonga_leg

# ----------------------- #


class _Entity(BaseModel):
    id: UUID
    a: str
    b: str


def _spec() -> SearchSpec[_Entity]:
    return SearchSpec(name="s", model_type=_Entity, fields=["a", "b"])


class _EntityWithExtra(BaseModel):
    id: UUID
    a: str
    note: str = ""  # returned, not indexed — eligible for leniency


def test_search_adapter_excludes_lenient_read_fields_from_projection() -> None:
    # A lenient read field has no column, so the adapter must not project it
    # (read_fields drives the result projection and cursor keyset).
    spec = SearchSpec(
        name="s",
        model_type=_EntityWithExtra,
        fields=["a"],
        lenient_read_fields={"note"},
    )
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=("public", "i"),
        index_heap_relation=("public", "h"),
        client=MagicMock(),
        model_type=_EntityWithExtra,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
        lenient_read_fields=spec.lenient_read_fields,
    )

    assert adapter.lenient_read_fields == frozenset({"note"})
    assert "note" not in adapter.read_fields
    assert {"id", "a"} <= adapter.read_fields


@pytest.mark.asyncio
async def test_pgroonga_v2_match_combined_empty_string_is_true_predicate() -> None:
    """Empty match text skips PGroonga clause construction (filter-only path uses ``TRUE`` elsewhere)."""
    spec = _spec()
    adapter = PostgresPGroongaSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=("public", "i"),
        index_heap_relation=("public", "h"),
        client=MagicMock(),
        model_type=_Entity,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
    )
    sw, _rank, params = await build_pgroonga_leg(
        introspector=adapter.introspector,
        index_qname=await adapter._index_qname(),
        search=adapter.spec,
        index_field_map=adapter.index_field_map,
        index_alias="t",
        queries=(),
        options=None,
        score_column="_pgroonga_rank",
        pgroonga_score_version=adapter.pgroonga_score_version,
    )
    assert params == []
    assert "TRUE" in str(sw)


def test_pgroonga_v2_rejects_duplicate_projection_join_columns() -> None:
    spec = _spec()
    with pytest.raises(CoreException, match="unique"):
        PostgresPGroongaSearchAdapter(
            spec=spec,
            codec=spec.resolved_read_codec,
            relation=("public", "v"),
            index_relation=("public", "i"),
            index_heap_relation=("public", "h"),
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )


def test_fts_v2_rejects_duplicate_projection_join_columns() -> None:
    spec = _spec()
    with pytest.raises(CoreException, match="unique"):
        PostgresFTSSearchAdapter(
            spec=spec,
            codec=spec.resolved_read_codec,
            index_relation=("public", "i"),
            relation=("public", "v"),
            index_heap_relation=("public", "h"),
            fts_groups={"A": ("a",), "B": ("b",)},
            client=MagicMock(),
            model_type=_Entity,
            introspector=MagicMock(),
            tenant_provider=None,
            tenant_aware=False,
            join_pairs=[("id", "c1"), ("id", "c2")],
        )


# ....................... #


def _pgroonga(
    join_pairs: list[tuple[str, str]] | None = None,
    index_field_map: dict[str, str] | None = None,
) -> PostgresPGroongaSearchAdapter[_Entity]:
    spec = _spec()
    # ``a`` lives on the heap under the same name, so an index-first cap can order by it.

    return PostgresPGroongaSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=("public", "i"),
        index_heap_relation=("public", "h"),
        client=MagicMock(),
        model_type=_Entity,
        introspector=MagicMock(),
        tenant_provider=None,
        tenant_aware=False,
        join_pairs=join_pairs,
        index_field_map={"a": "a", **(index_field_map or {})},
    )


class TestACapOrdersByTheRecordIdOnce:
    """A resolved page order already ends on the id; the cap must not repeat it as a key."""

    def test_on_the_heap_for_an_index_first_cap(self) -> None:
        adapter = _pgroonga()
        order = adapter._heap_cap_order({"a": "asc", "id": "asc"}, [("id", "id")])  # pyright: ignore[reportPrivateUsage]

        assert order is not None
        assert order.as_string() == '"t"."a" ASC NULLS FIRST, "t"."id" ASC'

    def test_on_the_filtered_rows_for_a_filter_first_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The page's own ORDER BY is the gateway's (column types need a catalog); stand it in.
        async def order_by_clause(
            _self: object, sorts: object, *, table_alias: str | None = None
        ) -> sql.Composable:
            alias = sql.Identifier(table_alias or "")
            return sql.SQL('{}."a" ASC, {}."id" ASC').format(alias, alias)

        monkeypatch.setattr(PostgresPGroongaSearchAdapter, "order_by_clause", order_by_clause)
        adapter = _pgroonga()
        order, _ = asyncio.run(
            adapter._capped_order(  # pyright: ignore[reportPrivateUsage]
                {"a": "asc", "id": "asc"}, coalesced=False, join_pairs=[("id", "id")]
            )
        )

        assert order.as_string() == '"f"."a" ASC, "f"."id" ASC'

    def test_a_key_the_sort_does_not_name_still_closes_the_order(self) -> None:
        adapter = _pgroonga([("id", "id"), ("tenant_id", "tid")])
        order = adapter._heap_cap_order(  # pyright: ignore[reportPrivateUsage]
            {"id": "asc"}, [("id", "id"), ("tenant_id", "tid")]
        )

        assert order is not None
        assert order.as_string() == '"t"."id" ASC, "tenant_id"'

    def test_without_a_sort_the_keys_alone_order_it(self) -> None:
        adapter = _pgroonga()
        order, _ = asyncio.run(
            adapter._capped_order(None, coalesced=False, join_pairs=[("id", "id")])  # pyright: ignore[reportPrivateUsage]
        )

        assert order.as_string() == '"id"'

    def test_a_key_kept_when_the_sort_reads_another_heap_column(self) -> None:
        # ``tenant_id`` joins on heap column ``tid`` but is indexed from ``other``: ordering by
        # ``t.other`` does not order by the scored key ``t.tid``, so the key must stay.
        join = [("id", "id"), ("tenant_id", "tid")]
        adapter = _pgroonga(join, {"tenant_id": "other"})
        order = adapter._heap_cap_order({"tenant_id": "asc", "id": "asc"}, join)  # pyright: ignore[reportPrivateUsage]

        assert order is not None
        assert order.as_string() == '"t"."other" ASC NULLS FIRST, "t"."id" ASC, "tenant_id"'

    def test_a_coalesced_cap_keeps_a_key_whose_heap_column_differs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Coalesced, the sort orders the heap by the field's own column; the scored key is the
        # join's heap column, which is a different one here.
        async def order_by_clause(
            _self: object, sorts: object, *, table_alias: str | None = None
        ) -> sql.Composable:
            alias = sql.Identifier(table_alias or "")
            return sql.SQL('{}."tenant_id" ASC, {}."id" ASC').format(alias, alias)

        monkeypatch.setattr(PostgresPGroongaSearchAdapter, "order_by_clause", order_by_clause)
        join = [("id", "id"), ("tenant_id", "tid")]
        order, _ = asyncio.run(
            _pgroonga(join)._capped_order(  # pyright: ignore[reportPrivateUsage]
                {"tenant_id": "asc", "id": "asc"}, coalesced=True, join_pairs=join
            )
        )

        assert order.as_string() == '"t"."tenant_id" ASC, "t"."id" ASC, "tenant_id"'

    def test_a_filtered_cap_drops_a_key_the_join_equates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The filtered CTE carries the projection column, which the join equates with the
        # scored key's heap column, so ordering by it already orders by the key.
        async def order_by_clause(
            _self: object, sorts: object, *, table_alias: str | None = None
        ) -> sql.Composable:
            alias = sql.Identifier(table_alias or "")
            return sql.SQL('{}."tenant_id" ASC, {}."id" ASC').format(alias, alias)

        monkeypatch.setattr(PostgresPGroongaSearchAdapter, "order_by_clause", order_by_clause)
        join = [("id", "id"), ("tenant_id", "tid")]
        order, _ = asyncio.run(
            _pgroonga(join)._capped_order(  # pyright: ignore[reportPrivateUsage]
                {"tenant_id": "asc", "id": "asc"}, coalesced=False, join_pairs=join
            )
        )

        assert order.as_string() == '"f"."tenant_id" ASC, "f"."id" ASC'
