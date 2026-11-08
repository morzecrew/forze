"""Unit tests for :class:`PsycopgFilterSqlCache`: filter SQL rendered once per shape."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.base.serialization import default_model_codec
from forze_postgres.kernel.catalog.introspect import (
    PostgresColumnTypes,
    PostgresIntrospector,
    PostgresType,
)
from forze_postgres.kernel.client import PostgresClient
from forze_postgres.kernel.gateways import PostgresReadGateway
from forze_postgres.kernel.sql.query.render import (
    PsycopgFilterSqlCache,
    PsycopgQueryRenderer,
)

# ----------------------- #


class _Address(BaseModel):
    city: str


class _Row(BaseModel):
    id: UUID
    status: str
    total: int
    limit: int
    note: str | None = None
    tags: list[str]
    labels: list[str]
    path: str
    address: _Address
    created_at: datetime


def _t(base: str, *, is_array: bool = False, not_null: bool = True) -> PostgresType:
    return PostgresType(base=base, is_array=is_array, not_null=not_null)


_TYPES: PostgresColumnTypes = {
    "id": _t("uuid"),
    "status": _t("text"),
    "total": _t("int4"),
    "limit": _t("int4"),
    "note": _t("text", not_null=False),
    "tags": _t("text", is_array=True),
    "labels": _t("jsonb"),
    "path": _t("ltree"),
    "address": _t("jsonb"),
    "created_at": _t("timestamptz"),
}

_U = [uuid4() for _ in range(4)]

_SAME_SHAPE: list[tuple[dict[str, Any], dict[str, Any]]] = [
    ({"$values": {"status": "open"}}, {"$values": {"status": "paid"}}),
    ({"$values": {"status": {"$neq": "a"}}}, {"$values": {"status": {"$neq": "b"}}}),
    (
        {"$values": {"total": {"$gte": 1, "$lt": 9}}},
        {"$values": {"total": {"$gte": 2, "$lt": 8}}},
    ),
    # One array parameter, whatever its length.
    ({"$values": {"id": {"$in": [_U[0], _U[1]]}}}, {"$values": {"id": {"$in": [str(_U[2])]}}}),
    ({"$values": {"id": {"$nin": [_U[0]]}}}, {"$values": {"id": {"$nin": [_U[1], _U[2]]}}}),
    ({"$values": {"status": {"$like": "a%"}}}, {"$values": {"status": {"$like": "%b"}}}),
    ({"$values": {"status": {"$ilike": "a%"}}}, {"$values": {"status": {"$ilike": "%b"}}}),
    ({"$values": {"status": {"$regex": "^a"}}}, {"$values": {"status": {"$regex": "b$"}}}),
    ({"$values": {"tags": {"$superset": ["a"]}}}, {"$values": {"tags": {"$superset": ["b", "c"]}}}),
    ({"$values": {"tags": {"$disjoint": ["a"]}}}, {"$values": {"tags": {"$disjoint": ["b"]}}}),
    ({"$values": {"tags": ["a"]}}, {"$values": {"tags": ["b", "c"]}}),
    (
        {"$values": {"path": {"$descendant_of": "a.b"}}},
        {"$values": {"path": {"$descendant_of": "c"}}},
    ),
    ({"$values": {"address.city": "Berlin"}}, {"$values": {"address.city": "Paris"}}),
    (
        {"$values": {"created_at": {"$gte": "2026-01-01T00:00:00Z"}}},
        {"$values": {"created_at": {"$gte": "2025-05-05T10:00:00+02:00"}}},
    ),
    ({"$not": {"$values": {"status": "a"}}}, {"$not": {"$values": {"status": "b"}}}),
    (
        {"$or": [{"$values": {"status": "a"}}, {"$values": {"total": 1}}]},
        {"$or": [{"$values": {"status": "b"}}, {"$values": {"total": 2}}]},
    ),
    (
        {"$and": [{"$fields": {"total": {"$lt": "limit"}}}, {"$values": {"status": "a"}}]},
        {"$and": [{"$fields": {"total": {"$lt": "limit"}}}, {"$values": {"status": "b"}}]},
    ),
    # Values that look like SQL stay parameters: the text never holds them.
    (
        {"$values": {"status": "x'); DROP TABLE t; --"}},
        {"$values": {"status": "%s {} '\"\\"}},
    ),
]


def _parse(filters: dict[str, Any]) -> QueryExpr:
    return QueryFilterExpressionParser.parse(filters)


def _fresh(
    expr: QueryExpr,
    types: PostgresColumnTypes = _TYPES,
    alias: str | None = None,
) -> tuple[str, list[Any]]:
    query, params = PsycopgQueryRenderer(
        types=types,
        model_type=_Row,
        table_alias=alias,
    ).render(expr)

    return query.as_string(None), params


def _cached(
    cache: PsycopgFilterSqlCache,
    expr: QueryExpr,
    types: PostgresColumnTypes = _TYPES,
    alias: str | None = None,
) -> tuple[str, list[Any]]:
    query, params = cache.render(expr, types=types, table_alias=alias)

    return query.as_string(None), params


def _entries(cache: PsycopgFilterSqlCache) -> int:
    return len(cache._entries)  # pyright: ignore[reportPrivateUsage]


# ....................... #


class TestPsycopgFilterSqlCache:
    """Tests for :class:`PsycopgFilterSqlCache`."""

    @pytest.mark.parametrize(("first", "second"), _SAME_SHAPE)
    def test_same_shape_reuses_the_sql_and_binds_the_new_values(
        self,
        first: dict[str, Any],
        second: dict[str, Any],
    ) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)

        first_query, first_params = cache.render(_parse(first), types=_TYPES, table_alias=None)
        second_query, second_params = cache.render(_parse(second), types=_TYPES, table_alias=None)

        assert _entries(cache) == 1
        assert second_query is first_query
        assert (first_query.as_string(None), first_params) == _fresh(_parse(first))
        assert (second_query.as_string(None), second_params) == _fresh(_parse(second))
        assert second_params != first_params

    def test_values_never_reach_the_text(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        _cached(cache, _parse({"$values": {"status": "open"}}))

        text, params = _cached(cache, _parse({"$values": {"status": "x'); DROP TABLE t; --"}}))

        assert text == '"status" = %s'
        assert params == ["x'); DROP TABLE t; --"]

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            # Field.
            (QueryField("status", "$eq", "a"), QueryField("note", "$eq", "a")),
            # Operator.
            (QueryField("status", "$like", "a"), QueryField("status", "$ilike", "a")),
            (QueryField("total", "$gt", 1), QueryField("total", "$gte", 1)),
            # A unary operand picks the SQL.
            (QueryField("note", "$null", True), QueryField("note", "$null", False)),
            (QueryField("tags", "$empty", True), QueryField("tags", "$empty", False)),
            # Operand type.
            (QueryField("status", "$eq", "1"), QueryField("status", "$eq", 1)),
            # Nesting.
            (
                _parse({"$and": [{"$values": {"status": "a"}}, {"$values": {"total": 1}}]}),
                _parse({"$or": [{"$values": {"status": "a"}}, {"$values": {"total": 1}}]}),
            ),
            (_parse({"$values": {"status": "a"}}), _parse({"$not": {"$values": {"status": "a"}}})),
            (
                _parse(
                    {
                        "$and": [
                            {"$values": {"status": "a"}},
                            {"$or": [{"$values": {"total": 1}}, {"$values": {"limit": 2}}]},
                        ],
                    }
                ),
                _parse(
                    {
                        "$and": [
                            {"$or": [{"$values": {"status": "a"}}, {"$values": {"total": 1}}]},
                            {"$values": {"limit": 2}},
                        ],
                    }
                ),
            ),
            # Field compare.
            (
                _parse({"$fields": {"total": {"$lt": "limit"}}}),
                _parse({"$fields": {"total": {"$gt": "limit"}}}),
            ),
        ],
    )
    def test_each_non_value_difference_renders_its_own_sql(
        self,
        first: QueryExpr,
        second: QueryExpr,
    ) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)

        assert _cached(cache, first) == _fresh(first)
        assert _cached(cache, second) == _fresh(second)
        assert _entries(cache) == 2

    def test_null_true_then_false_is_not_served_the_cached_is_null(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)

        assert _cached(cache, QueryField("note", "$null", True))[0] == '"note" IS NULL'
        assert _cached(cache, QueryField("note", "$null", False))[0] == '"note" IS NOT NULL'

    def test_an_operand_type_the_column_refuses_still_raises(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        _cached(cache, QueryField("tags", "$eq", ["a"]))

        with pytest.raises(CoreException, match="requires a list/tuple value"):
            _cached(cache, QueryField("tags", "$eq", "a"))

    def test_a_value_the_column_type_refuses_still_raises_on_a_hit(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        _cached(cache, QueryField("id", "$eq", str(uuid4())))

        with pytest.raises(CoreException):
            _cached(cache, QueryField("id", "$eq", "not-a-uuid"))

    def test_table_alias_is_part_of_the_key(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        expr = QueryField("status", "$eq", "a")

        assert _cached(cache, expr) == ('"status" = %s', ["a"])
        assert _cached(cache, expr, alias="v") == ('"v"."status" = %s', ["a"])
        assert _entries(cache) == 2

    def test_other_column_types_render_and_coerce_their_own_way(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        numeric = {**_TYPES, "total": _t("numeric")}

        assert _cached(cache, QueryField("total", "$gt", 5)) == _fresh(
            QueryField("total", "$gt", 5)
        )
        assert _cached(cache, QueryField("total", "$gt", 6), types=numeric) == (
            '"total" > %s',
            [Decimal(6)],
        )
        # An equal mapping that is another object is another key too.
        assert _cached(cache, QueryField("total", "$gt", 7), types=dict(_TYPES))[1] == [7]
        assert _entries(cache) == 3

    def test_element_quantifiers_are_never_cached(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)

        for value in ("a", "b"):
            expr = _parse(
                {
                    "$and": [
                        {"$values": {"status": value}},
                        {"$values": {"tags": {"$any": {"$in": [None, value]}}}},
                    ],
                }
            )
            assert _cached(cache, expr) == _fresh(expr)

        assert _entries(cache) == 0

    def test_an_element_predicate_that_binds_nothing_is_never_cached(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)

        # An empty ``$in`` over JSON elements is ``FALSE``, with nothing bound.
        for members in ([], ["a"]):
            expr = QueryElem("labels", "$any", QueryField(ELEM_SCALAR_FIELD, "$in", members))
            assert _cached(cache, expr) == _fresh(expr)

        assert _entries(cache) == 0

    def test_a_leaf_node_shared_by_two_places_is_never_cached(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        leaf = QueryField("total", "$eq", 5)

        assert _cached(cache, QueryAnd((leaf, leaf))) == ('("total" = %s AND "total" = %s)', [5, 5])
        assert _entries(cache) == 0

        distinct = QueryAnd((QueryField("total", "$eq", 5), QueryField("total", "$eq", 6)))
        assert _cached(cache, distinct)[1] == [5, 6]

    def test_is_bounded_least_recently_used_first(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row, max_entries=2)
        a = QueryField("status", "$eq", "a")
        b = QueryField("total", "$eq", 1)
        c = QueryField("limit", "$eq", 1)

        _cached(cache, a)
        _cached(cache, b)
        _cached(cache, a)
        _cached(cache, c)

        assert _entries(cache) == 2
        query_a, _ = cache.render(a, types=_TYPES, table_alias=None)
        query_c, _ = cache.render(c, types=_TYPES, table_alias=None)
        assert _entries(cache) == 2
        assert (
            cache.render(QueryField("status", "$eq", "z"), types=_TYPES, table_alias=None)[0]
            is query_a
        )
        assert (
            cache.render(QueryField("limit", "$eq", 9), types=_TYPES, table_alias=None)[0]
            is query_c
        )

    def test_clear_drops_every_entry(self) -> None:
        cache = PsycopgFilterSqlCache(model_type=_Row)
        _cached(cache, QueryField("status", "$eq", "a"))

        cache.clear()

        assert _entries(cache) == 0


# ....................... #


def _client() -> MagicMock:
    client = MagicMock(spec=PostgresClient)
    client.fetch_one = AsyncMock(return_value=None)
    client.fetch_all = AsyncMock(return_value=[])
    client.gather_concurrency_semaphore = MagicMock(return_value=asyncio.Semaphore(8))

    return client


def _introspector(types_by_schema: dict[str, PostgresColumnTypes]) -> MagicMock:
    intro = MagicMock(spec=PostgresIntrospector)

    async def _types(*, schema: str | None, relation: str) -> PostgresColumnTypes:
        return types_by_schema[schema or "public"]

    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.cache_partition_key = None

    return intro


class TestPostgresReadGatewayFilterSql:
    """The read gateway's filter SQL memo, tenancy and the eligibility read."""

    @pytest.mark.asyncio
    async def test_find_by_id_reuses_its_sql_with_each_new_id(self) -> None:
        client = _client()
        gw = PostgresReadGateway(
            relation=("public", "t"),
            client=client,
            model_type=_Row,
            codec=default_model_codec(_Row),
            introspector=_introspector({"public": _TYPES}),
            tenant_aware=False,
        )
        first, second = uuid4(), uuid4()

        await gw.find({"$values": {"id": first}})
        await gw.find({"$values": {"id": second}})

        [(_, first_params, *_), (stmt, second_params, *_)] = [
            call.args for call in client.fetch_one.await_args_list
        ]
        assert first_params == [first]
        assert second_params == [second]
        assert 'WHERE "id" = %s LIMIT 1' in stmt.as_string(None)
        assert _entries(gw._filter_sql_cache) == 1  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_tenant_predicate_stays_outside_the_memo(self) -> None:
        client = _client()
        tenant = {"id": uuid4()}
        gw = PostgresReadGateway(
            relation=("public", "t"),
            client=client,
            model_type=_Row,
            codec=default_model_codec(_Row),
            introspector=_introspector({"public": _TYPES}),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant["id"]),
        )
        a, b = tenant["id"], uuid4()

        where_a, params_a = await gw.where_clause({"$values": {"status": "x"}})
        tenant["id"] = b
        where_b, params_b = await gw.where_clause({"$values": {"status": "y"}})

        assert (
            where_a.as_string(None)
            == where_b.as_string(None)
            == ('"status" = %s AND "tenant_id" = %s')
        )
        assert params_a == ["x", a]
        assert params_b == ["y", b]
        assert _entries(gw._filter_sql_cache) == 1  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_a_schema_per_tenant_keys_its_own_column_types(self) -> None:
        tenant = {"id": uuid4()}
        a, b = tenant["id"], uuid4()
        numeric = {**_TYPES, "total": _t("numeric")}
        gw = PostgresReadGateway(
            relation=lambda tid: (f"s_{tid.hex}", "t"),
            client=_client(),
            model_type=_Row,
            codec=default_model_codec(_Row),
            introspector=_introspector({f"s_{a.hex}": _TYPES, f"s_{b.hex}": numeric}),
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant["id"]),
        )

        _, params_a = await gw.where_clause({"$values": {"total": {"$gt": 5}}})
        tenant["id"] = b
        _, params_b = await gw.where_clause({"$values": {"total": {"$gt": 5}}})

        assert params_a == [5, a]
        assert params_b == [Decimal(5), b]
        assert _entries(gw._filter_sql_cache) == 2  # pyright: ignore[reportPrivateUsage]
