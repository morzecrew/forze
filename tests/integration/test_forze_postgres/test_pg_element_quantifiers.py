"""Integration tests for ``$not`` and element quantifiers (``$any``, ``$all``, ``$none``)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
)
from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryFilterExpressionParser,
)
from forze.application.contracts.querying.internal.matching import _match_expr
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways import PostgresReadGateway
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.unit._gateway_codec_helpers import codec_for


@pytest.mark.asyncio
async def test_element_any_on_native_text_array(pg_client: PostgresClient) -> None:
    t = f"elem_any_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
        title: str
        tags: list[str]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            tags text[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_any_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="a", tags=["urgent", "ops"]))
    await cmd.create(_Create(title="b", tags=["ops"]))
    await cmd.create(_Create(title="c", tags=[]))

    urgent = {"$values": {"tags": {"$any": "urgent"}}}
    assert await query.count(urgent) == 1
    row = await query.find(urgent)
    assert row is not None and row.title == "a"

    all_ops = {"$values": {"tags": {"$all": {"$eq": "ops"}}}}
    # ``b`` has only ``ops``; empty ``c`` satisfies ``$all`` vacuously.
    assert await query.count(all_ops) == 2

    none_urgent = {"$values": {"tags": {"$none": "urgent"}}}
    assert await query.count(none_urgent) == 2


@pytest.mark.asyncio
async def test_not_combinator(pg_client: PostgresClient) -> None:
    t = f"elem_not_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str

    class _Create(CreateDocumentCmd):
        title: str

    class _Update(BaseDTO):
        title: str | None = None

    class _Read(ReadDocument):
        title: str

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_not_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="keep"))
    await cmd.create(_Create(title="drop"))

    filt = {"$not": {"$values": {"title": "drop"}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "keep"


@pytest.mark.asyncio
async def test_element_any_on_jsonb_object_array(pg_client: PostgresClient) -> None:
    t = f"elem_obj_{uuid4().hex[:12]}"

    class _Item(BaseModel):
        status: str
        qty: int

    class _Doc(Document):
        title: str
        items: list[_Item]

    class _Create(CreateDocumentCmd):
        title: str
        items: list[_Item]

    class _Update(BaseDTO):
        title: str | None = None
        items: list[_Item] | None = None

    class _Read(ReadDocument):
        title: str
        items: list[_Item]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            items jsonb NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_obj_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(
        _Create(
            title="match",
            items=[_Item(status="open", qty=2), _Item(status="closed", qty=1)],
        ),
    )
    await cmd.create(
        _Create(title="miss", items=[_Item(status="closed", qty=5)]),
    )

    filt = {
        "$values": {
            "items": {
                "$any": {
                    "$values": {
                        "status": "open",
                        "qty": {"$gte": 2},
                    },
                },
            },
        },
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "match"

    none_open = {
        "$values": {
            "items": {"$none": {"$values": {"status": "open"}}},
        },
    }
    assert await query.count(none_open) == 1
    assert (await query.find(none_open)).title == "miss"


@pytest.mark.asyncio
async def test_element_any_scalar_ordering(pg_client: PostgresClient) -> None:
    t = f"elem_ord_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        scores: list[int]

    class _Create(CreateDocumentCmd):
        title: str
        scores: list[int]

    class _Update(BaseDTO):
        title: str | None = None
        scores: list[int] | None = None

    class _Read(ReadDocument):
        title: str
        scores: list[int]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            scores int[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_ord_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="high", scores=[5, 15]))
    await cmd.create(_Create(title="low", scores=[1, 2]))

    filt = {"$values": {"scores": {"$any": {"$gte": 10}}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "high"


@pytest.mark.asyncio
async def test_element_any_decimal_on_jsonb_scalar_array(pg_client: PostgresClient) -> None:
    """Decimal elements live in jsonb as strings; a Decimal operand still compares
    numerically (9.5 < 10.5 < 100.25 — lexically "100.25" sorts before "9.5")."""

    from decimal import Decimal

    t = f"elem_dec_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        prices: list[Decimal]

    class _Create(CreateDocumentCmd):
        title: str
        prices: list[Decimal]

    class _Update(BaseDTO):
        title: str | None = None
        prices: list[Decimal] | None = None

    class _Read(ReadDocument):
        title: str
        prices: list[Decimal]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            prices jsonb NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_dec_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="high", prices=[Decimal("9.5"), Decimal("100.25")]))
    await cmd.create(_Create(title="low", prices=[Decimal("1.5"), Decimal("9.5")]))

    filt = {"$values": {"prices": {"$any": {"$gte": Decimal("10.5")}}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "high"

    eq_filt = {"$values": {"prices": {"$any": {"$eq": Decimal("1.5")}}}}
    assert await query.count(eq_filt) == 1

    in_filt = {"$values": {"prices": {"$any": {"$in": [Decimal("100.25")]}}}}
    assert await query.count(in_filt) == 1


@pytest.mark.asyncio
async def test_element_any_int_operand_on_decimal_typed_jsonb_array(
    pg_client: PostgresClient,
) -> None:
    """An int operand against a Decimal-annotated jsonb array compares numerically too —
    jsonb type ordering ranks the stored decimal strings above every number, so
    ``$gte: 5`` would otherwise match rows whose only decimal is 1.5."""

    from decimal import Decimal

    t = f"elem_dec_int_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        amounts: list[Decimal | int]

    class _Create(CreateDocumentCmd):
        title: str
        amounts: list[Decimal | int]

    class _Update(BaseDTO):
        title: str | None = None
        amounts: list[Decimal | int] | None = None

    class _Read(ReadDocument):
        title: str
        amounts: list[Decimal | int]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            amounts jsonb NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_dec_int_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="high", amounts=[Decimal("10.5"), 2]))
    await cmd.create(_Create(title="low", amounts=[Decimal("1.5"), 3]))

    filt = {"$values": {"amounts": {"$any": {"$gte": 5}}}}
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.title == "high"

    lte_filt = {"$values": {"amounts": {"$any": {"$lte": 2}}}}
    assert await query.count(lte_filt) == 2

    eq_filt = {"$values": {"amounts": {"$any": {"$eq": 3}}}}
    assert await query.count(eq_filt) == 1


@pytest.mark.asyncio
async def test_not_with_nested_or(pg_client: PostgresClient) -> None:
    t = f"elem_not_or_{uuid4().hex[:12]}"

    class _Doc(Document):
        status: str

    class _Create(CreateDocumentCmd):
        status: str

    class _Update(BaseDTO):
        status: str | None = None

    class _Read(ReadDocument):
        status: str

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            status text NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_not_or_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(status="active"))
    await cmd.create(_Create(status="archived"))
    await cmd.create(_Create(status="pending"))

    filt = {
        "$not": {
            "$or": [
                {"$values": {"status": "archived"}},
                {"$values": {"status": "pending"}},
            ],
        },
    }
    assert await query.count(filt) == 1
    row = await query.find(filt)
    assert row is not None and row.status == "active"


@pytest.mark.asyncio
async def test_element_quantifiers_combined_with_and(pg_client: PostgresClient) -> None:
    t = f"elem_and_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
        title: str
        tags: list[str]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            tags text[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_and_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="yes", tags=["api", "urgent"]))
    await cmd.create(_Create(title="no_tag", tags=["api"]))
    await cmd.create(_Create(title="no_title", tags=["urgent"]))

    filt = {
        "$and": [
            {"$values": {"title": "yes"}},
            {"$values": {"tags": {"$any": "urgent"}}},
        ],
    }
    assert await query.count(filt) == 1
    assert (await query.find(filt)).title == "yes"


@pytest.mark.asyncio
async def test_element_none_on_scalar_array(pg_client: PostgresClient) -> None:
    t = f"elem_none_{uuid4().hex[:12]}"

    class _Doc(Document):
        title: str
        tags: list[str]

    class _Create(CreateDocumentCmd):
        title: str
        tags: list[str]

    class _Update(BaseDTO):
        title: str | None = None
        tags: list[str] | None = None

    class _Read(ReadDocument):
        title: str
        tags: list[str]

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            tags text[] NOT NULL
        );
        """
    )
    spec = DocumentSpec(
        name="elem_none_ns",
        read=_Read,
        write={"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update},
    )
    ctx = document_context(pg_client, t)
    cmd = ctx.document.command(spec)
    query = ctx.document.query(spec)

    await cmd.create(_Create(title="clean", tags=["api"]))
    await cmd.create(_Create(title="dirty", tags=["urgent", "api"]))

    filt = {"$values": {"tags": {"$none": "urgent"}}}
    assert await query.count(filt) == 1
    assert (await query.find(filt)).title == "clean"


# ....................... #
# A quantified array named once, and its semantics measured against the mock evaluator.

_QUANTIFIERS = ("$any", "$all", "$none")


def _read_gateway(
    client: PostgresClient,
    relation: str,
    model: type[BaseModel],
) -> PostgresReadGateway[Any]:
    return PostgresReadGateway(
        relation=("public", relation),
        client=client,
        model_type=model,
        codec=codec_for(model),
        introspector=PostgresIntrospector(client=client),
        tenant_aware=False,
    )


async def _matching(
    client: PostgresClient,
    gateway: PostgresReadGateway[Any],
    relation: str,
    key: str,
    filters: dict[str, Any],
) -> set[Any]:
    where, params = await gateway.where_clause(filters)
    rows = await client.fetch_all(
        sql.SQL("SELECT {} FROM {} WHERE {}").format(
            sql.Identifier(key), sql.Identifier(relation), where
        ),
        params,
    )

    return {row[key] for row in rows}


async def _mock_matching(
    client: PostgresClient,
    relation: str,
    key: str,
    filters: dict[str, Any],
) -> set[Any]:
    rows = await client.fetch_all(
        sql.SQL("SELECT * FROM {}").format(sql.Identifier(relation)),
    )
    expr = QueryFilterExpressionParser.parse(filters)

    return {row[key] for row in rows if _match_expr(dict(row), expr)}


def _scans_of(plan: Any, relation: str) -> int:
    """How many plan nodes read *relation* — one per copy of a subquery over it."""

    if isinstance(plan, dict):
        own = 1 if plan.get("Relation Name") == relation else 0

        return own + sum(_scans_of(v, relation) for v in plan.values())

    if isinstance(plan, list):
        return sum(_scans_of(v, relation) for v in plan)

    return 0


class _Offer(BaseModel):
    available: bool
    size: str
    price: int


class _Product(BaseModel):
    id: int
    name: str
    offers: list[_Offer] | None = None


@pytest.mark.asyncio
@pytest.mark.parametrize("quantifier", _QUANTIFIERS)
async def test_a_quantified_view_column_runs_its_subquery_once_per_row(
    pg_client: PostgresClient,
    quantifier: str,
) -> None:
    products = f"elem_view_{uuid4().hex[:10]}"
    offers = f"{products}_offers"
    view = f"{products}_v"

    await pg_client.execute(
        f"""
        CREATE TABLE {products} (id int PRIMARY KEY, name text NOT NULL);
        CREATE TABLE {offers} (
            id serial PRIMARY KEY,
            product_id int NOT NULL,
            available boolean NOT NULL,
            size text NOT NULL,
            price int NOT NULL
        );
        CREATE VIEW {view} AS
            SELECT p.id, p.name, (
                SELECT jsonb_agg(
                    jsonb_build_object('available', o.available, 'size', o.size, 'price', o.price)
                    ORDER BY o.id
                )
                FROM {offers} o WHERE o.product_id = p.id
            ) AS offers
            FROM {products} p;
        INSERT INTO {products} VALUES (1, 'shirt'), (2, 'coat'), (3, 'hat');
        INSERT INTO {offers} (product_id, available, size, price)
            VALUES (1, true, 'M', 100), (1, false, 'L', 100), (2, false, 'M', 300);
        """
    )
    gateway = _read_gateway(pg_client, view, _Product)
    filters = {
        "$values": {
            "offers": {quantifier: {"$values": {"available": True, "size": "M"}}},
        },
    }

    where, params = await gateway.where_clause(filters)
    rows = await pg_client.fetch_all(
        sql.SQL("EXPLAIN (FORMAT JSON) SELECT id FROM {} WHERE {}").format(
            sql.Identifier(view), where
        ),
        params,
    )
    plan = rows[0]["QUERY PLAN"]

    # The view's offers aggregate is planned once: before, every reference to the
    # column (eight of them for $any) inlined another copy of it — five SubPlans.
    assert _scans_of(plan, offers) == 1, plan
    assert await _matching(pg_client, gateway, view, "id", filters) == await _mock_matching(
        pg_client, view, "id", filters
    )


class _OddRow(BaseModel):
    label: str
    arr: Any = None
    tags: list[str] | None = None


@pytest.mark.asyncio
@pytest.mark.parametrize("quantifier", _QUANTIFIERS)
async def test_quantifiers_on_null_scalar_object_and_empty_arrays_match_the_mock(
    pg_client: PostgresClient,
    quantifier: str,
) -> None:
    t = f"elem_odd_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (label text PRIMARY KEY, arr jsonb, tags text[]);
        INSERT INTO {t} VALUES
            ('sql_null', NULL, NULL),
            ('json_null', 'null', '{{}}'),
            ('number', '5', '{{a}}'),
            ('string', '"x"', '{{b}}'),
            ('object', '{{"k": "a"}}', '{{a,b}}'),
            ('empty', '[]', NULL),
            ('hit', '[{{"k": "a"}}]', '{{a,a}}'),
            ('miss', '[{{"k": "b"}}]', '{{b,c}}'),
            ('mixed', '[{{"k": "a"}}, {{"k": "b"}}]', '{{c}}');
        """
    )
    gateway = _read_gateway(pg_client, t, _OddRow)

    for filters in (
        {"$values": {"arr": {quantifier: {"$values": {"k": "a"}}}}},
        {"$values": {"tags": {quantifier: "a"}}},
    ):
        expected = await _mock_matching(pg_client, t, "label", filters)

        assert await _matching(pg_client, gateway, t, "label", filters) == expected, filters


class _UntypedRow(BaseModel):
    label: str
    offers: list[dict[Any, Any]] | None = None


_UNTYPED_FILTERS: tuple[dict[str, Any], ...] = (
    {"available": True, "size": {"$in": ["M"]}, "price": {"$lte": 20000}},
    {"price": {"$gt": 100}},
    {"price": {"$gte": 15000, "$lt": 25000}},
    {"price": {"$eq": 25000.0}},
    {"price": {"$neq": 100}},
    {"price": {"$in": [100, 15000]}},
    {"price": {"$nin": [100, 15000]}},
    {"available": {"$neq": True}},
    {"size": "M"},
    {"price": {"$eq": 1.0}},
    {"price": {"$eq": 1}},  # stored 1 and 1.0 alike: jsonb numeric equality
    {"price": {"$lt": 100}},
    {"price": {"$lt": 2**70}},
    {"size": {"$in": ["S", None]}},
    {"size": {"$nin": [None]}},
)


@pytest.mark.asyncio
@pytest.mark.parametrize("inner", _UNTYPED_FILTERS, ids=lambda f: str(f))
@pytest.mark.parametrize("quantifier", _QUANTIFIERS)
async def test_untyped_object_elements_compare_like_the_mock(
    pg_client: PostgresClient,
    quantifier: str,
    inner: dict[str, Any],
) -> None:
    t = f"elem_untyped_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (label text PRIMARY KEY, offers jsonb);
        INSERT INTO {t} VALUES
            ('hit', '[{{"available": true, "size": "M", "price": 15000}}]'),
            ('pricey', '[{{"available": true, "size": "M", "price": 25000}}]'),
            ('sold_out', '[{{"available": false, "size": "M", "price": 100}}]'),
            ('strings', '[{{"available": "true", "size": "M", "price": "100"}}]'),
            ('partial', '[{{"size": "M"}}]'),
            ('nulls', '[{{"available": null, "size": null, "price": null}}]'),
            ('mixed', '[{{"available": false, "size": "S", "price": 1}},
                        {{"available": true, "size": "M", "price": 20000}}]'),
            ('fraction', '[{{"available": true, "size": "L", "price": 99.5}}]'),
            ('odd_elements', '[5, null, {{"available": true, "size": "S", "price": 1.0}}]'),
            ('none', NULL);
        """
    )
    gateway = _read_gateway(pg_client, t, _UntypedRow)
    filters = {"$values": {"offers": {quantifier: {"$values": inner}}}}

    expected = await _mock_matching(pg_client, t, "label", filters)

    assert await _matching(pg_client, gateway, t, "label", filters) == expected


@pytest.mark.asyncio
async def test_an_untyped_element_refuses_an_operand_json_cannot_hold(
    pg_client: PostgresClient,
) -> None:
    t = f"elem_untyped_dec_{uuid4().hex[:10]}"
    await pg_client.execute(f"CREATE TABLE {t} (label text PRIMARY KEY, offers jsonb)")
    gateway = _read_gateway(pg_client, t, _UntypedRow)

    with pytest.raises(CoreException) as caught:
        await gateway.count(
            {"$values": {"offers": {"$any": {"$values": {"price": {"$lte": Decimal("20000")}}}}}},
        )

    assert caught.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
    assert "'offers.price'" in caught.value.summary


class _Item(BaseModel):
    k: str | None = None
    tags: list[str] | None = None


class _ReadableRow(BaseModel):
    label: str
    o: list[dict[Any, Any]] | None = None
    items: list[_Item] | None = None
    tags: list[str] | None = None
    nums: list[int] | None = None


async def _readable_rows(client: PostgresClient) -> str:
    """Rows whose elements a predicate cannot always read: a missing key, a JSON null, a
    scalar under an object predicate, a NULL native element, a null or missing sub-array."""

    t = f"elem_readable_{uuid4().hex[:10]}"
    await client.execute(
        f"""
        CREATE TABLE {t} (label text PRIMARY KEY, o jsonb, items jsonb, tags text[], nums int[]);
        INSERT INTO {t} VALUES
            ('null_value', '[{{"k": null}}]', '[{{"k": null, "tags": null}}]', '{{NULL}}', '{{NULL}}'),
            ('missing', '[{{}}]', '[{{}}]', '{{a,NULL}}', '{{1,NULL}}'),
            ('scalar', '[5]', '[5]', '{{a}}', '{{1}}'),
            ('json_null', '[null]', '[null]', '{{}}', '{{}}'),
            ('text', '[{{"k": "M"}}]', '[{{"k": "M", "tags": ["x"]}}]', '{{b}}', '{{2}}'),
            ('number', '[{{"k": 6}}]', '[{{"k": "6", "tags": []}}]', '{{a,b}}', '{{1,2}}'),
            ('mixed', '[{{"k": "M"}}, 5, {{"k": null}}]',
                      '[{{"k": "M", "tags": ["x", "y"]}}, null]', '{{a,a}}', '{{1,1}}'),
            ('none', NULL, NULL, NULL, NULL);
        """
    )

    return t


_READABLE_FILTERS: tuple[dict[str, Any], ...] = (
    {"o": {"$values": {"k": "M"}}},
    {"o": {"$values": {"k": {"$neq": "M"}}}},
    {"o": {"$values": {"k": {"$in": [None, "M"]}}}},
    {"o": {"$values": {"k": {"$in": [None, 6]}}}},
    {"o": {"$values": {"k": {"$nin": [None]}}}},
    {"o": {"$values": {"k": {"$nin": [None, "M"]}}}},
    {"items": {"$values": {"k": "M"}}},
    {"items": {"$values": {"k": {"$neq": "M"}}}},
    {"tags": "a"},
    {"tags": {"$neq": "a"}},
    {"nums": {"$gte": 1}},
)


@pytest.mark.asyncio
@pytest.mark.parametrize("inner", _READABLE_FILTERS, ids=lambda f: str(f))
@pytest.mark.parametrize("quantifier", _QUANTIFIERS)
async def test_elements_a_predicate_cannot_read_match_the_mock(
    pg_client: PostgresClient,
    quantifier: str,
    inner: dict[str, Any],
) -> None:
    t = await _readable_rows(pg_client)
    gateway = _read_gateway(pg_client, t, _ReadableRow)
    ((path, predicate),) = inner.items()
    filters = {"$values": {path: {quantifier: predicate}}}

    expected = await _mock_matching(pg_client, t, "label", filters)

    assert await _matching(pg_client, gateway, t, "label", filters) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("quantifier", ["$all", "$none"])
async def test_a_null_or_missing_sub_array_is_vacuous_under_all_and_none(
    pg_client: PostgresClient,
    quantifier: str,
) -> None:
    t = await _readable_rows(pg_client)
    gateway = _read_gateway(pg_client, t, _ReadableRow)
    filters = {"$values": {"items": {"$any": {"$values": {"tags": {quantifier: "z"}}}}}}

    matched = await _matching(pg_client, gateway, t, "label", filters)

    # An item whose `tags` is null or missing holds vacuously, so its row matches.
    assert {"null_value", "missing"} <= matched
    assert matched == await _mock_matching(pg_client, t, "label", filters)


class _NativeJsonRow(BaseModel):
    label: str
    jb: list[Any] | None = None
    js: list[Any] | None = None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inner",
    [{"jb": {"k": {"$neq": 1}}}, {"js": {"k": {"$neq": "a"}}}, {"js": {"k": {"$neq": 2}}}],
    ids=["jsonb[]", "json[]", "json[]-number"],
)
@pytest.mark.parametrize("quantifier", _QUANTIFIERS)
async def test_a_native_json_array_matches_the_mock_on_scalar_elements(
    pg_client: PostgresClient,
    quantifier: str,
    inner: dict[str, Any],
) -> None:
    t = f"elem_native_json_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (label text PRIMARY KEY, jb jsonb[], js json[]);
        INSERT INTO {t} VALUES
            ('object_and_scalar', ARRAY['{{"k": 2}}', '5']::jsonb[], ARRAY['{{"k": "b"}}', '5']::json[]),
            ('objects', ARRAY['{{"k": 2}}', '{{}}']::jsonb[], ARRAY['{{"k": "b"}}', '{{}}']::json[]),
            ('match', ARRAY['{{"k": 1}}']::jsonb[], ARRAY['{{"k": "a"}}']::json[]),
            ('null_element', ARRAY['null']::jsonb[], ARRAY['null']::json[]),
            ('none', NULL, NULL);
        """
    )
    gateway = _read_gateway(pg_client, t, _NativeJsonRow)
    ((path, fields),) = inner.items()
    filters = {"$values": {path: {quantifier: {"$values": fields}}}}

    expected = await _mock_matching(pg_client, t, "label", filters)

    assert await _matching(pg_client, gateway, t, "label", filters) == expected
