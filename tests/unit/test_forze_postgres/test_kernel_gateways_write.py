"""Unit tests for ``forze_postgres.kernel.gateways.write``."""

import asyncio
import random
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Context, Decimal
from enum import Enum, IntEnum, StrEnum
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import attrs
import pytest
from psycopg.types.json import Json, Jsonb

from forze.application.contracts.guarantees import SerializedBy
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.client import PostgresClient
from forze_postgres.kernel.gateways import (
    PostgresReadGateway,
    PostgresWriteGateway,
)
from tests.unit._gateway_codec_helpers import write_codecs_for


class MyDoc(Document):
    name: str

class MyCreateDoc(CreateDocumentCmd):
    name: str

class MyUpdateDoc(BaseDTO):
    name: str | None = None

def _build_gateway() -> (
    tuple[PostgresWriteGateway[MyDoc, MyCreateDoc, MyUpdateDoc], MagicMock]
):
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock()
    client.fetch_one = AsyncMock()
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})
    introspector.get_primary_key_columns = AsyncMock(return_value=(ID_FIELD,))
    introspector.constraint_exists_for_columns = AsyncMock(return_value=True)

    read = MagicMock(spec=PostgresReadGateway)
    read.relation = ("public", "docs")
    read.client = client
    read.tenant_aware = False

    domain_codec, create_codec, update_codec = write_codecs_for(
        domain_type=MyDoc,
        create_type=MyCreateDoc,
        update_type=MyUpdateDoc,
    )

    gw = PostgresWriteGateway(
        relation=("public", "docs"),
        client=client,
        model_type=MyDoc,
        codec=domain_codec,
        introspector=introspector,
        read_gw=read,
        create_cmd_type=MyCreateDoc,
        update_cmd_type=MyUpdateDoc,
        create_codec=create_codec,
        update_codec=update_codec,
        strategy="application",
    )
    return gw, client

def _row(*, pk: UUID, name: str, ts: datetime) -> dict[str, object]:
    return {
        "id": pk,
        "rev": 1,
        "created_at": ts,
        "last_update_at": ts,
        "name": name,
    }

def test_adapt_value_for_write_coerces_numeric_and_preserves_null() -> None:
    gw, _ = _build_gateway()
    numeric_t = PostgresType(base="numeric", is_array=False, not_null=False)

    assert gw.adapt_value_for_write("12.5", t=numeric_t) == 12.5
    assert gw.adapt_value_for_write(None, t=numeric_t) is None

def test_adapt_value_for_write_wraps_each_element_of_a_json_array() -> None:
    from psycopg.types.json import Json, Jsonb

    gw, _ = _build_gateway()

    for base, wrapper in (("jsonb", Jsonb), ("json", Json)):
        out = gw.adapt_value_for_write(
            [{"a": 1}, [2]], t=PostgresType(base=base, is_array=True, not_null=False)
        )

        assert [type(x) for x in out] == [wrapper, wrapper], base
        assert [x.obj for x in out] == [{"a": 1}, [2]], base

def test_adapt_value_for_write_keeps_a_none_json_array_element_an_sql_null() -> None:
    gw, _ = _build_gateway()
    out = gw.adapt_value_for_write(
        [None, {"a": 1}], t=PostgresType(base="jsonb", is_array=True, not_null=False)
    )

    assert out[0] is None and out[1].obj == {"a": 1}

@pytest.mark.parametrize("value", [{"a": 1}, "ab"])
def test_adapt_value_for_write_refuses_a_json_array_value_that_is_not_a_list(value) -> None:
    gw, _ = _build_gateway()

    for base in ("jsonb", "json"):
        with pytest.raises(CoreException, match=rf"{base}\[\] column takes a list"):
            gw.adapt_value_for_write(
                value, t=PostgresType(base=base, is_array=True, not_null=False)
            )


class _Plain(Enum):
    OPEN = "open"


class _Text(StrEnum):
    OPEN = "open"


class _Number(IntEnum):
    ONE = 1


class _Mixed(str, Enum):
    OPEN = "open"


def test_adapt_value_for_write_stores_an_enum_member_as_its_value() -> None:
    gw, _ = _build_gateway()
    text = PostgresType(base="text", is_array=False, not_null=True)

    assert gw.adapt_value_for_write(_Plain.OPEN, t=text) == "open"
    assert gw.adapt_value_for_write(
        [_Plain.OPEN], t=PostgresType(base="text", is_array=True, not_null=True)
    ) == ["open"]
    assert gw.adapt_value_for_write(
        _Plain.OPEN, t=PostgresType(base="jsonb", is_array=False, not_null=True)
    ).obj == "open"
    # A StrEnum or IntEnum member is written as it was before.
    assert gw.adapt_value_for_write(_Text.OPEN, t=text) == str(_Text.OPEN)
    assert gw.adapt_value_for_write(
        _Number.ONE, t=PostgresType(base="int8", is_array=False, not_null=True)
    ) == 1
    # A (str, Enum) member that is no StrEnum changes: str() wrote "_Mixed.OPEN", unreadable.
    assert str(_Mixed.OPEN) == "_Mixed.OPEN"
    assert gw.adapt_value_for_write(_Mixed.OPEN, t=text) == "open"


class _Unset(Enum):
    NONE = None


@pytest.mark.parametrize("base", ["jsonb", "json", "text"])
def test_adapt_value_for_write_stores_a_none_valued_member_as_sql_null(base: str) -> None:
    gw, _ = _build_gateway()

    t = PostgresType(base=base, is_array=False, not_null=False)

    assert gw.adapt_value_for_write(_Unset.NONE, t=t) is None


def test_adapt_value_for_write_keeps_a_sequence_holding_no_enum_member() -> None:
    gw, _ = _build_gateway()
    text = PostgresType(base="text", is_array=False, not_null=True)

    assert gw.adapt_value_for_write(("a", 1), t=text) == str(("a", 1))


class _Mood(StrEnum):
    HAPPY = "happy"


class _Level(IntEnum):
    ONE = 1


class _Shade(Enum):
    DARK = "dark"
    NONE = None


class _Moment(datetime):
    """A ``datetime`` subclass, as pendulum's ``DateTime`` is."""


class _Label(str):
    """A ``str`` subclass that is no enum member."""


_ADAPTED_BASES = [
    "uuid", "text", "varchar", "char", "citext", "bool", "int2", "int4", "int8", "float4",
    "float8", "numeric", "date", "timestamptz", "timestamp", "jsonb", "json", "bytea",
    "varchar(8)", "timestamptz(3)", "numeric(10,2)", "inet", "interval", "time", "mood",
    # With a modifier, as catalog and normalized names spell it: if either side starts reading
    # one as its bare type and the other does not, values come out apart here.
    "timestamp(3)", "timestamp(3) with time zone", "character(3)", "bpchar(3)", "char(3)",
    "time(3)",
]  # fmt: skip

_ADAPTED_VALUES: list[object] = [
    None, "text", "", " padded ", "1", "true", "2026-01-02", _Mood.HAPPY, _Level.ONE,
    _Shade.DARK, _Shade.NONE, 0, 1, -5, 2**63, True, False, 1.5, 0.0, -0.0, 1e20,
    float("nan"), float("inf"), Decimal("1.50"), Decimal("-0"), Decimal("NaN"),
    UUID(int=7), str(UUID(int=7)), date(2026, 1, 2), time(1, 2),
    datetime(2026, 1, 2, 3, 4, 5, 6), datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC),
    datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=5))), timedelta(days=1),
    b"\x00\x01", bytearray(b"x"), [], ["a", "b"], ["a", None], [_Mood.HAPPY], ("a", "b"),
    [1, 2], [1.5, None, float("nan")], [UUID(int=1)], [date(2026, 1, 2)], [[1]],
    [datetime(2026, 1, 2)], {"k": 1}, {"k": _Mood.HAPPY}, {_Mood.HAPPY: [1]},
    _Moment(2026, 1, 2, 3, 4, 5), _Moment(2026, 1, 2, 3, 4, 5, tzinfo=UTC), _Label("label"),
    memoryview(b"mv"), [_Moment(2026, 1, 2)], [_Label("a")], [memoryview(b"m")],
    # For the modifier-typed columns: extra scale, sub-millisecond instants, short strings.
    Decimal("12.345678"), Decimal("1.5E+3"), "ab", "x", time(1, 2, 3, 456789),
    datetime(2026, 1, 2, 3, 4, 5, 678901), datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
    [datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC), None], [Decimal("1.005")], ["ab"],
]  # fmt: skip


def _outcome(adapt: Callable[[], object]) -> object:
    """What *adapt* returns, spelled out (a wrapper's object, each element, the type of
    every value and its repr), or what it raises."""

    def spelled(value: object) -> object:
        if isinstance(value, Json | Jsonb):
            return type(value), spelled(value.obj)

        if isinstance(value, list | tuple):
            return type(value), [spelled(item) for item in value]

        if isinstance(value, dict):
            return type(value), [(spelled(k), spelled(v)) for k, v in value.items()]

        return type(value), repr(value)

    try:
        return spelled(adapt())

    except Exception as error:
        return type(error), str(error)


@pytest.mark.parametrize("is_array", [False, True])
@pytest.mark.parametrize("base", [*_ADAPTED_BASES, None])
def test_a_column_adapter_writes_every_value_as_adapt_value_for_write_does(
    base: str | None, is_array: bool
) -> None:
    """The adapter worked out once per column lets through only what the coercion would
    return as it is; every value, of every column type, comes out (or is refused) alike."""

    gw, _ = _build_gateway()
    t = None if base is None else PostgresType(base=base, is_array=is_array, not_null=False)
    adapter = gw._write_adapter(t)

    for value in _ADAPTED_VALUES:
        assert _outcome(lambda v=value: adapter(v)) == _outcome(
            lambda v=value: gw.adapt_value_for_write(v, t=t)
        ), value


def test_json_write_dumps_stores_decimal_exact_and_rejects_unknown_types() -> None:
    """The jsonb write encoder stores a Decimal as its exact string form; anything
    else orjson cannot encode stays a hard error, never a silent stringification."""

    from decimal import Decimal

    from forze_postgres.kernel.gateways.base import _json_write_dumps

    assert _json_write_dumps({"amount": Decimal("10.50")}) == b'{"amount":"10.50"}'

    with pytest.raises(TypeError, match="not JSON serializable"):
        _json_write_dumps({"bad": object()})

@pytest.mark.asyncio
async def test_patch_group_sql_includes_column_casts() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    pk = UUID("11111111-1111-1111-1111-111111111111")
    column_types = {
        ID_FIELD: PostgresType(base="uuid", is_array=False, not_null=True),
        "rev": PostgresType(base="int4", is_array=False, not_null=True),
        "amount": PostgresType(base="numeric", is_array=False, not_null=False),
    }
    gw.introspector.get_column_types = AsyncMock(return_value=column_types)  # type: ignore[method-assign]

    client.fetch_all = AsyncMock(
        return_value=[
            {
                "id": pk,
                "rev": 2,
                "created_at": ts,
                "last_update_at": ts,
                "name": "x",
                "amount": None,
            }
        ]
    )

    await gw._PostgresWriteGateway__patch_group(  # type: ignore[attr-defined]
        ("amount", "rev"),
        [(pk, 1, {"amount": None, "rev": 2})],
    )

    stmt = client.fetch_all.await_args.args[0]
    sql_text = stmt.as_string()
    assert '"amount" = v."amount"' in sql_text
    assert '"rev" = v."rev"' in sql_text
    assert "CAST(%s AS numeric)" in sql_text
    assert "CAST(%s AS int4)" in sql_text

@pytest.mark.asyncio
async def test_create_many_batches_and_preserves_parameter_order() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    id3 = UUID("33333333-3333-3333-3333-333333333333")

    payloads = [
        MyCreateDoc(name="first"),
        MyCreateDoc(name="second"),
        MyCreateDoc(name="third"),
    ]

    client.fetch_all.side_effect = [
        [_row(pk=id1, name="first", ts=ts), _row(pk=id2, name="second", ts=ts)],
        [_row(pk=id3, name="third", ts=ts)],
    ]

    result = await gw.create_many(payloads, batch_size=2)

    assert client.fetch_all.await_count == 2
    calls = client.fetch_all.await_args_list
    params_by_uuids: list[list[UUID]] = []
    for call in calls:
        params = call.args[1]
        params_by_uuids.append([x for x in params if isinstance(x, UUID)])
        assert call.kwargs["row_factory"] == "dict"
        assert call.kwargs["commit"] is False

    # ids are server-generated now; assert batch grouping by count (2 then 1).
    assert sorted(len(u) for u in params_by_uuids) == [1, 2]
    name_chunks: list[list[str]] = []
    for call in calls:
        params = call.args[1]
        name_chunks.append([x for x in params if x in {"first", "second", "third"}])
    assert sorted(name_chunks, key=lambda ns: ns[0] if ns else "") == [
        ["first", "second"],
        ["third"],
    ]

    assert [x.id for x in result] == [id1, id2, id3]
    assert [x.name for x in result] == ["first", "second", "third"]

@pytest.mark.asyncio
async def test_create_many_raises_when_batch_returns_fewer_rows() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)

    id1 = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    id2 = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    dtos = [
        MyCreateDoc(id=id1, created_at=ts, name="first"),
        MyCreateDoc(id=id2, created_at=ts, name="second"),
    ]

    client.fetch_all.return_value = [_row(pk=id1, name="first", ts=ts)]

    with pytest.raises(CoreException, match="mismatch in number of rows"):
        await gw.create_many(dtos, batch_size=100)

@pytest.mark.asyncio
async def test_ensure_uses_configured_conflict_target_in_sql() -> None:
    gw, client = _build_gateway()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    pk = UUID("11111111-1111-1111-1111-111111111111")
    object.__setattr__(gw, "conflict_target", ("tenant_id", "id"))

    client.fetch_one = AsyncMock(return_value=_row(pk=pk, name="n", ts=ts))

    await gw.ensure(pk, MyCreateDoc(name="n"))

    stmt = client.fetch_one.await_args.args[0]
    assert "ON CONFLICT" in str(stmt)
    introspector = gw.introspector
    introspector.constraint_exists_for_columns.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_many_skips_conflicts_and_loads_existing() -> None:
    gw, client = _build_gateway()
    read = gw.read_gw
    read.get_many = AsyncMock()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    conflict_row = _row(pk=id1, name="unchanged", ts=ts)
    insert_row = _row(pk=id2, name="inserted", ts=ts)
    ids = [id1, id2]
    payloads = [MyCreateDoc(name="try-overwrite"), MyCreateDoc(name="inserted")]
    client.fetch_all = AsyncMock(
        side_effect=[
            [insert_row],
            [conflict_row],
        ],
    )
    out = await gw.ensure_many(ids, payloads, batch_size=20)
    assert [d.id for d in out] == [id1, id2]
    assert out[0].name == "unchanged"
    assert out[1].name == "inserted"
    read.get_many.assert_not_awaited()
    assert client.fetch_all.await_count == 2

@pytest.mark.asyncio
async def test_upsert_inserts_or_updates() -> None:
    gw, client = _build_gateway()
    read = gw.read_gw
    read.get = AsyncMock()
    read.get_many = AsyncMock()
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    id1 = UUID("11111111-1111-1111-1111-111111111111")
    id2 = UUID("22222222-2222-2222-2222-222222222222")
    c1 = MyCreateDoc(name="one")
    c2 = MyCreateDoc(name="two")
    u2 = MyUpdateDoc(name="patched")
    new_row = _row(pk=id1, name="one", ts=ts)
    existing = MyDoc(
        id=id2,
        rev=1,
        created_at=ts,
        last_update_at=ts,
        name="old",
    )
    updated_row = {
        "id": id2,
        "rev": 2,
        "created_at": ts,
        "last_update_at": ts,
        "name": "patched",
    }
    read.get.return_value = existing
    client.fetch_one = AsyncMock(
        side_effect=[
            new_row,
            None,
            {
                "id": id2,
                "rev": 1,
                "created_at": ts,
                "last_update_at": ts,
                "name": "old",
            },
            updated_row,
        ]
    )

    out1 = await gw.upsert(id1, c1, MyUpdateDoc())
    assert out1.id == id1
    assert out1.name == "one"

    out2 = await gw.upsert(id2, c2, u2)
    assert out2.id == id2
    assert out2.name == "patched"
    assert out2.rev == 2
    assert read.get.await_count >= 1
    assert any(call.args[0] == id2 for call in read.get.await_args_list)

def _build_tenant_aware_gateway() -> (
    tuple[PostgresWriteGateway[MyDoc, MyCreateDoc, MyUpdateDoc], MagicMock]
):
    tid = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    client = MagicMock(spec=PostgresClient)
    client.execute = AsyncMock(return_value=1)
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})
    introspector.get_primary_key_columns = AsyncMock(return_value=(ID_FIELD,))
    introspector.constraint_exists_for_columns = AsyncMock(return_value=True)

    read = MagicMock(spec=PostgresReadGateway)
    read.relation = ("public", "docs")
    read.client = client
    read.tenant_aware = True

    domain_codec, create_codec, update_codec = write_codecs_for(
        domain_type=MyDoc,
        create_type=MyCreateDoc,
        update_type=MyUpdateDoc,
    )

    gw = PostgresWriteGateway(
        relation=("public", "docs"),
        client=client,
        model_type=MyDoc,
        codec=domain_codec,
        introspector=introspector,
        read_gw=read,
        create_cmd_type=MyCreateDoc,
        update_cmd_type=MyUpdateDoc,
        create_codec=create_codec,
        update_codec=update_codec,
        strategy="application",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    return gw, client

@pytest.mark.asyncio
async def test_kill_non_tenant_uses_rowcount_and_raises_when_missing() -> None:
    """Non-tenant ``kill`` verifies rowcount the same way the tenant-aware path does."""

    gw, client = _build_gateway()
    client.execute = AsyncMock(return_value=1)
    pk = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    await gw.kill(pk)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(CoreException, match="Record not found") as ei:
        await gw.kill(pk)

    assert ei.value.kind is ExceptionKind.NOT_FOUND

@pytest.mark.asyncio
async def test_kill_many_non_tenant_raises_when_rowcount_mismatch() -> None:
    gw, client = _build_gateway()
    client.execute = AsyncMock(return_value=1)
    pks = [UUID("11111111-1111-1111-1111-111111111111")]

    await gw.kill_many(pks, batch_size=10)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(CoreException, match="Some records not found") as ei:
        await gw.kill_many(pks)

    assert ei.value.kind is ExceptionKind.NOT_FOUND

def test_patch_codec_missing_update_codec_raises_configuration() -> None:
    """A missing update codec is a spec-wiring problem → ``configuration`` kind."""

    gw, _ = _build_gateway()
    object.__setattr__(gw, "update_codec", None)

    with pytest.raises(CoreException, match="Update codec is required") as ei:
        gw._patch_codec()

    assert ei.value.kind is ExceptionKind.CONFIGURATION

@pytest.mark.asyncio
async def test_kill_tenant_aware_uses_rowcount_and_raises_when_missing() -> None:
    gw, client = _build_tenant_aware_gateway()
    pk = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    await gw.kill(pk)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(CoreException, match="Record not found"):
        await gw.kill(pk)

@pytest.mark.asyncio
async def test_kill_many_tenant_aware_raises_when_rowcount_mismatch() -> None:
    gw, client = _build_tenant_aware_gateway()
    pks = [UUID("11111111-1111-1111-1111-111111111111")]

    await gw.kill_many(pks, batch_size=10)

    client.execute.assert_awaited_once()
    assert client.execute.await_args.kwargs.get("return_rowcount") is True

    client.execute = AsyncMock(return_value=0)
    with pytest.raises(CoreException, match="not accessible"):
        await gw.kill_many(pks)

@pytest.mark.asyncio
async def test_update_tenant_aware_includes_tenant_in_where_params() -> None:
    tid = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    pk = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    current = MyDoc(id=pk, rev=1, created_at=ts, last_update_at=ts, name="before")

    gw, client = _build_tenant_aware_gateway()
    read = gw.read_gw
    read.get = AsyncMock(return_value=current)

    client.fetch_one = AsyncMock(
        return_value={
            "id": pk,
            "rev": 2,
            "created_at": ts,
            "last_update_at": ts,
            "name": "after",
        }
    )

    await gw.update(pk, MyUpdateDoc(name="after"))

    params = client.fetch_one.await_args.args[1]
    assert tid in params


@pytest.mark.asyncio
async def test_create_and_update_use_dedicated_codecs() -> None:
    """Create/update paths must use create_codec and update_codec, not domain_codec alone."""
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    pk = UUID("11111111-1111-1111-1111-111111111111")
    dto = MyCreateDoc(id=pk, created_at=ts, name="new")
    model = MyDoc(id=pk, rev=1, created_at=ts, last_update_at=ts, name="new")
    current = MyDoc(id=pk, rev=1, created_at=ts, last_update_at=ts, name="before")

    create_codec = MagicMock()
    create_codec.transform.return_value = model
    create_codec.transform_many.return_value = [model]

    update_codec = MagicMock()
    # A plain codec has no ``encode_persistence_patch`` (the id-binding patch path is
    # encrypting-codec only); the bare mock would otherwise fabricate it and divert the
    # write off ``encode_persistence_mapping``.
    update_codec.encode_persistence_patch = None
    update_codec.encode_plain_patch = None
    update_codec.encrypt_mapping = None
    update_codec.encode_persistence_mapping.return_value = {"name": "after"}

    read_codec = MagicMock()
    # A plain (non-encrypting) codec has no ``prepare_encrypt``; the bare mock would
    # otherwise fabricate a truthy attribute the warm pre-pass tries to await.
    read_codec.prepare_encrypt = None
    read_codec.encode_persistence_mapping.return_value = {
        "id": pk,
        "rev": 1,
        "created_at": ts,
        "last_update_at": ts,
        "name": "new",
    }

    client = MagicMock(spec=PostgresClient)
    client.fetch_one = AsyncMock(return_value=_row(pk=pk, name="new", ts=ts))
    client.gather_concurrency_semaphore = MagicMock(
        return_value=asyncio.Semaphore(8),
    )

    introspector = MagicMock(spec=PostgresIntrospector)
    introspector.get_column_types = AsyncMock(return_value={})
    introspector.get_primary_key_columns = AsyncMock(return_value=(ID_FIELD,))
    introspector.constraint_exists_for_columns = AsyncMock(return_value=True)

    read = MagicMock(spec=PostgresReadGateway)
    read.relation = ("public", "docs")
    read.client = client
    read.tenant_aware = False
    read.get = AsyncMock(return_value=current)

    gw = PostgresWriteGateway(
        relation=("public", "docs"),
        client=client,
        model_type=MyDoc,
        codec=read_codec,
        introspector=introspector,
        read_gw=read,
        create_cmd_type=MyCreateDoc,
        update_cmd_type=MyUpdateDoc,
        create_codec=create_codec,
        update_codec=update_codec,
        strategy="application",
    )

    await gw.create(dto)

    create_codec.transform.assert_called_once_with(dto)
    read_codec.encode_persistence_mapping.assert_called_once_with(model)

    client.fetch_one = AsyncMock(
        return_value={
            "id": pk,
            "rev": 2,
            "created_at": ts,
            "last_update_at": ts,
            "name": "after",
        }
    )

    await gw.update(pk, MyUpdateDoc(name="after"))

    update_codec.encode_persistence_mapping.assert_called_once_with(
        MyUpdateDoc(name="after"),
        exclude={"unset": True},
    )


# ....................... #


def test_serializing_writes_without_a_scope_is_refused() -> None:
    # Every lock key carries the spec's name; without one, two aggregates keyed on the same
    # value would contend for no reason their declarations state.
    gw, _ = _build_gateway()

    with pytest.raises(CoreException) as caught:
        attrs.evolve(gw, serialized_by=(SerializedBy(key=("name",)),))

    assert caught.value.kind is ExceptionKind.CONFIGURATION

    attrs.evolve(gw, serialized_by=(SerializedBy(key=("name",)),), serialization_scope="docs")


# ....................... #
# Set-based upsert: which columns compare and how rows reach the statement


def test_a_column_compares_unless_its_equality_differs_from_the_domains() -> None:
    from forze_postgres.kernel.catalog.introspect import PostgresType
    from forze_postgres.kernel.gateways.write import _comparable

    def t(base: str, *, is_array: bool = False) -> PostgresType:
        return PostgresType(base=base, is_array=is_array, not_null=False)

    assert _comparable(None)
    assert _comparable(t("text")) and _comparable(t("numeric(10,2)")) and _comparable(t("json"))
    assert not _comparable(t("citext")) and not _comparable(t("character(3)"))
    assert not _comparable(t("xml")) and not _comparable(t("json", is_array=True))


def test_a_jsonb_array_compares_as_it_is_and_a_json_array_falls_back() -> None:
    from forze_postgres.kernel.catalog.introspect import PostgresType
    from forze_postgres.kernel.gateways.write import _comparable, _compared

    jsonb_array = PostgresType(base="jsonb", is_array=True, not_null=False)
    json_array = PostgresType(base="json", is_array=True, not_null=False)
    json = PostgresType(base="json", is_array=False, not_null=False)

    # jsonb has equality, so a jsonb[] stays on the set-based path, compared uncast.
    assert _comparable(jsonb_array)
    assert _compared("t", "tags", jsonb_array).as_string() == '"t"."tags"'
    # json has none: a json[] column takes the domain path; a json one compares as jsonb.
    assert not _comparable(json_array)
    assert _compared("t", "doc", json).as_string() == '"t"."doc"::jsonb'


# ....................... #
# An update's values, as a column that rounds or pads them stores them


def _pg(base: str, *, is_array: bool = False) -> PostgresType:
    return PostgresType(base=base, is_array=is_array, not_null=False)


def test_only_a_column_that_rounds_or_pads_converts() -> None:
    from forze_postgres.kernel.gateways.write import _stored_as

    converts = [
        "timestamp(3) with time zone",
        "timestamp(0) without time zone",
        "time(2) with time zone",
        "numeric(10,2)",
        "numeric(5,-1)",
        "numeric(7,0)",
        "float4",
        "character(3)",
    ]
    stores_as_sent = [
        "timestamp(6) with time zone",
        "timestamptz",
        "numeric",
        "float8",
        "bpchar",
        "varchar",
        "interval(3)",
        "bit(8)",
    ]

    assert all(_stored_as(_pg(base)) is not None for base in converts)
    assert all(_stored_as(_pg(base, is_array=True)) is not None for base in converts)
    assert all(_stored_as(_pg(base)) is None for base in stores_as_sent)


@pytest.mark.parametrize(
    ("sent", "digits", "stored"),
    [
        # Half a step rounds up after Postgres's epoch, 2000-01-01, and down before it.
        (datetime(2026, 1, 1, 12, 0, 0, 123500), 3, datetime(2026, 1, 1, 12, 0, 0, 124000)),
        (datetime(2026, 1, 1, 12, 0, 0, 123499), 3, datetime(2026, 1, 1, 12, 0, 0, 123000)),
        (datetime(1999, 12, 31, 23, 59, 59, 123500), 3, datetime(1999, 12, 31, 23, 59, 59, 123000)),
        (datetime(1999, 12, 31, 23, 59, 59, 123501), 3, datetime(1999, 12, 31, 23, 59, 59, 124000)),
        (datetime(2026, 1, 1, 12, 0, 0, 999500), 3, datetime(2026, 1, 1, 12, 0, 1)),
        (datetime(2026, 1, 1, 12, 0, 0, 123450), 4, datetime(2026, 1, 1, 12, 0, 0, 123500)),
        (datetime(2026, 1, 1, 12, 0, 0, 500000), 0, datetime(2026, 1, 1, 12, 0, 1)),
        # An aware instant counts from the epoch in UTC, and keeps its offset.
        (
            datetime(2000, 1, 1, 2, 0, 0, 500, tzinfo=timezone(timedelta(hours=3))),
            3,
            datetime(2000, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=3))),
        ),
        (
            datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC),
            3,
            datetime(2026, 1, 1, 12, 0, 0, 123000, tzinfo=UTC),
        ),
        # A time of day rounds half up; one that would reach 24:00:00 is left as sent.
        (time(12, 0, 0, 123500), 3, time(12, 0, 0, 124000)),
        (time(12, 0, 0, 123500, tzinfo=UTC), 3, time(12, 0, 0, 124000, tzinfo=UTC)),
        (time(23, 59, 59, 999500), 3, time(23, 59, 59, 999500)),
        # Rounding past 9999-12-31, which Postgres keeps and Python cannot hold: as sent.
        (datetime.max, 3, datetime.max),
        (datetime.max.replace(tzinfo=UTC), 3, datetime.max.replace(tzinfo=UTC)),
        (
            datetime(9999, 12, 31, 23, 59, 59, 999600, tzinfo=timezone(timedelta(hours=3))),
            3,
            datetime(9999, 12, 31, 23, 59, 59, 999600, tzinfo=timezone(timedelta(hours=3))),
        ),
        ("2026-01-01T12:00:00.123456", 3, "2026-01-01T12:00:00.123456"),
    ],
)
def test_an_instant_rounds_as_postgres_stores_it(sent: object, digits: int, stored: object) -> None:
    from forze_postgres.kernel.gateways.write import _rounded_as_stored

    got = _rounded_as_stored(sent, digits)

    assert got == stored
    assert getattr(got, "tzinfo", None) == getattr(stored, "tzinfo", None)


@pytest.mark.parametrize(
    ("sent", "precision", "scale", "stored"),
    [
        # Half away from zero, to a negative scale too.
        (Decimal("1.005"), 10, 2, Decimal("1.01")),
        (Decimal("-1.005"), 10, 2, Decimal("-1.01")),
        (Decimal("0.0049"), 10, 2, Decimal("0.00")),
        (Decimal("123.456"), 5, -1, Decimal("120")),
        (Decimal("1E-999999999"), 10, 2, Decimal("0.00")),
        # Too large for the precision once rounded: the write refuses it.
        (Decimal("999.995"), 5, 2, Decimal("1000.00")),
        # Too large as sent, however vast: as sent, never quantized.
        (Decimal("1000"), 5, 2, Decimal("1000")),
        (Decimal("1E+999999999"), 10, 2, Decimal("1E+999999999")),
        (Decimal("-Infinity"), 10, 2, Decimal("-Infinity")),
        # A float as the write sends it, its str(); one whose rounded value no float spells
        # exactly, as sent.
        (1.005, 10, 2, 1.01),
        (0.12499999999999999, 10, 2, 0.12),
        (12345678901234.567, 20, 2, 12345678901234.57),
        (0.1234567890123456, 20, 17, 0.1234567890123456),
        (float("nan"), 10, 2, None),
        ("1.005", 10, 2, "1.005"),
    ],
)
def test_a_number_rounds_to_its_scale(
    sent: object, precision: int, scale: int, stored: object
) -> None:
    from forze_postgres.kernel.gateways.write import _as_numeric

    got = _as_numeric(sent, precision, scale)

    assert got is sent if stored is None else got == stored


def test_a_float_rounds_only_to_a_value_the_write_stores_exactly() -> None:
    from forze_postgres.kernel.gateways.write import _as_numeric

    rng = random.Random(792)

    for _ in range(5000):
        sent = rng.uniform(1, 10) * 10.0 ** rng.randint(-6, 15)
        scale = rng.randint(0, 17)
        # What the write stores from either float: its str(), rounded half away from zero.
        step = Decimal(1).scaleb(-scale)
        stored = Decimal(str(sent)).quantize(step, rounding=ROUND_HALF_UP, context=Context(prec=60))
        got = _as_numeric(sent, 40, scale)

        assert got is sent or Decimal(str(got)) == stored, (sent, scale)


def test_a_nan_number_is_left_as_sent() -> None:
    from forze_postgres.kernel.gateways.write import _as_numeric

    nan = Decimal("NaN")

    assert _as_numeric(nan, 10, 2) is nan


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        (0.1, 0.1),
        (0.123456789, 0.12345679),
        (1 / 3, 0.33333334),
        (16777217.0, 16777216.0),
        (2.0**-149, 1e-45),
        # A power of two: the shortest spelling lies on the wider side of its interval.
        (2.0**-96, 1.2621775e-29),
        # Nine digits, a tie between two eight-digit spellings: the even one.
        (1.00390625, 1.0039062),
        (-0.0, -0.0),
        (float("inf"), float("inf")),
        # Out of range, which Postgres refuses: as sent.
        (3.5e38, 3.5e38),
        (1e-50, 1e-50),
        (1, 1),
    ],
)
def test_a_real_reads_back_as_its_nearest_single(sent: object, stored: object) -> None:
    from forze_postgres.kernel.gateways.write import _as_real

    assert _as_real(sent) == stored


@pytest.mark.parametrize(
    ("sent", "stored"),
    [
        ("ab", "ab "),
        ("ab ", "ab "),
        ("ab   ", "ab "),
        ("", "   "),
        ("a\t", "a\t "),
        # Too long: the write refuses it.
        ("abcd", "abcd"),
    ],
)
def test_a_char_is_blank_padded(sent: str, stored: str) -> None:
    from forze_postgres.kernel.gateways.write import _padded

    assert _padded(sent, 3) == stored


def test_an_update_converts_only_the_columns_that_round_or_pad() -> None:
    from forze_postgres.kernel.gateways.write import _as_stored

    at = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    rounded = at.replace(microsecond=123000)
    types = {
        "at": _pg("timestamp(3) with time zone"),
        "ats": _pg("timestamp(3) with time zone", is_array=True),
        "seen": _pg("timestamptz"),
        "price": _pg("numeric(10,2)"),
        "codes": _pg("character(3)", is_array=True),
    }
    patch = {
        "at": at,
        "ats": [at, None],
        "seen": at,
        "price": Decimal("1.005"),
        "codes": ["ab", None],
        "name": "n",
    }

    assert _as_stored(patch, types) == {
        "at": rounded,
        "ats": [rounded, None],
        "seen": at,
        "price": Decimal("1.01"),
        "codes": ["ab ", None],
        "name": "n",
    }
    assert patch["at"] == at
    assert _as_stored({"name": "n"}, types) == {"name": "n"}


@pytest.mark.asyncio
async def test_bulk_revisions_must_match_the_keys() -> None:
    gw, _ = _build_gateway()

    with pytest.raises(CoreException) as refused:
        await gw.touch_many([UUID(int=1)], revs=[1, 2])

    assert refused.value.kind is ExceptionKind.INTERNAL


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["same", "new"])
async def test_only_a_strict_check_of_an_unchanged_row_locks_its_revision(name: str) -> None:
    # The lock stands in for the UPDATE a changed row runs, and only where its rev is checked.
    ts = datetime(2025, 1, 1, tzinfo=UTC)
    pk = UUID(int=1)
    gw, client = _build_gateway()
    gw.read_gw.get_many = AsyncMock(
        return_value=[MyDoc(id=pk, rev=1, created_at=ts, last_update_at=ts, name="same")]
    )
    # What each statement returns: the row at its revision, or the row the UPDATE wrote.
    held = {"id": pk, "rev": 1} if name == "same" else {**_row(pk=pk, name="new", ts=ts), "rev": 2}
    client.fetch_all = AsyncMock(return_value=[held])

    _, merged = await gw.update_many([pk], [MyUpdateDoc(name="same")])
    _, strict = await gw.update_many([pk], [MyUpdateDoc(name=name)], revs=[1], merge=False)

    assert merged == [{}]
    assert bool(strict[0]) is (name == "new")
    sql = client.fetch_all.await_args.args[0].as_string()
    assert ("FOR SHARE" in sql) is (name == "same")
    client.fetch_all.assert_awaited_once()
