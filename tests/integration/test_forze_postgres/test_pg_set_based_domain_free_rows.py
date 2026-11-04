"""A set-based insert dumps its rows straight from the create payloads where the domain adds
nothing to them, and must store byte for byte what the domain path stores.

Each shape writes the same payloads twice under one frozen clock: through the domain path
(``set_based=False``) and set-based. Every column is compared as Postgres prints it, and the
set-based write is checked to have taken, or skipped, the domain-free rows as decided.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, Field

import forze_postgres.kernel.gateways.write as write_module
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
    UpsertItem,
)
from forze.application.execution import Deps
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze.domain.models import BaseDTO, CoreModel, Document, ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_AT = datetime(2026, 1, 1, 12, tzinfo=UTC)


class _Mood(StrEnum):
    CALM = "calm"
    LOUD = "loud"


class _Inner(BaseModel):
    x: int
    y: str | None = None


class _Typed(CoreModel):
    price: Decimal
    at: datetime
    day: date
    ref: UUID
    mood: _Mood
    blob: bytes
    span: timedelta
    note: str | None = None


class _Shaped(CoreModel):
    tags: list[str] = Field(default_factory=list)
    meta: dict[str, int] = Field(default_factory=dict)
    child: _Inner
    children: list[_Inner] = Field(default_factory=list)
    label: str = "dflt"


class _Extended(CoreModel):
    name: str
    qty: int = 0


class _Update(BaseDTO):
    pass


def _spec(fields: type[CoreModel], domain_body: dict[str, Any] | None = None) -> DocumentSpec:
    module = {"__module__": __name__}
    domain = type(f"{fields.__name__}Doc", (Document, fields), {**module, **(domain_body or {})})
    create = type(f"{fields.__name__}Create", (BaseDTO, fields), module)
    read = type(f"{fields.__name__}Read", (ReadDocument, fields), module)

    return DocumentSpec(
        name=f"domain_free_{fields.__name__}",
        read=read,
        write=DocumentWriteTypes(domain=domain, create_cmd=create, update_cmd=_Update),
    )


_LMT = timezone(timedelta(hours=2, minutes=30, seconds=17))

_SHAPES: dict[str, tuple[DocumentSpec, str, list[dict[str, Any]], bool]] = {
    "typed": (
        _spec(_Typed),
        (
            "price numeric NOT NULL, at timestamptz NOT NULL, day date NOT NULL,"
            " ref uuid NOT NULL, mood text NOT NULL, blob bytea NOT NULL,"
            " span interval NOT NULL, note text"
        ),
        [
            {
                "price": Decimal("12.3400"),
                "at": datetime(1880, 6, 1, 12, 0, 0, 5, tzinfo=_LMT),
                "day": date(1, 1, 1),
                "ref": uuid4(),
                "mood": "loud",
                "blob": bytes(range(256)),
                "span": timedelta(days=-1, microseconds=7),
            },
            {
                "price": Decimal("-0.5"),
                "at": datetime(2026, 3, 1, tzinfo=UTC),
                "day": date(9999, 12, 31),
                "ref": uuid4(),
                "mood": _Mood.CALM,
                "blob": b"",
                "span": timedelta(0),
                "note": "  trimmed  ",
            },
        ],
        True,
    ),
    "shaped": (
        _spec(_Shaped),
        (
            "tags jsonb NOT NULL, meta jsonb NOT NULL, child jsonb NOT NULL,"
            " children jsonb NOT NULL, label text NOT NULL"
        ),
        [
            {"child": {"x": 1}},
            {
                "tags": ["a", "é"],
                "meta": {"k": 1},
                "child": {"x": 2, "y": "y"},
                "children": [{"x": 3}],
            },
        ],
        True,
    ),
    # A field of the domain's own, with a default: not proven alike, so the domain builds it.
    "domain-only field": (
        _spec(_Extended, {"__annotations__": {"extra": int}, "extra": 7}),
        "name text NOT NULL, qty integer NOT NULL, extra integer NOT NULL",
        [{"name": "a"}, {"name": "b", "qty": 2}],
        False,
    ),
}


async def _store(
    pg_client: PostgresClient,
    spec: DocumentSpec,
    columns: str,
    payloads: list[dict[str, Any]],
    ids: list[UUID],
    *,
    strategy: str,
    set_based: bool,
) -> list[dict[str, Any]]:
    t = f"pg_domain_free_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"CREATE TABLE {t} (id uuid PRIMARY KEY, rev integer NOT NULL, created_at timestamptz "
        f"NOT NULL, last_update_at timestamptz NOT NULL, {columns})"
    )
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", t), write=("public", t), bookkeeping_strategy=strategy
        )
    )
    deps = {
        PostgresClientDepKey: pg_client,
        PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
        DocumentQueryDepKey: doc,
        DocumentCommandDepKey: doc,
    }
    cmd = context_from_deps(Deps.plain(deps)).document.command(spec)
    create = spec.write["create_cmd"]  # type: ignore[index]

    with bind_time_source(FrozenTimeSource(instant=_AT)):
        await cmd.upsert_many(
            [
                UpsertItem(id=pk, create=create(**payload), update=_Update())
                for pk, payload in zip(ids, payloads, strict=True)
            ],
            return_new=False,
            set_based=set_based,
        )

    names = await pg_client.fetch_all(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s "
        "ORDER BY ordinal_position",
        [t],
    )
    printed = ", ".join(f'"{r["column_name"]}"::text AS "{r["column_name"]}"' for r in names)

    return await pg_client.fetch_all(f"SELECT {printed} FROM {t} ORDER BY id", [])


@pytest.mark.parametrize("strategy", ["application", "database"])
@pytest.mark.parametrize("shape", list(_SHAPES))
async def test_a_set_based_insert_stores_what_the_domain_path_stores(
    pg_client: PostgresClient, monkeypatch: pytest.MonkeyPatch, shape: str, strategy: str
) -> None:
    spec, columns, payloads, fast = _SHAPES[shape]
    ids = [uuid4() for _ in payloads]
    built: list[bool] = []
    rows_from = write_module.rows_from_create_payloads

    def spy(*args: Any, **kwargs: Any) -> Any:
        rows = rows_from(*args, **kwargs)
        built.append(rows is not None)

        return rows

    monkeypatch.setattr(write_module, "rows_from_create_payloads", spy)

    by_domain = await _store(
        pg_client, spec, columns, payloads, ids, strategy=strategy, set_based=False
    )
    set_based = await _store(
        pg_client, spec, columns, payloads, ids, strategy=strategy, set_based=True
    )

    assert set_based == by_domain
    assert built == [fast]
