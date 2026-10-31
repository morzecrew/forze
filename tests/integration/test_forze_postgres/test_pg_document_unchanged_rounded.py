"""Postgres rounds a ``numeric(p,s)`` value to its scale, a ``real`` one to single precision,
and blank-pads a ``char(n)`` one, arrays element by element. An update compares what the
column will keep, so resending an unchanged value is no change; and what it keeps is exactly
what Postgres stores and reads back."""

from __future__ import annotations

import math
import random
import struct
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
    UpsertItem,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.gateways.write import _as_stored
from tests.integration.test_forze_postgres._document_fixtures import document_context

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_rng = random.Random(792)


def _single(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def _midpoints() -> list[float]:
    """Doubles at and beside the midpoint of two adjacent singles, below and above an even and
    an odd one, a power of two, a subnormal and the largest single: Postgres rounds the float8
    the write sends to the even one."""

    out: list[float] = []

    for bits in (0x3F800000, 0x3F800001, 0x3DCCCCCD, 0x00800000, 0x00000003, 0x4B800000):
        for low, high in ((_single(bits - 1), _single(bits)), (_single(bits), _single(bits + 1))):
            mid = (low + high) / 2
            out += [mid, math.nextafter(mid, -math.inf), math.nextafter(mid, math.inf)]

    top = (_single(0x7F7FFFFE) + _single(0x7F7FFFFF)) / 2

    return [*out, top, math.nextafter(top, math.inf)]


_SENT: dict[str, list[Any]] = {
    "price": [
        Decimal(v)
        for v in ("1.005", "-1.005", "0.0049", "-0.005", "123.456", "1E+2", "12345678.994999")
    ],
    "coarse": [Decimal(v) for v in ("123.456", "125", "-125", "99994.9", "0.4")],
    "weight": [0.1, 1 / 3, 0.123456789, 16777217.0, -0.0, float("inf")]
    + [2.0**e for e in range(-149, 128)]
    + [_rng.uniform(1, 10) * 10.0**e for e in range(-37, 38) for _ in range(20)]
    + _midpoints(),
    "code": ["ab", "ab ", "ab   ", "", "a\t", "é", "abc"],
}
"""Values around half a step of each column, and a few thousand ``real`` ones."""

_FLOATS: list[float] = [
    1.005,
    2.675,
    0.125,
    0.12499999999999999,
    0.1 + 0.2,
    12345678901234.567,
    123456789012345.67,
    1234567890123456.7,
    99999999999999.995,
    9007199254740.993,
    1e15 + 0.3,
] + [_rng.uniform(1, 10) * 10.0 ** _rng.randint(-3, 16) for _ in range(300)]
"""Floats for a ``numeric(20,2)`` column, sixteen and seventeen digits among them."""


async def _table(pg_client: PostgresClient) -> str:
    t = f"pg_rounded_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            price numeric(10,2),
            coarse numeric(5,-1),
            weight real,
            code char(3),
            prices numeric(10,2)[],
            coarses numeric(5,-1)[],
            weights real[],
            codes char(3)[],
            wide numeric(20,2),
            wides numeric(20,2)[]
        );
        """
    )
    return t


async def test_a_value_rounds_or_pads_as_postgres_stores_it(pg_client: PostgresClient) -> None:
    t = await _table(pg_client)
    types = await PostgresIntrospector(client=pg_client).get_column_types(
        schema="public", relation=t
    )

    for column, sent in _SENT.items():
        for value in sent if column != "weight" else sent[:6]:
            stored = await pg_client.fetch_value(
                f"INSERT INTO {t} (id, {column}) VALUES (%s, %s) RETURNING {column}",
                [uuid4(), value],
            )

            assert _as_stored({column: value}, types)[column] == stored, (column, value)

        # An array, element by element; every ``real`` value goes this way.
        stored = await pg_client.fetch_value(
            f"INSERT INTO {t} (id, {column}s) VALUES (%s, %s) RETURNING {column}s",
            [uuid4(), sent],
        )

        assert _as_stored({f"{column}s": sent}, types)[f"{column}s"] == stored, column

    # A float into ``numeric``: the write sends its str(). It rounds to what is stored, spelled
    # exactly by the float a read returns, or stays as sent.
    for value in _FLOATS:
        stored = await pg_client.fetch_value(
            f"INSERT INTO {t} (id, wide) VALUES (%s, %s) RETURNING wide",
            [uuid4(), Decimal(str(value))],
        )
        got = _as_stored({"wide": value}, types)["wide"]

        assert got is value or (Decimal(str(got)), got) == (stored, float(stored)), value

    stored = await pg_client.fetch_value(
        f"INSERT INTO {t} (id, wides) VALUES (%s, %s) RETURNING wides",
        [uuid4(), [Decimal(str(value)) for value in _FLOATS]],
    )

    assert _as_stored({"wides": _FLOATS}, types)["wides"] == [float(x) for x in stored]


class _Values(BaseModel):
    price: Decimal
    ratio: float
    weight: float
    code: str
    prices: list[Decimal]
    weights: list[float]
    codes: list[str]


class ValuesCreate(CreateDocumentCmd, _Values):
    pass


class ValuesDoc(Document, _Values):
    pass


class ValuesRead(ReadDocument, _Values):
    pass


class ValuesUpdate(BaseDTO):
    price: Decimal | None = None
    ratio: float | None = None
    weight: float | None = None
    code: str | None = None
    prices: list[Decimal] | None = None
    weights: list[float] | None = None
    codes: list[str] | None = None


SPEC = DocumentSpec(
    name="values",
    read=ValuesRead,
    write=DocumentWriteTypes(domain=ValuesDoc, create_cmd=ValuesCreate, update_cmd=ValuesUpdate),
)

PAYLOAD: dict[str, Any] = {
    "price": Decimal("1.005"),
    "ratio": 2.675,
    "weight": 0.123456789,
    "code": "ab",
    "prices": [Decimal("-1.005"), Decimal("7")],
    "weights": [1 / 3, 0.1],
    "codes": ["a", "abc "],
}
"""A value each column rounds or pads."""


async def test_an_unchanged_value_is_not_a_change(pg_client: PostgresClient) -> None:
    t = f"pg_rounded_doc_{uuid4().hex[:10]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            price numeric(10,2) NOT NULL,
            ratio numeric(10,2) NOT NULL,
            weight real NOT NULL,
            code char(4) NOT NULL,
            prices numeric(10,2)[] NOT NULL,
            weights real[] NOT NULL,
            codes char(4)[] NOT NULL
        );
        """
    )
    ctx = document_context(pg_client, t)
    command, query = ctx.document.command(SPEC), ctx.document.query(SPEC)
    create, update = ValuesCreate(**PAYLOAD), ValuesUpdate(**PAYLOAD)

    doc = await command.create(create)
    item = UpsertItem(id=doc.id, create=create, update=update)

    for _ in range(2):
        await command.upsert(doc.id, create, update, return_new=False)
        await command.upsert_many([item], return_new=False)
        await command.upsert_many([item], return_new=False, set_based=True)
        await command.update_many([KeyedUpdate(id=doc.id, rev=doc.rev, dto=update)])

    _, diff = await command.update(doc.id, doc.rev, update, return_diff=True)
    stored = await query.get(doc.id, skip_cache=True)

    assert diff == {}
    assert (stored.rev, stored.last_update_at) == (doc.rev, doc.last_update_at)
    assert stored == doc

    # A real change still is one, and what it returns is what a read returns.
    changed = await command.update(doc.id, doc.rev, ValuesUpdate(price=Decimal("1.015")))

    assert (changed.rev, changed.price) == (doc.rev + 1, Decimal("1.02"))
    assert changed == await query.get(doc.id, skip_cache=True)
