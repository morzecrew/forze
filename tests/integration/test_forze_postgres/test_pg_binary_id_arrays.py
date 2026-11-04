"""Every document statement that takes an array of ids binds it in binary, cast to ``uuid[]``,
and answers as it did: for ids passed as ``UUID``s and as their text, uppercase and unhyphenated
(which the commands read as the ``UUID`` it names)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import KeyedUpdate
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres._document_fixtures import document_context
from tests.integration.test_forze_postgres.test_pg_set_based_upsert import (
    _SPEC,
    _item,
    _table,
    _Update,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_Spelling = Callable[[UUID], Any]
_SPELLINGS = [
    pytest.param(lambda pk: pk, id="uuid"),
    pytest.param(lambda pk: pk.hex.upper(), id="text"),
]


async def _run(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[Any, Any, list[Any]], Awaitable[Any]],
    spell: _Spelling,
) -> tuple[Any, list[dict[str, Any]], list[str]]:
    """Run *operation* over two stored rows, their ids spelled by *spell*; what it returned, the
    rows left, and the statements it ran."""

    t = await _table(pg_client)
    ctx = document_context(pg_client, t)
    cmd, query = ctx.document.command(_SPEC), ctx.document.query(_SPEC)
    ids = sorted((uuid4(), uuid4()), key=lambda pk: pk.int)
    await cmd.upsert_many([_item(pk, f"n{i}", i) for i, pk in enumerate(ids)], return_new=False)

    statements: list[str] = []

    for name in ("fetch_all", "fetch_one", "execute"):
        method = getattr(pg_client, name)

        async def spy(query_: Any, *args: Any, _method: Any = method, **kwargs: Any) -> Any:
            statements.append(query_ if isinstance(query_, str) else query_.as_string(None))
            return await _method(query_, *args, **kwargs)

        monkeypatch.setattr(pg_client, name, spy)

    try:
        result = await operation(cmd, query, [spell(pk) for pk in ids])

    finally:
        monkeypatch.undo()

    rows = await pg_client.fetch_all(f"SELECT name, qty, note, rev FROM {t} ORDER BY id", [])

    return result, rows, statements


async def _get_many(cmd: Any, query: Any, ids: list[Any]) -> Any:
    return [(doc.name, doc.rev) for doc in await query.get_many(ids)]


async def _touch_many(cmd: Any, query: Any, ids: list[Any]) -> Any:
    return [doc.rev for doc in await cmd.touch_many(ids)]


async def _kill_many(cmd: Any, query: Any, ids: list[Any]) -> Any:
    return await cmd.kill_many(ids)


async def _update_many_unchanged(cmd: Any, query: Any, ids: list[Any]) -> Any:
    # A strict update that changes nothing holds the rows' revisions instead of writing.
    updates = [KeyedUpdate(id=pk, rev=1, dto=_Update(qty=i)) for i, pk in enumerate(ids)]

    return [doc.rev for doc in await cmd.update_many(updates, merge=False)]


async def _update_matching(cmd: Any, query: Any, ids: list[Any]) -> Any:
    return await cmd.update_matching(
        {"$values": {"qty": {"$gte": 0}}}, _Update(note="matched"), return_new=False
    )


@pytest.mark.parametrize("spell", _SPELLINGS)
@pytest.mark.parametrize(
    ("operation", "statement"),
    [
        pytest.param(_get_many, "SELECT", id="get_many"),
        pytest.param(_touch_many, "SELECT", id="touch_many"),
        pytest.param(_kill_many, "DELETE", id="kill_many"),
        pytest.param(_update_many_unchanged, "FOR SHARE", id="update_many_held"),
        pytest.param(_update_matching, "UPDATE", id="update_matching"),
    ],
)
async def test_an_id_array_goes_in_binary_and_answers_as_before(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[[Any, Any, list[Any]], Awaitable[Any]],
    statement: str,
    spell: _Spelling,
) -> None:
    result, rows, statements = await _run(pg_client, monkeypatch, operation, spell)
    expected_result, expected_rows, _ = await _run(pg_client, monkeypatch, operation, lambda pk: pk)

    assert (result, rows) == (expected_result, expected_rows)
    assert any(statement in s and "= ANY(%b::uuid[])" in s for s in statements), statements
