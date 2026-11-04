"""The id-taking batch commands read a ``str`` id as the ``UUID`` it names: two spellings of one
id are one id, and the cache entry a non-canonical spelling names is the one dropped."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import KeyedCreate, KeyedUpdate
from forze.base.exceptions import CoreException
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_postgres.test_pg_set_based_upsert import (
    _cached,
    _item,
    _table,
    _Update,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_Command = Callable[[Any, list[Any]], Awaitable[Any]]


def _ensure_many(cmd: Any, ids: list[Any]) -> Awaitable[Any]:
    return cmd.ensure_many(
        [KeyedCreate(id=i, payload=_item(uuid4(), "e", 1).create) for i in ids], return_new=False
    )


def _update_many(cmd: Any, ids: list[Any]) -> Awaitable[Any]:
    return cmd.update_many(
        [KeyedUpdate(id=i, rev=1, dto=_Update(name="patched")) for i in ids], return_new=False
    )


def _touch_many(cmd: Any, ids: list[Any]) -> Awaitable[Any]:
    return cmd.touch_many(ids, return_new=False)


def _kill_many(cmd: Any, ids: list[Any]) -> Awaitable[Any]:
    return cmd.kill_many(ids)


_COMMANDS = [
    pytest.param(_ensure_many, id="ensure_many"),
    pytest.param(_update_many, id="update_many"),
    pytest.param(_touch_many, id="touch_many"),
    pytest.param(_kill_many, id="kill_many"),
]


async def _stored(pg_client: PostgresClient) -> tuple[Any, Any, UUID, str]:
    """A cached command and query over a table holding one row, read once so it is cached."""

    t = await _table(pg_client)
    cmd, query = _cached(pg_client, t)
    pk = uuid4()
    await cmd.upsert_many([_item(pk, "stored", 1)], return_new=False)
    assert (await query.get(pk)).name == "stored"

    return cmd, query, pk, t


@pytest.mark.parametrize("command", _COMMANDS)
async def test_two_spellings_of_one_id_are_refused(
    pg_client: PostgresClient, command: _Command
) -> None:
    cmd, query, pk, _ = await _stored(pg_client)

    with pytest.raises(CoreException) as refused:
        await command(cmd, [pk, pk.hex.upper()])

    assert refused.value.kind == "precondition"
    stored = await query.get(pk, skip_cache=True)
    assert (stored.name, stored.rev) == ("stored", 1)


@pytest.mark.parametrize("command", _COMMANDS)
async def test_a_malformed_str_id_is_refused(pg_client: PostgresClient, command: _Command) -> None:
    cmd, _, _, _ = await _stored(pg_client)

    with pytest.raises(CoreException) as refused:
        await command(cmd, ["not-a-uuid"])

    assert refused.value.kind == "precondition"
    assert "Invalid UUID value" in str(refused.value)


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        pytest.param(_update_many, ("patched", 2), id="update_many"),
        pytest.param(_touch_many, ("stored", 2), id="touch_many"),
    ],
)
async def test_a_non_canonical_str_id_drops_the_cached_document(
    pg_client: PostgresClient, command: _Command, expected: tuple[str, int]
) -> None:
    cmd, query, pk, _ = await _stored(pg_client)

    await command(cmd, [pk.hex.upper()])

    read = await query.get(pk)
    assert (read.name, read.rev) == expected


async def test_a_non_canonical_str_id_kills_the_cached_document(pg_client: PostgresClient) -> None:
    cmd, query, pk, t = await _stored(pg_client)

    await cmd.kill_many([pk.hex.upper()])

    assert await pg_client.fetch_all(f"SELECT id FROM {t}", []) == []

    with pytest.raises(CoreException) as gone:
        await query.get(pk)

    assert gone.value.kind == "not_found"


async def test_a_non_canonical_str_id_ensures_the_row_under_its_uuid(
    pg_client: PostgresClient,
) -> None:
    cmd, query, _, _ = await _stored(pg_client)
    pk = uuid4()

    await _ensure_many(cmd, [pk.hex.upper()])

    assert (await query.get(pk)).id == pk
