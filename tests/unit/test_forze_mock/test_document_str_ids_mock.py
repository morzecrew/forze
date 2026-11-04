"""The mock reads a ``str`` id in an id-taking batch command as the real adapter does: as the
``UUID`` it names, so two spellings of one id are one id."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import KeyedCreate, KeyedUpdate
from forze.base.exceptions import CoreException
from tests.unit.test_forze_mock.test_set_based_upsert_mock import _Create, _mock, _Update

pytestmark = pytest.mark.unit

_Command = Callable[[Any, list[Any]], Awaitable[Any]]


def _ensure_many(doc: Any, ids: list[Any]) -> Awaitable[Any]:
    return doc.ensure_many([KeyedCreate(id=i, payload=_Create(name="e")) for i in ids])


def _update_many(doc: Any, ids: list[Any]) -> Awaitable[Any]:
    return doc.update_many([KeyedUpdate(id=i, rev=1, dto=_Update(name="patched")) for i in ids])


def _touch_many(doc: Any, ids: list[Any]) -> Awaitable[Any]:
    return doc.touch_many(ids)


def _kill_many(doc: Any, ids: list[Any]) -> Awaitable[Any]:
    return doc.kill_many(ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "expected"),
    [
        pytest.param(_ensure_many, ("a", 1), id="ensure_many"),
        pytest.param(_update_many, ("patched", 2), id="update_many"),
        pytest.param(_touch_many, ("a", 2), id="touch_many"),
        pytest.param(_kill_many, None, id="kill_many"),
    ],
)
async def test_a_str_id_names_the_stored_row(
    command: _Command, expected: tuple[str, int] | None
) -> None:
    doc = _mock()
    pk = uuid4()
    await doc.create(_Create(name="a"), id=pk)

    await command(doc, [pk.hex.upper()])

    rows = [(r.name, r.rev) for r in (await doc.find_many()).hits]
    assert rows == ([] if expected is None else [expected])


@pytest.mark.asyncio
@pytest.mark.parametrize("command", [_ensure_many, _update_many, _touch_many, _kill_many])
async def test_two_spellings_of_one_id_or_a_malformed_one_are_refused(command: _Command) -> None:
    doc = _mock()
    pk = uuid4()
    await doc.create(_Create(name="a"), id=pk)

    with pytest.raises(CoreException):
        await command(doc, [pk, str(pk)])

    with pytest.raises(CoreException) as refused:
        await command(doc, ["not-a-uuid"])

    assert refused.value.kind == "precondition"
    assert [(r.name, r.rev) for r in (await doc.find_many()).hits] == [("a", 1)]


_SINGLE: list[Any] = [
    pytest.param(
        lambda doc, pk: doc.update(pk, 1, _Update(name="patched")), [("patched", 2)], id="update"
    ),
    pytest.param(lambda doc, pk: doc.touch(pk), [("a", 2)], id="touch"),
    pytest.param(lambda doc, pk: doc.kill(pk), [], id="kill"),
    pytest.param(
        lambda doc, pk: doc.upsert(pk, _Create(name="x"), _Update(name="patched")),
        [("patched", 2)],
        id="upsert",
    ),
    pytest.param(lambda doc, pk: doc.ensure(pk, _Create(name="x")), [("a", 1)], id="ensure"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("command", "expected"), _SINGLE)
async def test_a_str_pk_names_the_stored_row_in_a_single_id_command(
    command: Callable[[Any, Any], Awaitable[Any]], expected: list[tuple[str, int]]
) -> None:
    doc = _mock()
    pk = uuid4()
    await doc.create(_Create(name="a"), id=pk.hex.upper())

    await command(doc, pk.hex.upper())

    assert [(r.id, r.name, r.rev) for r in (await doc.find_many()).hits] == [
        (pk, *row) for row in expected
    ]

    with pytest.raises(CoreException) as refused:
        await command(doc, "not-a-uuid")

    assert refused.value.kind == "precondition"


@pytest.mark.asyncio
async def test_a_read_by_a_str_pk_finds_the_row_it_names() -> None:
    doc = _mock()
    pk = uuid4()
    await doc.create(_Create(name="a"), id=pk)

    assert (await doc.get(pk.hex.upper())).id == pk
    assert [r.id for r in await doc.get_many([pk.hex.upper(), str(pk)])] == [pk, pk]

    for read in (lambda: doc.get("not-a-uuid"), lambda: doc.get_many(["not-a-uuid"])):
        with pytest.raises(CoreException) as refused:
            await read()

        assert refused.value.kind == "precondition"
