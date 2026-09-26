"""Conformance battery for owned reads — ``get`` / ``get_many`` with ``owned_by``.

``owned_by`` exists so that a row belonging to someone else is **not found**: the same error,
from the same read, as a row that does not exist. "The same" is the whole claim. A foreign row
that raises a different code, a different kind, or a not-found that does not name its resource
type is still an existence check — the last one because a non-disclosing posture only collapses
errors that say which type they are about.

The claims:

1. **the owner reads their own row**, singly and in a batch;
2. **a foreign row is not found**, and the not-found names the spec as its resource type;
3. **a foreign row reads exactly like a missing one** — kind, code and resource type, and under
   a non-disclosing posture the rendered envelope;
4. **a batch holding a foreign id fails like a batch holding a missing id**;
5. **the cache cannot serve a foreign row** — the owner's read puts the row in the cache (the
   leg proves it did, where it has a cache), and another principal's read of it is still
   not found;
6. **a misspelled owner field is refused, never answered** — a field the read model lacks would
   otherwise turn every read into a not-found, which looks exactly like correct enforcement.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, final
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes, OwnedBy
from forze.application.execution import ExecutionContext
from forze.base.exceptions import (
    CoreException,
    DenialPosture,
    ExceptionKind,
    configure_denial_posture,
    error_envelope,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockCacheAdapter, MockState, MockStateDepKey

# ----------------------- #


class OwnedDoc(Document):
    owner_id: UUID
    title: str = ""


class OwnedRead(ReadDocument):
    owner_id: UUID
    title: str = ""


class OwnedCreate(CreateDocumentCmd):
    owner_id: UUID
    title: str = ""


class OwnedUpdate(BaseDTO):
    title: str | None = None


def owned_spec(name: str, **kwargs: Any) -> DocumentSpec[Any, Any, Any, Any]:
    """A spec over :class:`OwnedDoc`; *kwargs* pass through (a cache, for instance)."""

    return DocumentSpec[OwnedRead, OwnedDoc, OwnedCreate, OwnedUpdate](
        name=name,
        read=OwnedRead,
        write=DocumentWriteTypes(
            domain=OwnedDoc,
            create_cmd=OwnedCreate,
            update_cmd=OwnedUpdate,
        ),
        **kwargs,
    )


OWNED_DDL = """
CREATE TABLE {table} (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    owner_id uuid NOT NULL,
    title text NOT NULL
)
"""
"""Postgres table for :class:`OwnedDoc` (``{table}`` is the relation name)."""


def mock_read_cache(cache: CacheSpec) -> tuple[dict[Any, Any], Callable[[UUID], Awaitable[bool]]]:
    """An in-memory read cache for a real backend: its deps, and whether it holds a pk."""

    state = MockState()

    def _factory(ctx: ExecutionContext, spec: CacheSpec) -> MockCacheAdapter:
        return MockCacheAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=spec.name)

    async def _holds(pk: UUID) -> bool:
        return any(key[0] == str(pk) for key in state.cache_bodies.get(cache.name, {}))

    return {MockStateDepKey: state, CacheDepKey: _factory}, _holds


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class OwnedReadsHarness:
    """One document backend under test, over a spec built by :func:`owned_spec`."""

    query: Any
    """The :class:`DocumentQueryPort` for the spec."""

    command: Any
    """The :class:`DocumentCommandPort` for the spec."""

    spec_name: str
    """The spec's name — the resource type every not-found must carry."""

    cache_holds: Callable[[UUID], Awaitable[bool]] | None = None
    """Whether the read cache holds *pk*; ``None`` for a leg with no cache."""


Check = Callable[[OwnedReadsHarness], Awaitable[None]]

_OWNER_FIELD = "owner_id"


def _owned(owner: UUID) -> OwnedBy:
    return OwnedBy(field=_OWNER_FIELD, value=owner)


async def _row(h: OwnedReadsHarness, owner: UUID) -> UUID:
    created = await h.command.create(OwnedCreate(owner_id=owner, title="t"))
    return created.id


async def _error(read: Awaitable[Any]) -> CoreException:
    with pytest.raises(CoreException) as caught:
        await read

    return caught.value


def _same_answer(h: OwnedReadsHarness, foreign: CoreException, missing: CoreException) -> None:
    """A foreign row and a missing one are one answer, server side and on the wire."""

    for error in (foreign, missing):
        assert error.kind is ExceptionKind.NOT_FOUND, error
        assert error.resource_type == h.spec_name, error

    assert foreign.code == missing.code

    previous = configure_denial_posture(
        DenialPosture(mode="non_disclosing", resource_types=frozenset({h.spec_name}))
    )

    try:
        assert error_envelope(foreign) == error_envelope(missing)

    finally:
        configure_denial_posture(previous)


# ....................... #


async def check_the_owner_reads_their_row(h: OwnedReadsHarness) -> None:
    owner = uuid4()
    pk = await _row(h, owner)

    assert (await h.query.get(pk, owned_by=_owned(owner))).id == pk
    assert [row.id for row in await h.query.get_many([pk], owned_by=_owned(owner))] == [pk]


async def check_a_foreign_row_is_not_found(h: OwnedReadsHarness) -> None:
    pk = await _row(h, uuid4())

    error = await _error(h.query.get(pk, owned_by=_owned(uuid4())))

    assert error.kind is ExceptionKind.NOT_FOUND
    assert error.resource_type == h.spec_name


async def check_a_foreign_row_reads_like_a_missing_one(h: OwnedReadsHarness) -> None:
    pk = await _row(h, uuid4())
    stranger = _owned(uuid4())

    foreign = await _error(h.query.get(pk, owned_by=stranger))
    missing = await _error(h.query.get(uuid4(), owned_by=stranger))

    _same_answer(h, foreign, missing)


async def check_a_batch_with_a_foreign_id_fails_like_a_missing_one(h: OwnedReadsHarness) -> None:
    owner = uuid4()
    mine = await _row(h, owner)
    theirs = await _row(h, uuid4())

    foreign = await _error(h.query.get_many([mine, theirs], owned_by=_owned(owner)))
    missing = await _error(h.query.get_many([mine, uuid4()], owned_by=_owned(owner)))

    _same_answer(h, foreign, missing)


async def check_the_cache_cannot_serve_a_foreign_row(h: OwnedReadsHarness) -> None:
    owner = uuid4()
    pk = await _row(h, owner)

    await h.query.get(pk, owned_by=_owned(owner))

    if h.cache_holds is not None:
        # Otherwise the leg below would read the database and pass without touching the cache.
        assert await h.cache_holds(pk), "the owner's read did not put the row in the cache"

    stranger = _owned(uuid4())

    assert (await _error(h.query.get(pk, owned_by=stranger))).kind is ExceptionKind.NOT_FOUND
    assert (await _error(h.query.get_many([pk], owned_by=stranger))).kind is ExceptionKind.NOT_FOUND


async def check_a_misspelled_owner_field_is_refused(h: OwnedReadsHarness) -> None:
    owner = uuid4()
    pk = await _row(h, owner)
    misspelled = OwnedBy(field="ownr_id", value=owner)

    for read in (h.query.get(pk, owned_by=misspelled), h.query.get_many([pk], owned_by=misspelled)):
        error = await _error(read)
        assert (error.kind, error.code) == (
            ExceptionKind.CONFIGURATION,
            "owned_by_unknown_field",
        ), error


OWNED_READS_BATTERY: tuple[Check, ...] = (
    check_the_owner_reads_their_row,
    check_a_foreign_row_is_not_found,
    check_a_foreign_row_reads_like_a_missing_one,
    check_a_batch_with_a_foreign_id_fails_like_a_missing_one,
    check_the_cache_cannot_serve_a_foreign_row,
    check_a_misspelled_owner_field_is_refused,
)
