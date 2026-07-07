"""Firestore ``create`` fails closed on an existing id (parity with Postgres/Mongo).

Firestore's ``set`` is an upsert, so an earlier build silently overwrote a document
when ``create`` was given an id that already existed. These exercise the create-only
path end to end against the emulator: the gateway ``create`` and the client's
``create_document`` / ``insert_many`` all raise ``conflict`` on a collision instead of
clobbering the existing document.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    make_create_cmd,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WRITE_TYPES = {
    "domain": IntegrationDocument,
    "create_cmd": IntegrationCreateCmd,
    "update_cmd": IntegrationUpdateCmd,
}


def _ctx(client: FirestoreClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))


def _write(ctx: ExecutionContext, collection: str) -> object:
    return doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_enabled=False,
        tenant_aware=False,
    )


def _read(ctx: ExecutionContext, collection: str) -> object:
    return read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )


async def test_create_rejects_existing_id(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``create`` with an id that already exists conflicts and leaves the row intact."""

    collection = f"gw_create_fc_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    doc_id = UUID("60000000-0000-0000-0000-000000000001")
    first = await write.create(make_create_cmd(name="original"), id=doc_id)
    assert first.name == "original"

    with pytest.raises(CoreException) as ei:
        await write.create(make_create_cmd(name="clobber"), id=doc_id)
    assert ei.value.kind is ExceptionKind.CONFLICT

    loaded = await read.get(doc_id)
    assert loaded.name == "original"  # the second create did not overwrite


async def test_client_create_document_is_create_only(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """The low-level ``create_document`` raises ``conflict`` on an existing id."""

    coll = await firestore_client.collection(f"client_cd_{unique_collection}")
    doc_id = uuid4().hex

    await firestore_client.create_document(coll, doc_id, {"v": 1})

    with pytest.raises(CoreException) as ei:
        await firestore_client.create_document(coll, doc_id, {"v": 2})
    assert ei.value.kind is ExceptionKind.CONFLICT

    snap = await firestore_client.get_document(coll, doc_id)
    assert snap is not None and snap["v"] == 1


async def test_client_insert_many_is_create_only(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``insert_many`` (backing ``create_many``) uses create-only batch writes."""

    coll = await firestore_client.collection(f"client_im_{unique_collection}")
    taken = uuid4().hex
    fresh = uuid4().hex  # non-conflicting id in the same batch

    await firestore_client.create_document(coll, taken, {"v": 1})

    with pytest.raises(CoreException) as ei:
        await firestore_client.insert_many(
            coll, [(fresh, {"v": 9}), (taken, {"v": 9})], create_only=True
        )
    assert ei.value.kind is ExceptionKind.CONFLICT

    # The pre-existing document is unchanged.
    snap = await firestore_client.get_document(coll, taken)
    assert snap is not None and snap["v"] == 1

    # Atomic: the non-conflicting document from the same batch was not created either
    # (WriteBatch.commit() is all-or-nothing).
    assert await firestore_client.get_document(coll, fresh) is None


async def test_ensure_returns_existing_without_overwrite(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """``ensure`` on a present id returns it unchanged rather than overwriting."""

    collection = f"gw_ensure_fc_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    doc_id = UUID("60000000-0000-0000-0000-000000000002")
    created = await write.create(make_create_cmd(name="original"), id=doc_id)

    got = await write.ensure(doc_id, make_create_cmd(name="would-overwrite"))
    assert got.name == "original"
    assert got.rev == created.rev  # no new write
