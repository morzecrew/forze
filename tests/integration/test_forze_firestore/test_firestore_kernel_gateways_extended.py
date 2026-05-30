"""Extended Firestore kernel gateway integration (batch, cursor, count, lifecycle)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.querying import encode_keyset_v1
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.constants import ID_FIELD
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw, read_gw
from forze_firestore.kernel.platform import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    make_create_cmd,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WRITE_TYPES = {
    "domain": IntegrationDocument,
    "create_cmd": IntegrationCreateCmd,
    "update_cmd": IntegrationUpdateCmd,
}


def _ctx(client: FirestoreClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))


def _write(
    ctx: ExecutionContext,
    collection: str,
    *,
    history: str | None = None,
) -> object:
    return doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", history) if history else None,
        history_enabled=history is not None,
        tenant_aware=False,
    )


def _read(ctx: ExecutionContext, collection: str) -> object:
    return read_gw(
        ctx,
        read_type=IntegrationDocument,
        read_relation=("(default)", collection),
        tenant_aware=False,
    )


async def test_firestore_read_gateway_get_many_preserves_order(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_many_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    id_a = UUID("10000000-0000-0000-0000-000000000011")
    id_b = UUID("10000000-0000-0000-0000-000000000012")
    id_c = UUID("10000000-0000-0000-0000-000000000013")

    await write.create(make_create_cmd(name="a", doc_id=id_c))
    await write.create(make_create_cmd(name="b", doc_id=id_a))
    await write.create(make_create_cmd(name="c", doc_id=id_b))

    ordered = await read.get_many([id_b, id_a, id_c])
    assert [d.name for d in ordered] == ["c", "b", "a"]


async def test_firestore_read_gateway_get_many_missing_raises(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_miss_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    existing = await write.create(make_create_cmd(name="one"))
    missing = uuid4()

    with pytest.raises(CoreException, match="not found"):
        await read.get_many([existing.id, missing])


async def test_firestore_read_gateway_find_many_with_cursor(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_cur_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    ids = [
        UUID("20000000-0000-0000-0000-000000000001"),
        UUID("20000000-0000-0000-0000-000000000002"),
        UUID("20000000-0000-0000-0000-000000000003"),
    ]
    for doc_id, label in zip(ids, ("c", "b", "a"), strict=True):
        await write.create(make_create_cmd(name=label, doc_id=doc_id))

    first = await read.find_many_with_cursor(
        None,
        cursor={"limit": 2},
        sorts={ID_FIELD: "asc"},
        return_model=IntegrationDocument,
    )
    assert len(first) >= 2

    tok = encode_keyset_v1(
        sort_keys=[ID_FIELD],
        directions=["asc"],
        values=[str(first[1].id)],
    )

    with pytest.raises(CoreException, match="at most one"):
        await read.find_many_with_cursor(
            None,
            cursor={"after": tok, "before": tok},
            sorts={ID_FIELD: "asc"},
        )

    with pytest.raises(CoreException, match="positive"):
        await read.find_many_with_cursor(None, cursor={"limit": 0})


async def test_firestore_read_gateway_count(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_cnt_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    await write.create(make_create_cmd(name="x", doc_id=uuid4()))
    await write.create(make_create_cmd(name="y", doc_id=uuid4()))

    total = await read.count(None)
    assert total == 2


async def test_firestore_write_gateway_create_many_and_update_many(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_batch_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)

    created = await write.create_many(
        [
            make_create_cmd(name="b1", doc_id=UUID("30000000-0000-0000-0000-000000000001")),
            make_create_cmd(name="b2", doc_id=UUID("30000000-0000-0000-0000-000000000002")),
        ],
    )
    assert len(created) == 2
    assert {d.name for d in created} == {"b1", "b2"}

    updated, _diffs = await write.update_many(
        [d.id for d in created],
        [
            IntegrationUpdateCmd(name="u1"),
            IntegrationUpdateCmd(name="u2"),
        ],
        revs=[d.rev for d in created],
    )
    assert {d.name for d in updated} == {"u1", "u2"}


async def test_firestore_write_gateway_ensure_and_kill(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_ens_{unique_collection}"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection)
    read = _read(ctx, collection)

    existing_id = UUID("40000000-0000-0000-0000-000000000001")
    new_id = UUID("40000000-0000-0000-0000-000000000002")

    seeded = await write.create(make_create_cmd(name="first", doc_id=existing_id))
    again = await write.ensure(make_create_cmd(name="ignored", doc_id=existing_id))
    assert again.id == seeded.id
    assert again.name == "first"

    other = await write.create(make_create_cmd(name="second", doc_id=new_id))
    ensured = await write.ensure_many(
        [
            make_create_cmd(name="ignored", doc_id=existing_id),
            make_create_cmd(name="ignored", doc_id=new_id),
        ],
    )
    assert len(ensured) == 2
    assert {d.id for d in ensured} == {existing_id, other.id}

    await write.kill(existing_id)
    with pytest.raises(CoreException, match="not found"):
        await read.get(existing_id)

    await write.kill_many([new_id])
    with pytest.raises(CoreException, match="not found"):
        await read.get(new_id)


async def test_firestore_write_gateway_history_reads_prior_revision(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"gw_hist_{unique_collection}"
    history = f"{collection}_history"
    ctx = _ctx(firestore_client)
    write = _write(ctx, collection, history=history)

    created = await write.create(make_create_cmd(name="v1"))
    await write.update(
        created.id,
        IntegrationUpdateCmd(name="v2"),
        rev=created.rev,
    )

    hist_gw = write.history_gw
    assert hist_gw is not None
    snap_v1 = await hist_gw.read(created.id, rev=1)
    assert snap_v1.name == "v1"
