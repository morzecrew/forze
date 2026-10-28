"""Integration tests for Firestore history gateway read/write paths."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_firestore.execution.deps.utils import doc_write_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.integration.test_forze_firestore._fixtures import client_context
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


async def test_history_read_many_skips_missing_revisions(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_many_{unique_collection}"
    history = f"{collection}_history"
    ctx = client_context(firestore_client)
    write = doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", history),
        history_enabled=True,
        tenant_aware=False,
    )

    created = await write.create(make_create_cmd(name="v1"))
    await write.update(
        created.id,
        IntegrationUpdateCmd(name="v2"),
        rev=created.rev,
    )

    hist = write.history_gw
    assert hist is not None

    missing = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    rows = await hist.read_many([created.id, missing], [1, 99])
    assert len(rows) == 1
    assert rows[0].name == "v1"


async def test_history_read_many_length_mismatch_raises(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_len_{unique_collection}"
    history = f"{collection}_history"
    ctx = client_context(firestore_client)
    write = doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", history),
        history_enabled=True,
        tenant_aware=False,
    )
    created = await write.create(make_create_cmd(name="x"))
    hist = write.history_gw
    assert hist is not None

    with pytest.raises(CoreException, match="same"):
        await hist.read_many([created.id], [1, 2])


async def test_history_write_many_persists_snapshots(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_wm_{unique_collection}"
    history = f"{collection}_history"
    ctx = client_context(firestore_client)
    write = doc_write_gw(
        ctx,
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", history),
        history_enabled=True,
        tenant_aware=False,
    )

    a = await write.create(make_create_cmd(name="a"))
    b = await write.create(make_create_cmd(name="b"))
    hist = write.history_gw
    assert hist is not None

    await hist.write_many([a, b])

    snap_a = await hist.read(a.id, rev=1)
    snap_b = await hist.read(b.id, rev=1)
    assert snap_a.name == "a"
    assert snap_b.name == "b"


async def test_a_future_revision_is_a_revision_mismatch(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_future_{unique_collection}"
    write = doc_write_gw(
        client_context(firestore_client),
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", f"{collection}_history"),
        history_enabled=True,
        tenant_aware=False,
    )
    created = await write.create(make_create_cmd(name="v1"))

    with pytest.raises(CoreException) as ei:
        await write.update(created.id, IntegrationUpdateCmd(name="v2"), rev=created.rev + 1)

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "revision_mismatch"


async def test_a_missing_history_snapshot_is_a_precondition_to_resubmit(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_lost_{unique_collection}"
    history = f"{collection}_history"
    write = doc_write_gw(
        client_context(firestore_client),
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", history),
        history_enabled=True,
        tenant_aware=False,
    )
    created = await write.create(make_create_cmd(name="v1"))
    await write.update(created.id, IntegrationUpdateCmd(name="theirs"), rev=created.rev)

    # Lost history: every snapshot gone.
    async for snapshot in (await firestore_client.collection(history)).stream():
        await snapshot.reference.delete()

    with pytest.raises(CoreException) as ei:
        await write.update(created.id, IntegrationUpdateCmd(name="ours"), rev=created.rev)

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.code == "history_not_found_retry"


async def test_a_strict_check_refuses_the_stale_rev_history_would_merge(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_strict_{unique_collection}"
    write = doc_write_gw(
        client_context(firestore_client),
        write_types=_WRITE_TYPES,
        write_relation=("(default)", collection),
        history_relation=("(default)", f"{collection}_history"),
        history_enabled=True,
        tenant_aware=False,
    )
    created = await write.create(make_create_cmd(name="v1"))
    other = await write.create(make_create_cmd(name="o1"))
    await write.update(created.id, IntegrationUpdateCmd(name="theirs"), rev=created.rev)

    # Merged by default: resending the value it now holds changes nothing, at a stale rev.
    await write.update(created.id, IntegrationUpdateCmd(name="theirs"), rev=created.rev)
    refused: list[str | None] = []

    for stale in (
        lambda: write.update(
            created.id, IntegrationUpdateCmd(name="theirs"), rev=created.rev, merge=False
        ),
        lambda: write.touch(created.id, rev=created.rev),
        lambda: write.touch_many([other.id, created.id], revs=[other.rev, created.rev]),
    ):
        with pytest.raises(CoreException) as ei:
            await stale()

        refused.append(ei.value.code)

    assert refused == ["revision_mismatch"] * 3
    assert (await write.touch(created.id, rev=created.rev + 1)).rev == created.rev + 2
