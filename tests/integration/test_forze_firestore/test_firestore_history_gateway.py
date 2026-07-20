"""Integration tests for Firestore history gateway read/write paths."""

from __future__ import annotations

from uuid import UUID

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.execution.deps.utils import doc_write_gw
from forze_firestore.kernel.client import FirestoreClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    make_create_cmd,
)
from tests.support.execution_context import (
    context_from_deps,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WRITE_TYPES = {
    "domain": IntegrationDocument,
    "create_cmd": IntegrationCreateCmd,
    "update_cmd": IntegrationUpdateCmd,
}


def _ctx(client: FirestoreClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({FirestoreClientDepKey: client}))


async def test_history_read_many_skips_missing_revisions(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    collection = f"hist_many_{unique_collection}"
    history = f"{collection}_history"
    ctx = _ctx(firestore_client)
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
    ctx = _ctx(firestore_client)
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
    ctx = _ctx(firestore_client)
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
