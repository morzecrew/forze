"""Integration coverage for write gateway edge paths and ``update_matching``."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.gateways import MongoWriteGateway
from tests.support.execution_context import context_from_deps

# ----------------------- #


class WmDoc(Document):
    name: str
    category: str = "default"


class WmCreate(CreateDocumentCmd):
    name: str
    category: str = "default"


class WmUpdate(BaseDTO):
    name: str | None = None
    category: str | None = None


def _wm_write_types() -> DocumentWriteTypes[WmDoc, WmCreate, WmUpdate]:
    return DocumentWriteTypes(
        domain=WmDoc,
        create_cmd=WmCreate,
        update_cmd=WmUpdate,
    )


def _wm_write_types_no_update() -> DocumentWriteTypes[WmDoc, WmCreate, WmUpdate]:
    return DocumentWriteTypes(domain=WmDoc, create_cmd=WmCreate)


@pytest.fixture
def wm_ctx(mongo_client: MongoClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))


async def _new_relation(mongo_client: MongoClient, prefix: str) -> tuple[str, str]:
    db_name = (await mongo_client.db()).name
    return (db_name, f"{prefix}_{uuid4().hex[:8]}")


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_batches_and_empty_match(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``update_matching`` pages by id, bumps revs, and returns 0 on no match."""

    relation = await _new_relation(mongo_client, "wm_um")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )

    for i in range(5):
        await write.create(WmCreate(name=f"n{i}", category="books"))
    await write.create(WmCreate(name="other", category="hardware"))

    # Small batch_size to force multiple keyset pages.
    total, domains = await write.update_matching(
        {"$values": {"category": {"$eq": "books"}}},
        WmUpdate(category="archived"),
        batch_size=2,
    )
    assert total == 5
    assert len(domains) == 5
    assert all(d.category == "archived" for d in domains)
    assert all(d.rev == 2 for d in domains)

    # No documents match -> early break, total 0.
    total_none, domains_none = await write.update_matching(
        {"$values": {"category": {"$eq": "nonexistent"}}},
        WmUpdate(name="x"),
        batch_size=2,
    )
    assert total_none == 0
    assert domains_none == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_matching_empty_payload_returns_zero(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """An update DTO with no set fields short-circuits to ``(0, [])``."""

    relation = await _new_relation(mongo_client, "wm_um_empty")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    await write.create(WmCreate(name="a", category="books"))

    total, domains = await write.update_matching(
        {"$values": {"category": {"$eq": "books"}}},
        WmUpdate(),
    )
    assert total == 0
    assert domains == []


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_touch_and_touch_many_bump_rev(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``touch`` / ``touch_many`` bump revisions without changing data."""

    relation = await _new_relation(mongo_client, "wm_touch")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="a"))
    b = await write.create(WmCreate(name="b"))

    touched = await write.touch(a.id)
    assert touched.rev == a.rev + 1
    assert touched.name == "a"

    many = await write.touch_many([a.id, b.id])
    assert {d.id for d in many} == {a.id, b.id}
    assert next(d for d in many if d.id == b.id).rev == b.rev + 1

    # Empty pks short-circuits ``_patch_many`` (no updates path).
    assert await write.touch_many([]) == []

    with pytest.raises(CoreException, match="unique"):
        await write.touch_many([a.id, a.id])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kill_many_empty_and_unique_and_delete(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``kill_many`` no-ops on empty, guards duplicates, and deletes."""

    relation = await _new_relation(mongo_client, "wm_kill")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    a = await write.create(WmCreate(name="a"))
    b = await write.create(WmCreate(name="b"))

    await write.kill_many([])  # no-op

    with pytest.raises(CoreException, match="unique"):
        await write.kill_many([a.id, a.id])

    await write.kill_many([a.id, b.id])
    with pytest.raises(CoreException, match="not found"):
        await read.get(a.id)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_ensure_upsert_many_empty_noops(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """Empty inputs short-circuit the bulk write methods."""

    relation = await _new_relation(mongo_client, "wm_empty")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    assert await write.create_many([]) == []
    assert await write.ensure_many([]) == []
    assert await write.upsert_many([]) == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ensure_many_all_new_no_conflicts(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``ensure_many`` with only fresh ids has no conflict lookup (empty existing)."""

    relation = await _new_relation(mongo_client, "wm_allnew")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    out = await write.ensure_many(
        [WmCreate(name="x"), WmCreate(name="y")],
        batch_size=10,
    )
    assert {d.name for d in out} == {"x", "y"}
    assert all(d.rev == 1 for d in out)

    # All ids already exist -> no inserts, conflict lookup returns existing docs.
    again = await write.ensure_many(
        [WmCreate(id=d.id, name="ignored") for d in out],
        batch_size=10,
    )
    assert {d.name for d in again} == {"x", "y"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_upsert_many_all_new_then_all_existing(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``upsert_many`` covers the all-inserted and all-existing branches."""

    relation = await _new_relation(mongo_client, "wm_um_branches")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )

    # All new -> only the insert branch runs (no update path).
    out = await write.upsert_many(
        [
            (WmCreate(name="a"), WmUpdate(name="ignored")),
            (WmCreate(name="b"), WmUpdate(name="ignored")),
        ],
        batch_size=10,
    )
    assert {d.name for d in out} == {"a", "b"}
    assert all(d.rev == 1 for d in out)

    # All existing -> only the update branch runs (no inserts).
    again = await write.upsert_many(
        [(WmCreate(id=d.id, name="ignored"), WmUpdate(name=f"u-{d.name}")) for d in out],
        batch_size=10,
    )
    assert {d.name for d in again} == {"u-a", "u-b"}
    assert all(d.rev == 2 for d in again)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_many_revs_length_mismatch(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``update_many`` rejects a ``revs`` list whose length differs from pks."""

    relation = await _new_relation(mongo_client, "wm_revs")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="a"))

    with pytest.raises(CoreException, match="revisions"):
        await write.update_many([a.id], [WmUpdate(name="x")], revs=[1, 2])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_no_diff_returns_current(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """Updating with the same value yields an empty diff and no rev bump."""

    relation = await _new_relation(mongo_client, "wm_nodiff")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="same", category="books"))

    updated, diff = await write.update(a.id, WmUpdate(name="same"))
    assert diff == {}
    assert updated.rev == a.rev


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_many_no_diff_returns_currents(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """``update_many`` returns current docs and empty diffs when nothing changes."""

    relation = await _new_relation(mongo_client, "wm_nodiff_many")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="a"))
    b = await write.create(WmCreate(name="b"))

    updated, diffs = await write.update_many(
        [a.id, b.id],
        [WmUpdate(name="a"), WmUpdate(name="b")],
    )
    assert [d.rev for d in updated] == [a.rev, b.rev]
    assert diffs == [{}, {}]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrency_conflict_on_stale_rev(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """A stale expected ``rev`` raises a concurrency error from ``_patch``."""

    relation = await _new_relation(mongo_client, "wm_conc")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="a"))
    await write.update(a.id, WmUpdate(name="b"))

    # Expected rev is now stale (doc moved to rev 2).
    with pytest.raises(CoreException) as err:
        await write.update(a.id, WmUpdate(name="c"), rev=a.rev)
    assert err.value.kind in (ExceptionKind.CONCURRENCY, ExceptionKind.PRECONDITION)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_require_update_cmd_raises_without_update_type(
    mongo_client: MongoClient,
    wm_ctx: ExecutionContext,
) -> None:
    """Methods needing an update command fail when no update type is configured."""

    relation = await _new_relation(mongo_client, "wm_noupd")
    write = doc_write_gw(
        wm_ctx,
        write_types=_wm_write_types_no_update(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    a = await write.create(WmCreate(name="a"))

    with pytest.raises(CoreException, match="Update command type is not supported"):
        await write.update(a.id, WmUpdate(name="x"))


# ....................... #


def _read_gw_for(
    client: MongoClient,
    relation: tuple[str, str],
    *,
    tenant_aware: bool = False,
) -> Any:
    from forze.application.contracts.codecs import default_model_codec
    from forze_mongo.kernel.gateways import MongoReadGateway

    return MongoReadGateway(
        relation=relation,
        client=client,
        model_type=WmDoc,
        codec=default_model_codec(WmDoc),
        tenant_aware=tenant_aware,
    )


def _direct_write_gw(
    mongo_client: MongoClient,
    relation: tuple[str, str],
    *,
    read_relation: tuple[str, str] | None = None,
    read_client: MongoClient | None = None,
    read_tenant_aware: bool = False,
    tenant_aware: bool = False,
    update_codec_none: bool = False,
) -> MongoWriteGateway:
    from forze.application.contracts.codecs import default_model_codec

    domain_codec = default_model_codec(WmDoc)
    create_codec = default_model_codec(WmDoc)
    update_codec = None if update_codec_none else default_model_codec(WmUpdate)

    read = _read_gw_for(
        read_client or mongo_client,
        read_relation or relation,
        tenant_aware=read_tenant_aware,
    )

    return MongoWriteGateway(
        relation=relation,
        client=mongo_client,
        model_type=WmDoc,
        codec=domain_codec,
        create_cmd_type=WmCreate,
        update_cmd_type=WmUpdate,
        read_gw=read,
        create_codec=create_codec,
        update_codec=update_codec,
        history_gw=None,
        tenant_aware=tenant_aware,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_gateway_relation_mismatch_raises(
    mongo_client: MongoClient,
) -> None:
    """A read gateway with a different relation is rejected at construction."""

    db_name = (await mongo_client.db()).name
    relation = (db_name, f"wm_a_{uuid4().hex[:8]}")
    other = (db_name, f"wm_b_{uuid4().hex[:8]}")

    with pytest.raises(CoreException, match="Relation mismatch"):
        _direct_write_gw(mongo_client, relation, read_relation=other)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_gateway_tenant_mismatch_raises(
    mongo_client: MongoClient,
) -> None:
    """A read gateway with differing tenant awareness is rejected."""

    db_name = (await mongo_client.db()).name
    relation = (db_name, f"wm_t_{uuid4().hex[:8]}")

    with pytest.raises(CoreException, match="Tenant awareness mismatch"):
        _direct_write_gw(
            mongo_client,
            relation,
            tenant_aware=True,
            read_tenant_aware=False,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_gateway_update_codec_required_raises(
    mongo_client: MongoClient,
) -> None:
    """``_patch_codec`` requires an update codec when update commands are supported."""

    db_name = (await mongo_client.db()).name
    relation = (db_name, f"wm_pc_{uuid4().hex[:8]}")

    write = _direct_write_gw(mongo_client, relation, update_codec_none=True)
    a = await write.create(WmCreate(name="a"))

    with pytest.raises(CoreException, match="Update codec is required"):
        await write.update(a.id, WmUpdate(name="x"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_gateway_client_mismatch_raises(
    mongo_client: MongoClient,
    mongo_container,
) -> None:
    """A read gateway bound to a different client instance is rejected."""

    from forze.application.contracts.codecs import default_model_codec

    db_name = (await mongo_client.db()).name
    relation = (db_name, f"wm_cm_{uuid4().hex[:8]}")

    other_client = MongoClient()
    await other_client.initialize(mongo_container.get_connection_url(), db_name=db_name)
    try:
        read = _read_gw_for(other_client, relation)
        with pytest.raises(CoreException, match="Client mismatch"):
            MongoWriteGateway(
                relation=relation,
                client=mongo_client,
                model_type=WmDoc,
                codec=default_model_codec(WmDoc),
                create_cmd_type=WmCreate,
                update_cmd_type=WmUpdate,
                read_gw=read,
                create_codec=default_model_codec(WmDoc),
                update_codec=default_model_codec(WmUpdate),
                history_gw=None,
                tenant_aware=False,
            )
    finally:
        await other_client.close()


def _history_gw_for(
    client: MongoClient,
    *,
    relation: tuple[str, str],
    target_relation: tuple[str, str],
    tenant_aware: bool = False,
):
    from forze_mongo.kernel.gateways import MongoHistoryGateway
    from tests.unit._gateway_codec_helpers import history_codecs_for

    domain_codec, history_codec = history_codecs_for(WmDoc)
    return MongoHistoryGateway(
        relation=relation,
        target_relation=target_relation,
        client=client,
        model_type=WmDoc,
        codec=domain_codec,
        history_codec=history_codec,
        tenant_aware=tenant_aware,
    )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "match"),
    [
        ("client", "Client mismatch"),
        ("relation", "Relation mismatch"),
        ("tenant", "Tenant awareness mismatch"),
    ],
)
async def test_write_gateway_history_mismatch_raises(
    mongo_client: MongoClient,
    mongo_container,
    kind: str,
    match: str,
) -> None:
    """History gateway client / relation / tenant-awareness mismatches are rejected."""

    from forze.application.contracts.codecs import default_model_codec

    db_name = (await mongo_client.db()).name
    relation = (db_name, f"wm_hm_{uuid4().hex[:8]}")
    hist_relation = (db_name, f"wm_h_{uuid4().hex[:8]}")
    wt_tenant = False
    other_client: MongoClient | None = None

    if kind == "client":
        other_client = MongoClient()
        await other_client.initialize(
            mongo_container.get_connection_url(), db_name=db_name
        )
        hist = _history_gw_for(
            other_client, relation=hist_relation, target_relation=relation
        )
    elif kind == "relation":
        target = (db_name, f"wm_other_{uuid4().hex[:8]}")
        hist = _history_gw_for(
            mongo_client, relation=hist_relation, target_relation=target
        )
    else:
        hist = _history_gw_for(
            mongo_client,
            relation=hist_relation,
            target_relation=relation,
            tenant_aware=True,
        )

    read = _read_gw_for(mongo_client, relation, tenant_aware=wt_tenant)

    try:
        with pytest.raises(CoreException, match=match):
            MongoWriteGateway(
                relation=relation,
                client=mongo_client,
                model_type=WmDoc,
                codec=default_model_codec(WmDoc),
                create_cmd_type=WmCreate,
                update_cmd_type=WmUpdate,
                read_gw=read,
                create_codec=default_model_codec(WmDoc),
                update_codec=default_model_codec(WmUpdate),
                history_gw=hist,
                tenant_aware=wt_tenant,
            )
    finally:
        if other_client is not None:
            await other_client.close()
