"""Unit tests for the stored-file write wiring: the unfrozen binder and its freeze helper."""

import pytest

from forze import build_runtime
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authn import AuthnRequired
from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.stored_file import (
    StoredFileIdRevDTO,
    StoredFileKernelOp,
    StoredFileOutboxPayload,
    UploadStoredFileRequestDTO,
    bind_stored_file_writes,
    build_stored_file_registry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze_kits.integrations.stored_file import freeze_stored_file_registry
from forze_mock import MockDepsModule

# ----------------------- #

_WITH_OUTBOX = StoredFileKitSpec(
    name="files",
    outbox=OutboxSpec(name="files", codec=PydanticModelCodec(StoredFileOutboxPayload)),
)
_PLAIN = StoredFileKitSpec(name="files")

# op → (outbox flush step on tx success, after-commit step)
_STAGES = {
    StoredFileKernelOp.UPLOAD: ("stored_file_outbox_flush_upload", "stored_file_complete_upload"),
    StoredFileKernelOp.DELETE: ("stored_file_outbox_flush_delete", "stored_file_purge_blob"),
}


def _stages(frozen: FrozenOperationRegistry, kit: StoredFileKitSpec) -> dict[str, tuple]:
    """Per write op: its tx route, on-success step ids and after-commit step ids."""

    ns = kit.document.default_namespace
    out: dict[str, tuple] = {}

    for op in _STAGES:
        tx = frozen.plans[ns.key(op)].tx
        out[str(op)] = (tx.route, set(tx.on_success.steps), set(tx.after_commit.steps))

    return out


def test_freeze_stored_file_registry_returns_frozen_registry() -> None:
    frozen = freeze_stored_file_registry(_PLAIN, tx_route="mock")
    assert isinstance(frozen, FrozenOperationRegistry)


def test_the_binder_leaves_the_registry_open_for_guards() -> None:
    # An app layers its own authn/authz on the stored-file ops before freezing.
    reg = bind_stored_file_writes(_WITH_OUTBOX, tx_route="mock")
    upload = _WITH_OUTBOX.document.default_namespace.key(StoredFileKernelOp.UPLOAD)

    assert isinstance(reg, OperationRegistry)

    frozen = (
        reg.bind(upload).bind_outer().before(AuthnRequired().to_step()).finish(deep=True).freeze()
    )

    assert "authn.principal" in set(frozen.plans[upload].outer.before.steps)
    assert frozen.plans[upload].requires_authn


@pytest.mark.parametrize("kit", [_WITH_OUTBOX, _PLAIN], ids=["outbox", "no-outbox"])
def test_the_binder_wires_every_write_stage(kit: StoredFileKitSpec) -> None:
    frozen = bind_stored_file_writes(kit, tx_route="mock").freeze()

    expected = {
        str(op): ("mock", {flush} if kit.outbox is not None else set(), {after})
        for op, (flush, after) in _STAGES.items()
    }

    assert _stages(frozen, kit) == expected


@pytest.mark.parametrize("kit", [_WITH_OUTBOX, _PLAIN], ids=["outbox", "no-outbox"])
def test_the_freeze_helper_freezes_what_the_binder_binds(kit: StoredFileKitSpec) -> None:
    assert _stages(freeze_stored_file_registry(kit, tx_route="mock"), kit) == _stages(
        bind_stored_file_writes(kit, tx_route="mock").freeze(), kit
    )


def test_the_binder_extends_a_registry_it_is_given() -> None:
    # What the given registry already carries survives the binding.
    upload = _PLAIN.document.default_namespace.key(StoredFileKernelOp.UPLOAD)
    base = (
        build_stored_file_registry(_PLAIN)
        .bind(upload)
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )

    frozen = bind_stored_file_writes(_PLAIN, tx_route="mock", registry=base).freeze()

    assert "authn.principal" in set(frozen.plans[upload].outer.before.steps)
    assert _stages(frozen, _PLAIN)[str(StoredFileKernelOp.UPLOAD)][0] == "mock"


def test_the_binder_binds_the_namespace_the_registry_was_built_with() -> None:
    # A registry built under its own namespace has none of the default-namespace keys.
    media = StrKeyNamespace(prefix="media")
    base = build_stored_file_registry(_PLAIN, ns=media)

    frozen = bind_stored_file_writes(_PLAIN, tx_route="mock", registry=base, ns=media).freeze()
    built = bind_stored_file_writes(_PLAIN, tx_route="mock", ns=media).freeze()
    helper = freeze_stored_file_registry(_PLAIN, tx_route="mock", ns=media)

    for registry in (frozen, built, helper):
        for op in _STAGES:
            assert registry.plans[media.key(op)].tx.route == "mock"


# ....................... #
# Keeping the blob on delete: the row is soft-deleted, the object stays, the index forgets it.

_SEARCHED = StoredFileKitSpec(name="files", search=StoredFileKitSpec.default_search("files"))


def _delete_stages(frozen: FrozenOperationRegistry, kit: StoredFileKitSpec) -> tuple:
    tx = frozen.plans[kit.document.default_namespace.key(StoredFileKernelOp.DELETE)].tx
    return tx.route, set(tx.after_commit.steps)


@pytest.mark.parametrize(
    ("kit", "after_commit"),
    [(_PLAIN, set()), (_SEARCHED, {"stored_file_unindex"})],
    ids=["no-search", "search"],
)
def test_keeping_the_blob_drops_the_purge(kit: StoredFileKitSpec, after_commit: set) -> None:
    binder = bind_stored_file_writes(kit, tx_route="mock", purge_on_delete=False).freeze()
    helper = freeze_stored_file_registry(kit, tx_route="mock", purge_on_delete=False)

    # Still transactional; only the after-commit purge changes.
    assert _delete_stages(binder, kit) == ("mock", after_commit)
    assert _delete_stages(helper, kit) == ("mock", after_commit)


def test_the_blob_is_purged_by_default() -> None:
    frozen = bind_stored_file_writes(_SEARCHED, tx_route="mock").freeze()

    assert _delete_stages(frozen, _SEARCHED) == ("mock", {"stored_file_purge_blob"})


@pytest.mark.parametrize("purge", [True, False], ids=["purge", "keep"])
async def test_a_delete_through_the_registry_keeps_or_purges_the_blob(purge: bool) -> None:
    # End to end, so the stage behind each step id is what is checked, not the id.
    reg = freeze_stored_file_registry(_SEARCHED, tx_route="mock", purge_on_delete=purge)
    key = _SEARCHED.document.default_namespace.key
    runtime = build_runtime(MockDepsModule())

    async with runtime.scope():
        ctx = runtime.get_context()
        index = ctx.search.query(_SEARCHED.search_spec)
        uploaded = await run_operation(
            reg,
            key(StoredFileKernelOp.UPLOAD),
            UploadStoredFileRequestDTO(filename="kept.txt", data=b"payload"),
            ctx,
        )
        ready = await ctx.doc.query(_SEARCHED.document).get(uploaded.id)
        assert [hit.id for hit in (await index.search("kept")).hits] == [ready.id]

        await run_operation(
            reg,
            key(StoredFileKernelOp.DELETE),
            StoredFileIdRevDTO(id=ready.id, rev=ready.rev),
            ctx,
        )

        assert (await index.search("kept")).hits == []
        listed = await ctx.storage.query(_SEARCHED.resolved_storage).list(limit=10, offset=0)
        assert listed.total == (0 if purge else 1)
