"""Unit tests for the stored-file write wiring: the unfrozen binder and its freeze helper."""

import pytest

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authn import AuthnRequired
from forze.base.serialization import PydanticModelCodec
from forze_kits.aggregates.stored_file import (
    StoredFileKernelOp,
    StoredFileOutboxPayload,
    bind_stored_file_writes,
    build_stored_file_registry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec
from forze_kits.integrations.stored_file import freeze_stored_file_registry

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
