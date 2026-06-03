"""Freeze a stored-file registry with transaction and after-commit stages."""

from __future__ import annotations

from forze.application.contracts.execution import OnSuccessStep
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze_kits.domain.stored_file import StoredFileKitSpec

from .factories import build_stored_file_registry
from .operations import StoredFileKernelOp
from .stages import (
    stored_file_complete_upload_after_commit_factory,
    stored_file_outbox_flush_factory,
    stored_file_purge_blob_after_commit_factory,
)

# ----------------------- #


def freeze_stored_file_registry(
    kit: StoredFileKitSpec,
    *,
    tx_route: str = "default",
    registry: OperationRegistry | None = None,
) -> FrozenOperationRegistry:
    """Build, patch, and freeze a stored-file operation registry.

    Write operations (``upload``, ``delete``) run in a transaction. Outbox rows
    flush on tx success; blob upload and purge run in ``after_commit`` hooks.
    """

    reg = registry if registry is not None else build_stored_file_registry(kit)
    ns = kit.document.default_namespace

    upload_key = ns.key(StoredFileKernelOp.UPLOAD)
    delete_key = ns.key(StoredFileKernelOp.DELETE)

    upload_plan = reg.bind(upload_key).bind_tx().set_route(tx_route)
    if kit.outbox is not None:
        upload_plan = upload_plan.on_success(
            OnSuccessStep(
                id="stored_file_outbox_flush_upload",
                factory=stored_file_outbox_flush_factory(kit.outbox),
            )
        )
    reg = upload_plan.after_commit(
        OnSuccessStep(
            id="stored_file_complete_upload",
            factory=stored_file_complete_upload_after_commit_factory(kit),
        )
    ).finish(deep=True)

    delete_plan = reg.bind(delete_key).bind_tx().set_route(tx_route)
    if kit.outbox is not None:
        delete_plan = delete_plan.on_success(
            OnSuccessStep(
                id="stored_file_outbox_flush_delete",
                factory=stored_file_outbox_flush_factory(kit.outbox),
            )
        )
    reg = delete_plan.after_commit(
        OnSuccessStep(
            id="stored_file_purge_blob",
            factory=stored_file_purge_blob_after_commit_factory(kit),
        )
    ).finish(deep=True)

    return reg.freeze()
