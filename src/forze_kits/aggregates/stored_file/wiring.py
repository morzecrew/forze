"""Freeze a stored-file registry with transaction and after-commit stages."""

from __future__ import annotations

from typing import Callable

from forze.application.contracts.execution import OnSuccessFactory, OnSuccessStep
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


def _bind_write_op(
    reg: OperationRegistry,
    *,
    key: str,
    tx_route: str,
    kit: StoredFileKitSpec,
    flush_step_id: str,
    after_commit_step_id: str,
    after_commit_factory: Callable[[StoredFileKitSpec], OnSuccessFactory],
) -> OperationRegistry:
    """Bind one transactional write op: optional outbox flush on success, then after-commit."""

    plan = reg.bind(key).bind_tx().set_route(tx_route)

    if kit.outbox is not None:
        plan = plan.on_success(
            OnSuccessStep(
                id=flush_step_id,
                factory=stored_file_outbox_flush_factory(kit.outbox),
            )
        )

    return plan.after_commit(
        OnSuccessStep(
            id=after_commit_step_id,
            factory=after_commit_factory(kit),
        )
    ).finish(deep=True)


# ....................... #


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

    reg = _bind_write_op(
        reg,
        key=ns.key(StoredFileKernelOp.UPLOAD),
        tx_route=tx_route,
        kit=kit,
        flush_step_id="stored_file_outbox_flush_upload",
        after_commit_step_id="stored_file_complete_upload",
        after_commit_factory=stored_file_complete_upload_after_commit_factory,
    )

    reg = _bind_write_op(
        reg,
        key=ns.key(StoredFileKernelOp.DELETE),
        tx_route=tx_route,
        kit=kit,
        flush_step_id="stored_file_outbox_flush_delete",
        after_commit_step_id="stored_file_purge_blob",
        after_commit_factory=stored_file_purge_blob_after_commit_factory,
    )

    return reg.freeze()
