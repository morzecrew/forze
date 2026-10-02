"""Bind a stored-file registry's write operations to their transaction and after-commit stages."""

from __future__ import annotations

from forze.application.contracts.execution import OnSuccessStep
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.primitives import StrKeyNamespace
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
    after_commit: OnSuccessStep | None,
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

    if after_commit is not None:
        plan = plan.after_commit(after_commit)

    return plan.finish(deep=True)


# ....................... #


def _delete_after_commit(kit: StoredFileKitSpec, *, purge_on_delete: bool) -> OnSuccessStep | None:
    """The after-commit stage ``delete`` gets: purge the blob, only unindex it, or none."""

    if purge_on_delete:
        return OnSuccessStep(
            id="stored_file_purge_blob",
            factory=stored_file_purge_blob_after_commit_factory(kit),
        )

    if kit.search_spec is not None:
        return OnSuccessStep(
            id="stored_file_unindex",
            factory=stored_file_purge_blob_after_commit_factory(kit, keep_blob=True),
        )

    return None


# ....................... #


def bind_stored_file_writes(
    kit: StoredFileKitSpec,
    *,
    tx_route: str = "default",
    registry: OperationRegistry | None = None,
    ns: StrKeyNamespace | None = None,
    purge_on_delete: bool = True,
) -> OperationRegistry:
    """Bind a stored-file registry's write operations, leaving it unfrozen.

    Write operations (``upload``, ``delete``) run in a transaction. Outbox rows
    flush on tx success; blob upload and purge run in ``after_commit`` hooks.
    The registry stays open, so the app can layer its own guards (authn, authz)
    on the stored-file operations before it freezes. Pass the *ns* a given *registry* was
    built with (:func:`build_stored_file_registry`'s ``ns``); the default is the kit
    document's namespace.

    With ``purge_on_delete=False``, ``delete`` soft-deletes the row and leaves its object in
    storage; a search entry is still dropped, so the file stops appearing in search.
    """

    ns = ns or kit.document.default_namespace
    reg = registry if registry is not None else build_stored_file_registry(kit, ns=ns)

    reg = _bind_write_op(
        reg,
        key=ns.key(StoredFileKernelOp.UPLOAD),
        tx_route=tx_route,
        kit=kit,
        flush_step_id="stored_file_outbox_flush_upload",
        after_commit=OnSuccessStep(
            id="stored_file_complete_upload",
            factory=stored_file_complete_upload_after_commit_factory(kit),
        ),
    )

    reg = _bind_write_op(
        reg,
        key=ns.key(StoredFileKernelOp.DELETE),
        tx_route=tx_route,
        kit=kit,
        flush_step_id="stored_file_outbox_flush_delete",
        after_commit=_delete_after_commit(kit, purge_on_delete=purge_on_delete),
    )

    return reg


# ....................... #


def freeze_stored_file_registry(
    kit: StoredFileKitSpec,
    *,
    tx_route: str = "default",
    registry: OperationRegistry | None = None,
    ns: StrKeyNamespace | None = None,
    purge_on_delete: bool = True,
) -> FrozenOperationRegistry:
    """:func:`bind_stored_file_writes`, frozen — for an app that adds nothing to the
    stored-file operations."""

    return bind_stored_file_writes(
        kit,
        tx_route=tx_route,
        registry=registry,
        ns=ns,
        purge_on_delete=purge_on_delete,
    ).freeze()
