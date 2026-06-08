"""Factories for soft-deletion document registries."""

from typing import Any, TypeVar

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.document.dto import DocumentIdRevDTO
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)

from .handlers import DeleteDocument, RestoreDocument
from .operations import SoftDeletionKernelOp

# ----------------------- #

D = TypeVar("D", bound=DocWithSoftDeletion)
U = TypeVar("U", bound=UpdateCmdWithSoftDeletion)

# ....................... #


def build_soft_deletion_registry(
    spec: DocumentSpec[Any, D, Any, U],
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build soft-deletion document operation registry.

    :param spec: Document specification.
    :param ns: Optional namespace.
    :returns: Operation registry with all supported operations.
    """

    ns = ns or spec.default_namespace

    if spec.write is not None and spec.supports_update():
        reg = OperationRegistry(
            handlers={
                ns.key(SoftDeletionKernelOp.DELETE): lambda ctx: DeleteDocument(
                    doc=ctx.doc.command(spec),
                ),
                ns.key(SoftDeletionKernelOp.RESTORE): lambda ctx: RestoreDocument(
                    doc=ctx.doc.command(spec),
                ),
            },
        )

        # Both operations write (they update the soft-deletion flag) — kept COMMAND.
        return reg.set_descriptors(
            {
                SoftDeletionKernelOp.DELETE: OperationDescriptor(
                    input_type=DocumentIdRevDTO,
                    output_type=spec.read,
                    description="Soft-delete a document by primary key.",
                ),
                SoftDeletionKernelOp.RESTORE: OperationDescriptor(
                    input_type=DocumentIdRevDTO,
                    output_type=spec.read,
                    description="Restore a soft-deleted document by primary key.",
                ),
            },
            namespace=ns,
        )

    return OperationRegistry()
