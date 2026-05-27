from enum import StrEnum
from typing import Any, TypeVar, final

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace

from .handlers import DeleteDocument, RestoreDocument
from .models import DocWithSoftDeletion, UpdateCmdWithSoftDeletion

# ----------------------- #

D = TypeVar("D", bound=DocWithSoftDeletion)
U = TypeVar("U", bound=UpdateCmdWithSoftDeletion)

# ....................... #


@final
class SoftDeletionKernelOp(StrEnum):
    """Kernel segments (suffix only) for soft-deletion document usecase operation keys."""

    DELETE = "delete"
    """Soft-delete a document."""

    RESTORE = "restore"
    """Restore a soft-deleted document."""


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
        return OperationRegistry(
            handlers={
                ns.key(SoftDeletionKernelOp.DELETE): lambda ctx: DeleteDocument(
                    doc=ctx.doc.command(spec),
                ),
                ns.key(SoftDeletionKernelOp.RESTORE): lambda ctx: RestoreDocument(
                    doc=ctx.doc.command(spec),
                ),
            },
        )

    return OperationRegistry()
