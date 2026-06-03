"""Factories for soft-deletion document registries."""

from typing import Any, TypeVar

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
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
