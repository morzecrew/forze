"""Document operation catalog for transport attach (protocol-agnostic)."""

from dataclasses import dataclass
from typing import Any, Final

from forze.application.composition.document.operations import DocumentKernelOp
from forze.application.composition.document.value_objects import DocumentDTOs
from forze.application.contracts.document import DocumentSpec
from forze.base.primitives import StrKey
from forze_contrib.soft_deletion.composition import SoftDeletionKernelOp

# ----------------------- #
#! Super useless isn't it?


@dataclass(frozen=True, slots=True)
class DocumentOperationEntry:
    """One attachable document operation."""

    enable_name: str
    facade_attr: str | None
    kernel_op: StrKey
    uses_registry: bool = False


# ....................... #


class DocumentPreset:
    """Named sets of document operations for ``enable=``."""

    READ: Final = ("get", "list")
    READ_WITH_RAW: Final = ("get", "list", "raw_list")
    CRUD: Final = ("get", "list", "create", "update", "kill")
    FULL: Final = (
        "get",
        "list",
        "raw_list",
        "list_cursor",
        "raw_list_cursor",
        "aggregated_list",
        "create",
        "update",
        "kill",
        "delete",
        "restore",
    )


DOCUMENT_OPERATIONS: dict[str, DocumentOperationEntry] = {
    "get": DocumentOperationEntry("get", "get", DocumentKernelOp.GET),
    "list": DocumentOperationEntry("list", "list", DocumentKernelOp.LIST),
    "raw_list": DocumentOperationEntry(
        "raw_list", "raw_list", DocumentKernelOp.RAW_LIST
    ),
    "list_cursor": DocumentOperationEntry(
        "list_cursor",
        "list_cursor",
        DocumentKernelOp.LIST_CURSOR,
    ),
    "raw_list_cursor": DocumentOperationEntry(
        "raw_list_cursor",
        "raw_list_cursor",
        DocumentKernelOp.RAW_LIST_CURSOR,
    ),
    "aggregated_list": DocumentOperationEntry(
        "aggregated_list",
        "agg_list",
        DocumentKernelOp.AGG_LIST,
    ),
    "create": DocumentOperationEntry("create", "create", DocumentKernelOp.CREATE),
    "update": DocumentOperationEntry("update", "update", DocumentKernelOp.UPDATE),
    "kill": DocumentOperationEntry("kill", "kill", DocumentKernelOp.KILL),
    "delete": DocumentOperationEntry(
        "delete",
        None,
        SoftDeletionKernelOp.DELETE,
        uses_registry=True,
    ),
    "restore": DocumentOperationEntry(
        "restore",
        None,
        SoftDeletionKernelOp.RESTORE,
        uses_registry=True,
    ),
}


def document_capability_allows(
    name: str,
    document: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
) -> bool:
    """Return whether *name* is supported for *document* / *dtos*."""

    if name == "create":
        return document.write is not None and dtos.create is not None
    if name in ("update", "kill", "delete", "restore"):
        if document.write is None:
            return False
    if name == "update":
        return bool(dtos.update) and document.supports_update()
    if name in ("delete", "restore"):
        return document.supports_soft_delete()
    if name == "kill":
        return document.write is not None
    return True
