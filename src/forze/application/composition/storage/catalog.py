"""Storage operation catalog for transport attach (protocol-agnostic)."""

from dataclasses import dataclass
from typing import Final

from forze.application.composition.storage.operations import StorageKernelOp
from forze.base.primitives import StrKey

# ----------------------- #
#! Super useless isn't it?


@dataclass(frozen=True, slots=True)
class StorageOperationEntry:
    """One attachable storage operation."""

    enable_name: str
    facade_attr: str
    kernel_op: StrKey


class StoragePreset:
    """Named sets of storage operations for ``enable=``."""

    ALL: Final = ("list_", "upload", "download", "delete")


STORAGE_OPERATIONS: dict[str, StorageOperationEntry] = {
    "list_": StorageOperationEntry("list_", "list", StorageKernelOp.LIST),
    "upload": StorageOperationEntry("upload", "upload", StorageKernelOp.UPLOAD),
    "download": StorageOperationEntry("download", "download", StorageKernelOp.DOWNLOAD),
    "delete": StorageOperationEntry("delete", "delete", StorageKernelOp.DELETE),
}
