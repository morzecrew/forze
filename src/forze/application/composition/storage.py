from enum import StrEnum
from typing import final

# ----------------------- #


@final
class StorageOperation(StrEnum):
    """Logical operation identifiers for storage usecases."""

    UPLOAD = "storage.upload"
    LIST = "storage.list"
    DOWNLOAD = "storage.download"
    DELETE = "storage.delete"
