from enum import StrEnum

# ----------------------- #


class StorageOperation(StrEnum):
    """Logical operation identifiers for storage usecases."""

    UPLOAD = "upload"
    LIST = "list"
    DOWNLOAD = "download"
    DELETE = "delete"
