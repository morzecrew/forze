from enum import StrEnum

# ----------------------- #


class StorageOperation(StrEnum):
    UPLOAD = "upload"
    LIST = "list"
    DOWNLOAD = "download"
    DELETE = "delete"
