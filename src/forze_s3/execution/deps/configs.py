from typing import NotRequired, TypedDict, final

# ----------------------- #


@final
class S3StorageConfig(TypedDict):
    """Configuration for the S3 storage."""

    bucket: str
    """The name of the bucket to use for the storage."""

    tenant_aware: NotRequired[bool]
    """Whether the storage is tenant-aware."""
