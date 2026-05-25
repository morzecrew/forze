from typing import NotRequired, TypedDict, final

# ----------------------- #


@final
class GCSStorageConfig(TypedDict):
    """Configuration for a GCS-backed storage route."""

    bucket: str
    """GCS bucket name."""

    tenant_aware: NotRequired[bool]
    """Whether keys are prefixed with the active tenant id."""
