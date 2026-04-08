from typing import NotRequired, TypedDict

# ----------------------- #


class TemporalWorkflowConfig(TypedDict):
    """Configuration for a Temporal workflow."""

    queue: str
    """Temporal task queue name."""

    tenant_aware: NotRequired[bool]
    """Whether the workflow is tenant-aware."""
