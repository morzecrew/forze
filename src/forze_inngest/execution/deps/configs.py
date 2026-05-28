from typing import TypedDict

# ----------------------- #


class InngestEventConfig(TypedDict, total=False):
    """Configuration for an Inngest-backed durable function event command."""

    include_execution_context: bool
    """When ``True`` (default), embed invocation identity in event payloads."""
