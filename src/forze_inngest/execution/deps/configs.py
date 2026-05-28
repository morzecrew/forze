"""Inngest durable function execution configs."""

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestEventConfig:
    """Configuration for an Inngest-backed durable function event command."""

    include_execution_context: bool = True
    """When True, embed invocation identity in event payloads."""
