"""Exposure policy: which catalog operations become MCP tools.

The read-only MVP exposes only ``QUERY`` operations; ``include_writes`` opts into command
(mutating) operations as well. *Which* operations a surface exposes is an interface
decision and lives here, not in the engine.
"""

from typing import Mapping

from forze.application.execution.operations import OperationCatalogEntry
from forze.base.primitives import StrKey

# ----------------------- #


def exposed_operations(
    catalog: Mapping[StrKey, OperationCatalogEntry],
    *,
    include_writes: bool = False,
) -> dict[str, StrKey]:
    """Map exposed tool name → operation key for the exposed slice of the catalog."""

    return {
        str(entry.op): entry.op
        for entry in catalog.values()
        if include_writes or entry.is_read_only
    }
