"""Exposure policy: which catalog operations become MCP tools.

The read-only MVP exposes only ``QUERY`` operations; ``include_writes`` opts into command
(mutating) operations as well, and ``operations`` narrows the surface to a named set. The
policy itself is shared by every generated surface
(:func:`~forze.application.execution.ingress.select_operations`).
"""

from collections.abc import Iterable

from forze.application.execution.ingress import select_operations
from forze.application.execution.operations import OperationCatalogEntry
from forze.base.primitives import StrKey, StrKeyMapping

# ----------------------- #


def exposed_operations(
    catalog: StrKeyMapping[OperationCatalogEntry],
    *,
    include_writes: bool = False,
    operations: Iterable[StrKey] | None = None,
) -> StrKeyMapping[StrKey]:
    """Map exposed tool name → operation key for the exposed slice of the catalog.

    :param catalog: The registry's operation catalog.
    :param include_writes: Expose command operations as well as ``QUERY`` ones.
    :param operations: Expose only these operations, by key; a single key is one operation,
        not a sequence of characters. A name the catalog does not
        have, or a command operation without *include_writes*, is refused rather than
        dropped: an allowlist is reviewed like a grant, and a typo that silently exposed
        less (or a command that silently stayed hidden) would mislead whoever reviewed it.
    :raises CoreException: When *operations* is empty, names an unknown operation, or names
        a command operation while *include_writes* is off.
    """

    return select_operations(
        catalog,
        include_writes=include_writes,
        operations=operations,
        surface="MCP tool",
    )
