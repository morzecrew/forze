"""Exposure policy: which catalog operations become MCP tools.

The read-only MVP exposes only ``QUERY`` operations; ``include_writes`` opts into command
(mutating) operations as well, and ``operations`` narrows the surface to a named set. *Which*
operations a surface exposes is an interface decision and lives here, not in the engine.
"""

from collections.abc import Iterable

from forze.application.execution.operations import OperationCatalogEntry
from forze.base.exceptions import exc
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

    if operations is None:
        return {
            str(entry.op): entry.op
            for entry in catalog.values()
            if include_writes or entry.is_read_only
        }

    selected = (operations,) if isinstance(operations, str) else tuple(operations)

    if not selected:
        raise exc.configuration("An MCP tool allowlist must name at least one operation")

    unknown = sorted(str(op) for op in selected if catalog.get(op) is None)

    if unknown:
        raise exc.configuration(
            f"Unknown operations in the MCP tool allowlist: {', '.join(unknown)}"
        )

    commands = sorted(str(op) for op in selected if not catalog[op].is_read_only)

    if commands and not include_writes:
        raise exc.configuration(
            f"Command operations in the MCP tool allowlist need include_writes=True: "
            f"{', '.join(commands)}"
        )

    return {str(catalog[op].op): catalog[op].op for op in selected}
