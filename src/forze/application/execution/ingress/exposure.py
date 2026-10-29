"""Exposure policy: which catalog operations a generated surface exposes.

Queries are exposed by default; ``include_writes`` opts into command (mutating) operations
as well, and ``operations`` narrows the surface to a named set. *Which* operations a surface
exposes is an interface decision and lives at the ingress, not in the engine.
"""

from collections.abc import Iterable

from forze.base.exceptions import exc
from forze.base.primitives import StrKey, StrKeyMapping

from ..operations import OperationCatalogEntry

# ----------------------- #


def select_operations(
    catalog: StrKeyMapping[OperationCatalogEntry],
    *,
    include_writes: bool,
    operations: Iterable[StrKey] | None,
    surface: str,
) -> StrKeyMapping[StrKey]:
    """Map exposed name → operation key for the exposed slice of the catalog.

    :param catalog: The registry's operation catalog.
    :param include_writes: Expose command operations as well as ``QUERY`` ones.
    :param operations: Expose only these operations, by key; a single key is one operation,
        not a sequence of characters. A name the catalog does not have, or a command
        operation without *include_writes*, is refused rather than dropped: an allowlist is
        reviewed like a grant, and a typo that silently exposed less (or a command that
        silently stayed hidden) would mislead whoever reviewed it.
    :param surface: What the surface exposes operations as (``"MCP tool"``), named in a
        refusal.
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
        raise exc.configuration(f"An {surface} allowlist must name at least one operation")

    unknown = sorted(str(op) for op in selected if catalog.get(op) is None)

    if unknown:
        raise exc.configuration(
            f"Unknown operations in the {surface} allowlist: {', '.join(unknown)}"
        )

    commands = sorted(str(op) for op in selected if not catalog[op].is_read_only)

    if commands and not include_writes:
        raise exc.configuration(
            f"Command operations in the {surface} allowlist need include_writes=True: "
            f"{', '.join(commands)}"
        )

    return {str(catalog[op].op): catalog[op].op for op in selected}


# ....................... #


def refuse_sensitive(
    catalog: StrKeyMapping[OperationCatalogEntry],
    ops: Iterable[StrKey],
    *,
    surface: str,
) -> None:
    """Refuse *ops* when one is sensitive (its result carries credential or secret material).

    Called before anything is exposed, so a credential-bearing read model never leaks
    through a generated surface.

    :param catalog: The registry's operation catalog; every op in *ops* must be in it.
    :param ops: The operations about to be exposed.
    :param surface: What the surface exposes operations as (``"MCP tool"``), named in the
        refusal.
    :raises CoreException: When an operation's spec is marked ``sensitive=True``.
    """

    for op in ops:
        descriptor = catalog[op].descriptor

        if descriptor is not None and descriptor.sensitive:
            raise exc.configuration(
                f"Refusing to register {surface}s: operation '{op}' is sensitive (its "
                "result carries credential or secret material, which a generated surface "
                "must not expose). Leave it out with an operations= allowlist, or serve "
                "it from your own guarded route"
            )
