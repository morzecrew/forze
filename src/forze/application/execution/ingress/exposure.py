"""Exposure policy: which catalog operations a generated surface exposes.

Queries are exposed by default; ``include_writes`` opts into command (mutating) operations
as well, and ``operations`` narrows the surface to a named set. Per-operation query
policies narrow it further, to the fields a surface advertises and accepts. An operation
whose result is a byte stream is never exposed: these surfaces answer in JSON. *Which*
operations a surface exposes is an interface decision and lives at the ingress, not in the
engine.
"""

from collections.abc import Iterable
from typing import Any, Final

from forze.application.contracts.querying import QueryFieldGuard, QueryFieldPolicy
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
        not a sequence of characters. A name the catalog does not have, a command operation
        without *include_writes*, or a streaming operation is refused rather than dropped:
        an allowlist is reviewed like a grant, and a typo that silently exposed less (or a
        command that silently stayed hidden) would mislead whoever reviewed it. Without
        an allowlist, streaming operations are left out.
    :param surface: What the surface exposes operations as (``"MCP tool"``), named in a
        refusal.
    :raises CoreException: When *operations* is empty, names an unknown operation, names
        a command operation while *include_writes* is off, or names an operation whose
        result is a byte stream (``OperationDescriptor.streams``).
    """

    if operations is None:
        return {
            str(entry.op): entry.op
            for entry in catalog.values()
            if (include_writes or entry.is_read_only) and not _streams(entry)
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

    if streaming := sorted(str(op) for op in selected if _streams(catalog[op])):
        raise exc.configuration(
            f"Streaming operations in the {surface} allowlist return a byte stream, which "
            f"this surface cannot carry; serve them from a streaming route: "
            f"{', '.join(streaming)}"
        )

    return {str(catalog[op].op): catalog[op].op for op in selected}


# ....................... #


def _streams(entry: OperationCatalogEntry) -> bool:
    return entry.descriptor is not None and entry.descriptor.streams


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


# ....................... #

_POLICY_ARGUMENTS: Final = {
    "filterable": "filters",
    "sortable": "sorts",
    "aggregatable": "aggregates",
}
"""Each :class:`QueryFieldPolicy` axis, and the input field its guard reads the call from."""


def query_policy_guards(
    catalog: StrKeyMapping[OperationCatalogEntry],
    ops: Iterable[StrKey],
    policies: StrKeyMapping[QueryFieldPolicy] | None,
    *,
    surface: str,
) -> dict[str, QueryFieldGuard]:
    """Check narrower per-operation query policies, and build the guard enforcing each.

    A policy narrows what a surface advertises for an operation and what it accepts, never
    what the operation itself allows: each allow-set it declares must be a subset of the
    operation's own (its :class:`~forze.application.contracts.querying.QueryDiscovery`), and
    an axis it leaves ``None`` keeps the operation's. A policy is refused rather than
    clipped, as an allowlist is reviewed like a grant.

    :param catalog: The registry's operation catalog.
    :param ops: The operations the surface exposes.
    :param policies: Operation key → the narrower policy, or ``None`` for none.
    :param surface: What the surface exposes operations as (``"MCP tool"``), named in a
        refusal.
    :returns: Operation key → the guard to apply to that operation's calls.
    :raises CoreException: ``configuration`` when a key is not one of *ops*, names an
        operation that takes no filter, widens an axis beyond the operation's own
        allow-set, or narrows an axis whose input field the operation does not have.
    """

    if not policies:
        return {}

    exposed = {str(op) for op in ops}
    unknown = sorted(str(op) for op in policies if str(op) not in exposed)

    if unknown:
        raise exc.configuration(
            f"Query policies name operation(s) no {surface} exposes: {', '.join(unknown)}",
            code="query_policy_unknown_operation",
            details={"ops": unknown},
        )

    guards: dict[str, QueryFieldGuard] = {}

    for op, policy in policies.items():
        descriptor = catalog[op].descriptor
        discovery = descriptor.query_discovery if descriptor is not None else None

        if descriptor is None or discovery is None:
            raise exc.configuration(
                f"{str(op)!r} takes no filter, so it has no query policy to narrow",
                code="query_policy_not_queryable",
                details={"op": str(op)},
            )

        own = {
            "filterable": {support.field for support in discovery.filterable},
            "sortable": set(discovery.sortable),
            "aggregatable": set(discovery.aggregatable),
        }
        fields = descriptor.input_type.model_fields if descriptor.input_type else {}

        for axis, argument in _POLICY_ARGUMENTS.items():
            narrowed: frozenset[str] | None = getattr(policy, axis)

            if narrowed is None:
                continue

            if wider := sorted(narrowed - own[axis]):
                raise exc.configuration(
                    f"The query policy for {str(op)!r} widens {axis} beyond the operation's "
                    f"own allow-set: {', '.join(wider)}",
                    code="query_policy_widened",
                    details={"op": str(op), "axis": axis, "fields": wider},
                )

            # The guard reads the call's argument by this name: one it cannot see, it
            # cannot refuse, so the operation would accept more than the surface states.
            if argument not in fields:
                raise exc.configuration(
                    f"The query policy for {str(op)!r} narrows {axis}, but the operation's "
                    f"input has no {argument!r} for it to check",
                    code="query_policy_unenforceable",
                    details={"op": str(op), "axis": axis},
                )

        # shortcut: parses under the default filter limits, as the descriptor does not carry
        # the spec's; carry them on QueryDiscovery if a spec that raises them needs this.
        guards[str(op)] = QueryFieldGuard(policy=policy, spec_name=str(op))

    return guards


# ....................... #


def check_query_policy(guard: QueryFieldGuard | None, args: Any) -> None:
    """Refuse a call whose validated *args* reach outside *guard*'s narrower policy.

    :param guard: From :func:`query_policy_guards`, or ``None`` when the operation has no
        narrower policy.
    :param args: The operation's validated input DTO.
    :raises CoreException: ``precondition`` (``field_not_filterable``,
        ``field_not_sortable`` or ``field_not_aggregatable``) on the first field outside it.
    """

    if guard is None:
        return

    # An axis the guard checks was refused at build unless its field exists, so a missing
    # field here is only ever an axis the guard skips.
    guard.check(**{name: getattr(args, name, None) for name in _POLICY_ARGUMENTS.values()})
