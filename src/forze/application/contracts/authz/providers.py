"""Permission providers: grants and denials derived from state or configuration per decision."""

from typing import TYPE_CHECKING, Protocol, final, runtime_checkable
from uuid import UUID

import attrs

from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def _keys(value: frozenset[str] | set[str] | tuple[str, ...] | list[str]) -> frozenset[str]:
    # A bare string would become the set of its letters, and a denial of "ledger.write" a
    # denial of "l", "e", ... — never of the key.
    # Materialised once: checking a one-shot iterator's items would otherwise use them up, and
    # a denial built from a generator would keep nothing.
    items = () if isinstance(value, (str, bytes)) else tuple(value)

    if isinstance(value, (str, bytes)) or not all(isinstance(key, str) for key in items):
        raise exc.validation(
            "Derived permission keys are a collection of permission-key strings, "
            f"not {type(value).__name__}."
        )

    return frozenset(items)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DerivedPermissions:
    """What one provider derived for one principal."""

    granted: frozenset[str] = attrs.field(factory=frozenset, converter=_keys)
    """Keys granted, as if the principal held a binding for each."""

    denied: frozenset[str] = attrs.field(factory=frozenset, converter=_keys)
    """Keys refused whatever else grants them — catalog bindings, roles and groups included.
    Marking an employee inactive is then a complete authorization action."""


@runtime_checkable
class PermissionProvider(Protocol):  # pragma: no cover
    """Derives permissions per decision from documents or reviewed configuration.

    Registered on the identity plane's authz kernel, run in declaration order after the catalog
    grants are resolved; the result is the union of every provider's grants minus the union of
    their denials. A provider reads and never writes. One that raises, returns something other
    than :class:`DerivedPermissions`, or names a key outside :attr:`keys` denies every key it
    declares — an outage or a typo must not become an authorization bypass.
    """

    name: str
    """What a derived grant is attributed to."""

    keys: frozenset[str]
    """Every key this provider may grant or deny. Checked against the permission catalog when
    the runtime starts; a result naming a key outside it is treated as a failure."""

    async def derive(self, principal_id: UUID, ctx: "ExecutionContext") -> DerivedPermissions:
        """Derive grants and denials for *principal_id*."""
        ...
