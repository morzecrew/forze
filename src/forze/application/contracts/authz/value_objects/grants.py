"""Effective grant snapshots for a subject."""

from datetime import datetime
from typing import final

import attrs

from forze.base.primitives import utcnow

from .catalog import PermissionRef, RoleRef

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DerivedPermissionRef:
    """A permission a provider derived for one decision — a key and who said so.

    Never a catalog row: a :class:`PermissionRef` promises one exists, and a derived permission
    comes from documents or configuration instead, so it carries the provider's name where a
    catalog grant carries an id.
    """

    permission_key: str
    """The permission key."""

    provider: str
    """The name of the provider that derived it."""

    denied: bool = False
    """Whether the provider denied the key rather than granting it. A denial outranks every
    other source, catalog bindings included."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class EffectiveGrants:
    """Effective grants for a principal."""

    roles: frozenset[RoleRef] = attrs.field(factory=frozenset)
    permissions: frozenset[PermissionRef] = attrs.field(factory=frozenset)
    """Administered grants: catalog permissions reached through bindings, roles and groups."""

    derived: frozenset[DerivedPermissionRef] = attrs.field(factory=frozenset)
    """Grants and denials derived per decision by permission providers, kept apart from
    :attr:`permissions` so an administered grant can be told from a derived one."""

    resolved_at: datetime = attrs.field(factory=utcnow)

    # ....................... #

    @property
    def denied_keys(self) -> frozenset[str]:
        """Keys a provider denied — refused whatever else grants them."""

        return frozenset(ref.permission_key for ref in self.derived if ref.denied)

    @property
    def granted_keys(self) -> frozenset[str]:
        """Every key granted by the catalog or a provider, minus the denied ones."""

        granted = {ref.permission_key for ref in self.permissions} | {
            ref.permission_key for ref in self.derived if not ref.denied
        }

        return frozenset(granted) - self.denied_keys
