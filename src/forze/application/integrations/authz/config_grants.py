"""Permissions granted by reviewed deployment configuration: break-glass and external recipients."""

from collections import Counter
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, final
from uuid import UUID

import attrs
from pydantic import BaseModel, ConfigDict, StringConstraints

from forze.application.contracts.authz import DerivedPermissions, PermissionProvider
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
class ConfigGrants(BaseModel):
    """Who holds each permission key a :class:`ConfigGrantsProvider` declares.

    A model the app mounts on its own settings root, never an environment the framework reads.
    The provider declares the keys in code; the deployment lists who holds each, and a key it
    lists nobody under, or omits, is granted to nobody. Principals are exact ids, never patterns.

    A key the provider declares may not also be granted through the permission catalog: the
    startup step refuses the overlap, and :class:`ConfigGrantsProvider` denies the key to every
    principal it does not list, so a catalog binding grants nothing whenever it was written.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    grants: Mapping[Annotated[str, StringConstraints(pattern=r"\S")], frozenset[UUID]] = {}
    """Permission key → the principals granted it. A blank key is refused."""


# ....................... #


def _snapshot(provider: "ConfigGrantsProvider") -> Mapping[str, frozenset[UUID]]:
    # Settings validate the mapping to a dict: read a copy, so nothing holding the settings can
    # add a principal after the keys were checked.
    return MappingProxyType(dict(provider.grants.grants))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigGrantsProvider:
    """A :class:`~forze.application.contracts.authz.PermissionProvider` over :class:`ConfigGrants`.

    Grants each key to the principals the configuration lists under it and denies it to every
    other, so a key the configuration owns can come from nowhere else. The keys are code and the
    principals configuration: a configuration naming a key outside :attr:`keys` is refused, and a
    declared key it omits is granted to nobody.
    """

    keys: frozenset[str]
    """Every key the configuration owns — declared in code, checked against the catalog like any
    provider's."""

    grants: ConfigGrants = attrs.field(factory=ConfigGrants)
    """Who holds each key, as the app's settings loaded it; read once, when the provider is
    built."""

    name: str = "config"
    """What a derived grant is attributed to."""

    table: Mapping[str, frozenset[UUID]] = attrs.field(
        init=False, default=attrs.Factory(_snapshot, takes_self=True)
    )
    """A read-only copy of :attr:`grants`, which :meth:`derive` reads."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # A set difference against a bare string would read its letters as keys.
        if not isinstance(self.keys, frozenset):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.configuration(
                f"ConfigGrantsProvider {self.name!r} declares its keys as a frozenset of "
                f"permission-key strings, not {type(self.keys).__name__}.",
                code="authz_provider_declaration",
            )

        if unknown := sorted(set(self.table) - self.keys):
            raise exc.configuration(
                f"Configuration grants name keys {unknown} that ConfigGrantsProvider "
                f"{self.name!r} does not declare. The keys are declared in code; a deployment "
                "lists only who holds them.",
                code="authz_provider_declaration",
            )

    # ....................... #

    async def derive(self, principal_id: UUID, ctx: "ExecutionContext") -> DerivedPermissions:
        granted = frozenset(key for key in self.keys if principal_id in self.table.get(key, ()))

        return DerivedPermissions(granted=granted, denied=self.keys - granted)


# ....................... #


def check_config_grants_exclusive(providers: Iterable[PermissionProvider]) -> None:
    """Refuse a key a :class:`ConfigGrantsProvider` owns that another provider declares too.

    The config provider denies its keys to every principal it does not list, and a denial
    outranks every grant, so another provider's grant of the same key would never count.

    :raises CoreException: ``configuration`` (``authz_provider_declaration``), naming the keys.
    """

    declared = tuple(providers)
    counts = Counter(key for provider in declared for key in provider.keys)
    shared = sorted(
        {
            key
            for provider in declared
            if isinstance(provider, ConfigGrantsProvider)
            for key in provider.keys
            if counts[key] > 1
        }
    )

    if shared:
        raise exc.configuration(
            f"Permission keys {shared} are owned by configuration grants and declared by another "
            "provider too. The configuration denies them to everyone it does not list, so the "
            "other provider's grants would never count; declare each in one place.",
            code="authz_provider_declaration",
        )
