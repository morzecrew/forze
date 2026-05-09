"""Tenancy dependency module for the application kernel."""

from collections.abc import Collection
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.application.execution import Deps, DepsModule

from .deps import ConfigurableTenantManagement, ConfigurableTenantResolver

# ----------------------- #


def _normalize_route_set[K: str | StrEnum](
    routes: Collection[K] | None,
) -> frozenset[K]:
    return frozenset(routes) if routes else frozenset()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenancyDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers tenant resolver and management factories (document-backed defaults)."""

    tenant_resolver: Collection[K] | None = attrs.field(default=None)
    """Route names for :class:`~forze.application.contracts.tenancy.TenantResolverDepKey`."""

    tenant_management: Collection[K] | None = attrs.field(default=None)
    """Route names for :class:`~forze.application.contracts.tenancy.TenantManagementDepKey`."""

    verify_tenant_active: bool = attrs.field(default=True)
    """Forwarded to :class:`~forze_tenancy.execution.deps.deps.ConfigurableTenantResolver`."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        tr = _normalize_route_set(self.tenant_resolver)
        tm = _normalize_route_set(self.tenant_management)

        if not tr and not tm:
            return Deps[K]()

        merged: Deps[K] = Deps[K]()

        if tr:
            factory = ConfigurableTenantResolver(
                verify_tenant_active=self.verify_tenant_active,
            )

            merged = merged.merge(
                Deps[K].routed(
                    {
                        TenantResolverDepKey: {name: factory for name in tr},
                    },
                ),
            )

        if tm:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        TenantManagementDepKey: {
                            name: ConfigurableTenantManagement() for name in tm
                        },
                    },
                ),
            )

        return merged
