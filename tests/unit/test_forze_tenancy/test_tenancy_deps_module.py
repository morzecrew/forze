"""Unit tests for ``forze_identity.tenancy.execution`` dependency wiring."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.tenancy.execution import TenancyDepsModule


class TestTenancyDepsModule:
    def test_empty_module(self) -> None:
        deps = TenancyDepsModule()()

        assert isinstance(deps, Deps)

    def test_registers_resolver_route(self) -> None:
        deps = TenancyDepsModule(tenant_resolver={"main"})()

        assert deps.exists(TenantResolverDepKey, route="main")

    def test_registers_management_route(self) -> None:
        deps = TenancyDepsModule(tenant_management={"admin"})()

        assert deps.exists(TenantManagementDepKey, route="admin")

    def test_registers_both_routes(self) -> None:
        deps = TenancyDepsModule(
            tenant_resolver={"r"},
            tenant_management={"m"},
        )()

        assert deps.exists(TenantResolverDepKey, route="r")
        assert deps.exists(TenantManagementDepKey, route="m")

    def test_a_resolver_override_wins_its_declared_route(self) -> None:
        def override(ctx: object) -> None:
            _ = ctx

        deps = TenancyDepsModule(
            tenant_resolver={"main"},
            tenant_resolvers={"main": override},
        )()

        assert deps.routed_deps[TenantResolverDepKey]["main"] is override

    @pytest.mark.parametrize("declared", [None, {"other"}])
    def test_a_resolver_override_for_an_undeclared_route_is_refused(
        self, declared: set[str] | None
    ) -> None:
        # It used to register nothing, failing the first request with "not registered".
        def override(ctx: object) -> None:
            _ = ctx

        with pytest.raises(CoreException, match="'main'") as excinfo:
            TenancyDepsModule(tenant_resolver=declared, tenant_resolvers={"main": override})

        assert excinfo.value.kind is ExceptionKind.CONFIGURATION
