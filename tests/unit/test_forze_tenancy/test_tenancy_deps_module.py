"""Unit tests for ``forze_identity.tenancy.execution`` dependency wiring."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.application.execution import Deps
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.tenancy.execution import TenancyDepsModule


class TestTenancyDepsModule:
    def test_empty_module(self) -> None:
        deps = TenancyDepsModule()()

        assert isinstance(deps, Deps)

    def test_registers_resolver_route(self) -> None:
        deps = TenancyDepsModule(tenant_resolver={"main"})()

        assert deps.exists(TenantResolverDepKey, route="main")

    def test_registers_management_route(self) -> None:
        deps = TenancyDepsModule(tenant_management={"admin"}, detach_keeps_roles=True)()

        assert deps.exists(TenantManagementDepKey, route="admin")

    def test_registers_both_routes(self) -> None:
        deps = TenancyDepsModule(
            tenant_resolver={"r"},
            tenant_management={"m"},
            detach_keeps_roles=True,
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


class TestWhatDetachDoesWithRoles:
    def test_tenant_management_must_say_what_detach_does_with_roles(self) -> None:
        # A default would silently keep a former member's roles in the tenant it left.
        with pytest.raises(CoreException) as excinfo:
            TenancyDepsModule(tenant_management={"m"})

        assert excinfo.value.code == "tenancy_detach_roles_unset"
        assert "authz_route=..." in excinfo.value.summary
        assert "detach_keeps_roles=True" in excinfo.value.summary

    def test_naming_both_is_refused(self) -> None:
        with pytest.raises(CoreException) as excinfo:
            TenancyDepsModule(tenant_management={"m"}, authz_route="a", detach_keeps_roles=True)

        assert excinfo.value.code == "tenancy_detach_roles_ambiguous"

    def test_a_resolver_alone_needs_neither(self) -> None:
        TenancyDepsModule(tenant_resolver={"r"})

    @pytest.mark.parametrize(
        ("kwargs", "route"),
        [({"authz_route": "a"}, "a"), ({"detach_keeps_roles": True}, None)],
        ids=["authz-route", "keeps-roles"],
    )
    def test_the_management_factory_gets_the_route(
        self, kwargs: dict[str, object], route: str | None
    ) -> None:
        deps = TenancyDepsModule(tenant_management={"m"}, **kwargs)()  # type: ignore[arg-type]

        assert deps.routed_deps[TenantManagementDepKey]["m"].authz_route == route

    def test_an_authz_route_with_no_role_assignment_refuses_the_freeze(self) -> None:
        deps = TenancyDepsModule(tenant_management={"m"}, authz_route="a")()

        with pytest.raises(CoreException) as excinfo:
            DepsRegistry.from_deps(deps).freeze()

        assert excinfo.value.code == "dependency_requirement_unbound"
        assert "resolves 'authz_role_assignment' on route 'a'" in excinfo.value.summary

    def test_the_authz_role_assignment_satisfies_it(self) -> None:
        tenancy = TenancyDepsModule(tenant_management={"m"}, authz_route="a")()
        authz = AuthzDepsModule(kernel=AuthzKernelConfig(), role_assignment={"a"})()

        DepsRegistry.from_deps(tenancy, authz).freeze()
