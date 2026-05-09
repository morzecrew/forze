"""Unit tests for ``forze_tenancy.execution`` dependency wiring."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.application.execution import Deps
from forze_tenancy.execution import TenancyDepsModule


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
