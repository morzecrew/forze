"""Unit tests for ``forze_authz.execution`` dependency wiring."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from forze.application.contracts.authz import AuthzDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze_authz.execution import (
    AuthzDepsModule,
    AuthzKernelConfig,
    ConfigurableAuthz,
    build_authz_shared_services,
)


class TestAuthzDepsModule:
    def test_empty_module(self) -> None:
        deps = AuthzDepsModule()()

        assert isinstance(deps, Deps)

    def test_requires_kernel_when_routes_registered(self) -> None:
        with pytest.raises(CoreError, match="kernel"):
            AuthzDepsModule(authz={"main"})()

    def test_registers_authz_route_with_kernel(self) -> None:
        deps = AuthzDepsModule(kernel=AuthzKernelConfig(), authz={"main"})()

        assert deps.exists(AuthzDepKey, route="main")


class TestAuthzSharedServices:
    def test_build_policy_service(self) -> None:
        shared = build_authz_shared_services(AuthzKernelConfig())

        assert shared.policy is not None


class TestConfigurableAuthzFactory:
    def test_builds_adapter(self) -> None:
        from unittest.mock import MagicMock

        from forze.application.contracts.document import (
            DocumentCommandDepKey,
            DocumentQueryDepKey,
        )
        from forze.application.contracts.authz import AuthzSpec
        from forze.application.execution import Deps
        from forze_authz.application.constants import AuthzResourceName

        def factory(ctx: object, spec: object) -> MagicMock:
            port = MagicMock()
            port.spec = spec

            return port

        doc_deps = Deps.routed(
            {
                DocumentQueryDepKey: {
                    AuthzResourceName.POLICY_PRINCIPALS: factory,
                    AuthzResourceName.PERMISSIONS: factory,
                    AuthzResourceName.ROLES: factory,
                    AuthzResourceName.GROUPS: factory,
                    AuthzResourceName.ROLE_PERMISSION_BINDINGS: factory,
                    AuthzResourceName.PRINCIPAL_ROLE_BINDINGS: factory,
                    AuthzResourceName.PRINCIPAL_PERMISSION_BINDINGS: factory,
                    AuthzResourceName.GROUP_PRINCIPAL_BINDINGS: factory,
                    AuthzResourceName.GROUP_ROLE_BINDINGS: factory,
                    AuthzResourceName.GROUP_PERMISSION_BINDINGS: factory,
                },
                DocumentCommandDepKey: {
                    AuthzResourceName.POLICY_PRINCIPALS: factory,
                },
            },
        )

        ctx = ExecutionContext(deps=doc_deps)

        shared = build_authz_shared_services(AuthzKernelConfig())

        port = ConfigurableAuthz(shared=shared)(ctx, AuthzSpec(name="z"))

        from forze_authz.adapters.authorization import AuthzAdapter

        assert isinstance(port, AuthzAdapter)
