"""Unit tests for ``forze_authz.execution`` dependency wiring."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException

pytestmark = pytest.mark.unit

from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
    GrantQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_authz.execution import (
    AuthzDepsModule,
    AuthzKernelConfig,
    ConfigurableAuthzDecision,
    build_authz_shared_services,
)


class TestAuthzDepsModule:
    def test_empty_module(self) -> None:
        deps = AuthzDepsModule()()

        assert isinstance(deps, Deps)

    def test_requires_kernel_when_routes_registered(self) -> None:
        with pytest.raises(CoreException, match="kernel"):
            AuthzDepsModule(decision={"main"})()

    def test_registers_decision_and_scope_routes(self) -> None:
        deps = AuthzDepsModule(
            kernel=AuthzKernelConfig(),
            decision={"main"},
            scope={"main"},
            grant_query={"main"},
        )()

        assert deps.exists(AuthzDecisionDepKey, route="main")
        assert deps.exists(AuthzScopeDepKey, route="main")
        assert deps.exists(GrantQueryDepKey, route="main")


class TestAuthzSharedServices:
    def test_build_policy_service(self) -> None:
        shared = build_authz_shared_services(AuthzKernelConfig())

        assert shared.policy is not None


class TestConfigurableRuntimeFactory:
    def test_builds_adapter(self) -> None:
        from unittest.mock import MagicMock

        from forze.application.contracts.authz import AuthzSpec
        from forze.application.contracts.document import (
            DocumentCommandDepKey,
            DocumentQueryDepKey,
        )
        from forze_authz.adapters.authorization import AuthzDecisionAdapter
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

        port = ConfigurableAuthzDecision(shared=shared)(
            ctx,
            AuthzSpec(name="z"),
        )

        assert isinstance(port, AuthzDecisionAdapter)
