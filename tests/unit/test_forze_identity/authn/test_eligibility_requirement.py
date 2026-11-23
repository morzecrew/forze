"""An authn gate that could never be built refuses the boot, however its wiring is passed."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import Deps
from forze.application.contracts.document import DocumentQueryDepKey
from forze.application.execution import DepsRegistry, ProductionPosture, build_runtime
from forze.base.exceptions import CoreException
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig, policy_principal_spec
from forze_identity.builtin.idp._oidc import oidc_bootstrap_identity_deps
from forze_identity.builtin.idp.google import GoogleOidcConfig
from forze_identity.builtin.idp.google.deps import ConfigurableGoogleOidcVerifier
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

# ----------------------- #

_UNBOUND = (
    "'authn_principal_eligibility' on route 'main' resolves 'document_query' on route "
    "'authz_policy_principals', which is not registered and has no plain fallback."
)
_REMEDY = (
    "Wire the authz plane's policy principal document, pass AuthnDepsModule(eligibility=...) a "
    'gate over your own principal registry, or declare eligibility="allow_all".'
)
_STORE = Deps.routed(
    {DocumentQueryDepKey: {policy_principal_spec.name: lambda ctx, spec: MagicMock()}}
)


def _module(**kwargs: Any) -> AuthnDepsModule:
    return AuthnDepsModule(
        kernel=AuthnKernelConfig(access_token_secret=b"k" * 32, refresh_token_pepper=b"p" * 32),
        authn={"main": frozenset({"token"})},
        **kwargs,
    )


def _refusal(registry: DepsRegistry) -> str:
    with pytest.raises(CoreException) as caught:
        registry.freeze()

    assert caught.value.code == "dependency_requirement_unbound"

    return caught.value.summary


# ....................... #


class TestTheDefaultGate:
    @pytest.mark.parametrize("passed", ["module", "built-deps"])
    def test_without_its_store_the_boot_is_refused(self, passed: str) -> None:
        registry = (
            DepsRegistry.from_modules(_module())
            if passed == "module"
            else DepsRegistry.from_deps(_module()())
        )

        assert _refusal(registry) == f"{_UNBOUND} {_REMEDY}"

    def test_a_routed_store_satisfies_it(self) -> None:
        DepsRegistry.from_deps(_module()(), _STORE).freeze()

    def test_the_mocks_plain_fallback_satisfies_it(self) -> None:
        DepsRegistry.from_modules(_module(), MockDepsModule()).freeze()

    @pytest.mark.parametrize("eligibility", ["allow_all", lambda ctx, spec: MagicMock()])
    def test_any_other_gate_needs_no_store(self, eligibility: Any) -> None:
        DepsRegistry.from_modules(_module(eligibility=eligibility)).freeze()


class TestThePresets:
    def test_the_oidc_bootstrap_preset_without_authz_is_refused(self) -> None:
        deps = oidc_bootstrap_identity_deps(
            authn_route="main",
            token_verifier=ConfigurableGoogleOidcVerifier(config=GoogleOidcConfig(client_id="a")),
        )

        assert _UNBOUND in _refusal(DepsRegistry.from_deps(deps))


class TestPrincipalDeactivation:
    def _registry(self, *extra: Deps) -> DepsRegistry:
        module = _module(token_lifecycle={"main"}, principal_deactivation={"main"}, authz_route="a")

        return DepsRegistry.from_deps(module(), _STORE, *extra)

    def test_without_the_authz_registry_the_boot_is_refused(self) -> None:
        summary = _refusal(self._registry())

        assert summary.startswith(
            "'authn_principal_deactivation' on route 'main' resolves 'authz_principal_registry' "
            "on route 'a', which is not registered and has no plain fallback."
        )
        assert "AuthzDepsModule(principal_registry=...)" in summary

    def test_the_authz_registry_satisfies_it(self) -> None:
        authz = AuthzDepsModule(kernel=AuthzKernelConfig(), principal_registry={"a"})()

        self._registry(authz).freeze()

    def test_without_a_token_lifecycle_on_its_route_the_boot_is_refused(self) -> None:
        # Deactivation revokes the principal's tokens on its own route, so that route needs
        # a token lifecycle as well as the authz registry.
        authz = AuthzDepsModule(kernel=AuthzKernelConfig(), principal_registry={"a"})()
        module = _module(principal_deactivation={"main"}, authz_route="a")

        summary = _refusal(DepsRegistry.from_deps(module(), _STORE, authz))

        assert summary.startswith(
            "'authn_principal_deactivation' on route 'main' resolves 'authn_token_lifecycle' "
            "on route 'main', which is not registered and has no plain fallback."
        )
        assert "AuthnDepsModule(token_lifecycle=...)" in summary


class _Settings(BaseModel):
    env: str | None = None


class TestAllowAllPassedAsBuiltDeps:
    def test_a_production_posture_refuses_it(self) -> None:
        posture = ProductionPosture(settings=_Settings(env="production"), environment="env")

        with pytest.raises(CoreException) as caught:
            build_runtime(deps=_module(eligibility="allow_all")(), posture=posture)

        assert "authn_eligibility:main: admits every principal" in caught.value.summary

    def test_the_module_and_its_providers_report_each_route_once(self) -> None:
        frozen = DepsRegistry.from_modules(
            _module(eligibility="allow_all", token_lifecycle={"admin"})
        ).freeze()

        assert sorted(f.target for f in frozen.posture_findings) == [
            "authn_eligibility:admin",
            "authn_eligibility:main",
        ]
