"""The local preset's eligibility gate: the authz registry where it resolves, the config elsewhere."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnSpec,
    PrincipalEligibilityDepKey,
)
from forze.application.contracts.deps import Deps
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters.principal_eligibility import (
    PolicyPrincipalEligibilityAdapter,
)
from forze_identity.authn.execution.deps.deps import (
    ConfigurableAllowAllEligibility,
    ConfigurablePolicyPrincipalEligibility,
)
from forze_identity.authz import policy_principal_spec
from forze_identity.authz.domain import CreatePolicyPrincipalCmd
from forze_identity.builtin.local import (
    ConfigurableLocalPrincipalEligibility,
    LocalIdentityConfig,
    LocalPrincipalEligibility,
    local_identity_deps,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.execution.factories import ConfigurableMockDocument
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

_PID = UUID("550e8400-e29b-41d4-a716-446655440000")
_TENANT_ONLY = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
_TID = UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
_UNKNOWN = UUID("00000000-0000-4000-8000-000000000001")

_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))
_CONFIG = LocalIdentityConfig.from_mapping(
    {
        "api_keys": {"dev-key": {"principal_id": str(_PID)}},
        "principal_tenants": {str(_TENANT_ONLY): str(_TID)},
    },
)

# ----------------------- #


def _routed_store() -> Deps:
    """The policy-principal document on its own route, with no plain document fallback."""

    mock = MockDepsModule()
    document = ConfigurableMockDocument(module=mock)

    return Deps.routed(
        {
            DocumentQueryDepKey: {policy_principal_spec.name: document},
            DocumentCommandDepKey: {policy_principal_spec.name: document},
        },
    ).merge(Deps.plain({MockStateDepKey: mock.state}))


def _mock() -> Deps:
    """The mock's deps, whose plain document fallback resolves every route."""

    return MockDepsModule()()


def _gate(ctx: ExecutionContext) -> Any:
    return ConfigurableLocalPrincipalEligibility(config=_CONFIG)(ctx, _SPEC)


async def _refused(gate: Any, principal: UUID) -> CoreException:
    with pytest.raises(CoreException) as caught:
        await gate.require_authentication_allowed(principal)

    return caught.value


# ....................... #


class TestTheConfigGate:
    async def test_a_principal_named_on_a_key_is_admitted(self) -> None:
        await LocalPrincipalEligibility.of(_CONFIG).require_authentication_allowed(
            _PID, for_update=True
        )

    @pytest.mark.parametrize("principal", [_TENANT_ONLY, _UNKNOWN], ids=["tenant-only", "unknown"])
    async def test_any_other_principal_is_refused(self, principal: UUID) -> None:
        # A principal named only in principal_tenants holds no credential, so it is not one.
        error = await _refused(LocalPrincipalEligibility.of(_CONFIG), principal)

        assert error.kind is ExceptionKind.AUTHENTICATION
        assert "Principal not found" in str(error)


class TestDetection:
    def test_without_a_policy_principal_store_the_config_is_the_registry(self) -> None:
        gate = _gate(context_from_deps(Deps()))

        assert isinstance(gate, LocalPrincipalEligibility)
        assert gate.principals == frozenset({_PID})

    @pytest.mark.parametrize("store", [_routed_store, _mock], ids=["routed", "plain-fallback"])
    async def test_where_the_store_resolves_it_builds_the_default_gate(self, store: Any) -> None:
        ctx = context_from_deps(store())
        gate = _gate(ctx)

        assert isinstance(gate, PolicyPrincipalEligibilityAdapter)
        # The registry decides now: a configured principal with no row is refused...
        assert (await _refused(gate, _PID)).kind is ExceptionKind.AUTHENTICATION

        # ...and a deactivated one is refused too, which the config gate would admit.
        await ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user", is_active=False),
            id=_PID,
        )
        assert (await _refused(gate, _PID)).kind is ExceptionKind.AUTHENTICATION

        await ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user"),
            id=_UNKNOWN,
        )
        await gate.require_authentication_allowed(_UNKNOWN)


class TestThePreset:
    async def test_it_authenticates_its_own_keys_without_an_authz_plane(self) -> None:
        # Before the config gate, this wiring failed every authentication with a 500.
        ctx = context_from_deps(local_identity_deps(_CONFIG))

        result = await ctx.authn.authn(_SPEC).authenticate_with_api_key(
            ApiKeyCredentials(key="dev-key"),
        )

        assert result.identity.principal_id == _PID

    def test_it_registers_the_detecting_gate_by_default(self) -> None:
        ctx = context_from_deps(local_identity_deps(_CONFIG))

        assert isinstance(
            ctx.deps.provide(PrincipalEligibilityDepKey, route="main"),
            ConfigurableLocalPrincipalEligibility,
        )

    @pytest.mark.parametrize(
        ("choice", "registered"),
        [
            ("policy_principal", ConfigurablePolicyPrincipalEligibility),
            ("allow_all", ConfigurableAllowAllEligibility),
        ],
    )
    def test_an_explicit_choice_skips_detection(self, choice: Any, registered: type) -> None:
        deps = local_identity_deps(_CONFIG, eligibility=choice).merge(_routed_store())
        ctx = context_from_deps(deps)

        assert isinstance(ctx.deps.provide(PrincipalEligibilityDepKey, route="main"), registered)

    def test_an_explicit_policy_principal_choice_is_refused_without_its_store(self) -> None:
        # The detecting gate declares nothing; the default one needs its document at boot.
        with pytest.raises(CoreException) as caught:
            context_from_deps(local_identity_deps(_CONFIG, eligibility="policy_principal"))

        assert caught.value.code == "dependency_requirement_unbound"
