"""Identity stub ports registered by :class:`~forze_mock.execution.MockDepsModule`."""

from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    AuthnDepKey,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordVerifierDepKey,
    PrincipalDeactivationDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
)
from forze.application.contracts.authz import (
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
    GrantQueryDepKey,
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
)
from forze.application.contracts.authn.value_objects.credentials import (
    PasswordCredentials,
)
from forze.application.contracts.authz.value_objects import (
    AuthzRequest,
    AuthzScope,
    AuthzSensitiveAccessRequest,
    AuthzSubject,
)
from forze.application.contracts.secrets import SecretsDepKey, SecretRef
from forze.application.contracts.tenancy import (
    NoopTenantProvisioner,
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.identity import (
    MockAuthzDecisionPort,
    MockAuthzScopePort,
    MockPasswordVerifierPort,
    MockSecretsPort,
    MockTenantManagementPort,
    MockTenantResolverPort,
)
from tests.support.execution_context import context_from_modules

# ----------------------- #

_AUTHN_KEYS = (
    AuthnDepKey,
    PasswordVerifierDepKey,
    TokenVerifierDepKey,
    ApiKeyVerifierDepKey,
    PrincipalResolverDepKey,
    PrincipalEligibilityDepKey,
    PrincipalDeactivationDepKey,
    TokenLifecycleDepKey,
    PasswordLifecycleDepKey,
    ApiKeyLifecycleDepKey,
    PasswordAccountProvisioningDepKey,
)

_AUTHZ_KEYS = (
    PrincipalRegistryDepKey,
    RoleAssignmentDepKey,
    GrantQueryDepKey,
    AuthzDecisionDepKey,
    AuthzScopeDepKey,
)

_TENANCY_KEYS = (TenantResolverDepKey, TenantManagementDepKey)


async def test_identity_stubs_resolve_from_deps_module() -> None:
    mod = MockDepsModule()
    ctx = context_from_modules(mod)

    assert isinstance(ctx.deps.provide(SecretsDepKey), MockSecretsPort)

    for key in _AUTHN_KEYS:
        assert ctx.deps.provide(key, route="main") is not None

    for key in _AUTHZ_KEYS:
        assert ctx.deps.provide(key, route="main") is not None

    for key in _TENANCY_KEYS:
        assert ctx.deps.provide(key, route="main") is not None


async def test_secrets_port_reads_from_state() -> None:
    mod = MockDepsModule()
    mod.state.identity["secrets"]["db/dsn"] = "postgres://local"
    ctx = context_from_modules(mod)
    secrets = ctx.deps.provide(SecretsDepKey)
    value = await secrets.resolve_str(SecretRef(path="db/dsn"))
    assert value == "postgres://local"


# ----------------------- #
# Authn: password verifier compares the seeded password


def _seed_password(state: MockState, login: str, password: str) -> None:
    authn = state.identity.setdefault("authn", {})
    route = authn.setdefault("main", {})
    route.setdefault("passwords", {})[login] = {
        "subject": login,
        "password": password,
    }


async def test_password_verifier_accepts_correct_password() -> None:
    state = MockState()
    _seed_password(state, "alice", "s3cret")
    verifier = MockPasswordVerifierPort(state=state)

    assertion = await verifier.verify_password(
        PasswordCredentials(login="alice", password="s3cret")
    )

    assert assertion.subject == "alice"


async def test_password_verifier_rejects_wrong_password() -> None:
    state = MockState()
    _seed_password(state, "alice", "s3cret")
    verifier = MockPasswordVerifierPort(state=state)

    with pytest.raises(CoreException, match="Invalid login or password") as excinfo:
        await verifier.verify_password(
            PasswordCredentials(login="alice", password="wrong")
        )

    assert excinfo.value.kind is ExceptionKind.AUTHENTICATION


async def test_password_verifier_rejects_entry_without_password() -> None:
    state = MockState()
    authn = state.identity.setdefault("authn", {})
    authn.setdefault("main", {}).setdefault("passwords", {})["bob"] = {
        "subject": "bob",
    }
    verifier = MockPasswordVerifierPort(state=state)

    with pytest.raises(CoreException, match="Invalid login or password"):
        await verifier.verify_password(
            PasswordCredentials(login="bob", password="anything")
        )


async def test_password_verifier_unknown_login_same_message() -> None:
    verifier = MockPasswordVerifierPort(state=MockState())

    with pytest.raises(CoreException, match="Invalid login or password"):
        await verifier.verify_password(
            PasswordCredentials(login="ghost", password="x")
        )


# ----------------------- #
# Authz: deny-by-default decision and sensitive-resource stubs


async def test_authz_decision_denies_by_default() -> None:
    port = MockAuthzDecisionPort()
    request = AuthzRequest(
        subject=AuthzSubject(principal_id=uuid4()),
        action="doc.read",
    )

    decision = await port.authorize(request)

    assert decision.allowed is False


async def test_authz_decision_allows_when_opted_in() -> None:
    port = MockAuthzDecisionPort(allow_by_default=True)
    request = AuthzRequest(
        subject=AuthzSubject(principal_id=uuid4()),
        action="doc.read",
    )

    decision = await port.authorize(request)

    assert decision.allowed is True


async def test_authz_scope_sensitive_resource_denied_by_default() -> None:
    request = AuthzSensitiveAccessRequest(
        subject=AuthzSubject(principal_id=uuid4()),
        scope=AuthzScope(),
        resource_type="document",
        resource_id=uuid4(),
        action="doc.read",
    )

    assert await MockAuthzScopePort().authorize_sensitive_resource(request) is False
    assert (
        await MockAuthzScopePort(
            allow_sensitive_by_default=True
        ).authorize_sensitive_resource(request)
        is True
    )


# ----------------------- #
# Tenancy: resolver mirrors the real adapter semantics


async def test_tenant_resolver_requested_tenant_requires_membership() -> None:
    state = MockState()
    mgmt = MockTenantManagementPort(state=state)
    resolver = MockTenantResolverPort(state=state)

    tenant = await mgmt.provision_tenant(tenant_key="acme")
    principal_id = uuid4()
    # Tenant exists but the principal is NOT a member.

    with pytest.raises(CoreException) as excinfo:
        await resolver.resolve_from_principal(
            principal_id,
            requested_tenant_id=tenant.tenant_id,
        )

    assert excinfo.value.kind is ExceptionKind.AUTHENTICATION
    assert excinfo.value.code == "tenant_mismatch"


async def test_deprovision_missing_tenant_raises_like_real_adapter() -> None:
    # Parity: the real adapter loads the tenant (a document ``get`` that raises) before
    # tearing down infra, so the mock must fail closed on a missing tenant too.
    state = MockState()
    mgmt = MockTenantManagementPort(state=state, provisioner=NoopTenantProvisioner())

    with pytest.raises(CoreException) as excinfo:
        await mgmt.deprovision_tenant(uuid4())

    assert excinfo.value.kind is ExceptionKind.NOT_FOUND


async def test_tenant_resolver_resolves_member_requested_tenant() -> None:
    state = MockState()
    mgmt = MockTenantManagementPort(state=state)
    resolver = MockTenantResolverPort(state=state)

    tenant = await mgmt.provision_tenant(tenant_key="acme")
    principal_id = uuid4()
    await mgmt.attach_principal(principal_id, tenant.tenant_id)

    identity = await resolver.resolve_from_principal(
        principal_id,
        requested_tenant_id=tenant.tenant_id,
    )

    assert identity is not None
    assert identity.tenant_id == tenant.tenant_id
    assert identity.tenant_key == "acme"


async def test_tenant_resolver_inactive_tenant_raises() -> None:
    state = MockState()
    mgmt = MockTenantManagementPort(state=state)
    resolver = MockTenantResolverPort(state=state)

    tenant = await mgmt.provision_tenant(tenant_key="acme")
    principal_id = uuid4()
    await mgmt.attach_principal(principal_id, tenant.tenant_id)
    await mgmt.deactivate_tenant(tenant.tenant_id)

    with pytest.raises(CoreException) as excinfo:
        await resolver.resolve_from_principal(
            principal_id,
            requested_tenant_id=tenant.tenant_id,
        )

    assert excinfo.value.code == "tenant_inactive"

    with pytest.raises(CoreException) as excinfo:
        await resolver.resolve_from_principal(principal_id)

    assert excinfo.value.code == "tenant_inactive"


async def test_tenant_resolver_ambiguous_membership_raises() -> None:
    state = MockState()
    mgmt = MockTenantManagementPort(state=state)
    resolver = MockTenantResolverPort(state=state)

    principal_id = uuid4()
    t1 = await mgmt.provision_tenant(tenant_key="one")
    t2 = await mgmt.provision_tenant(tenant_key="two")
    await mgmt.attach_principal(principal_id, t1.tenant_id)
    await mgmt.attach_principal(principal_id, t2.tenant_id)

    with pytest.raises(CoreException) as excinfo:
        await resolver.resolve_from_principal(principal_id)

    assert excinfo.value.code == "tenant_ambiguous"


async def test_tenant_resolver_no_membership_returns_none() -> None:
    resolver = MockTenantResolverPort(state=MockState())

    assert await resolver.resolve_from_principal(uuid4()) is None
