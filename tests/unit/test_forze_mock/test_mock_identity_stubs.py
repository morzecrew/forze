"""Identity stub ports registered by :class:`~forze_mock.execution.MockDepsModule`."""

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
from forze.application.contracts.secrets import SecretsDepKey, SecretRef
from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantResolverDepKey,
)
from forze_mock import MockDepsModule
from forze_mock.adapters.identity import MockSecretsPort
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
