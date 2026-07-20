"""Unit tests for ``forze_identity.authn.execution`` dependency module and factories."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException
from tests.support.execution_context import (
    context_from_deps,
)

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    AuthnDepKey,
    AuthnSpec,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PasswordVerifierDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_identity.authn import (
    Argon2PasswordVerifier,
    AuthnOrchestrator,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
    JwtNativeUuidResolver,
)
from forze_identity.authn.adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    PasswordResetAdapter,
    TokenLifecycleAdapter,
)
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.execution import (
    AuthnDepsModule,
    AuthnKernelConfig,
    AuthnSharedServices,
    ConfigurableApiKeyLifecycle,
    ConfigurableArgon2PasswordVerifier,
    ConfigurableAuthn,
    ConfigurableForzeJwtTokenVerifier,
    ConfigurableHmacApiKeyVerifier,
    ConfigurableJwtNativeUuidResolver,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurablePasswordReset,
    ConfigurablePolicyPrincipalEligibility,
    ConfigurableTokenLifecycle,
    build_authn_shared_services,
)
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.constants import AuthzResourceName

# ----------------------- #


def _slow_password_config() -> PasswordConfig:
    return PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1)


def _kernel_min_access() -> AuthnKernelConfig:
    return AuthnKernelConfig(access_token_secret=b"k" * 32)


def _kernel_full() -> AuthnKernelConfig:
    return AuthnKernelConfig(
        access_token_secret=b"k" * 32,
        refresh_token_pepper=b"p" * 32,
        password=_slow_password_config(),
        api_key_pepper=b"a" * 32,
        reset_token_pepper=b"r" * 32,
    )


def _mock_doc_query_routes() -> dict[AuthnResourceName, object]:
    def factory(ctx: object, spec: object) -> MagicMock:
        port = MagicMock()
        port.spec = spec

        return port

    return {
        AuthzResourceName.POLICY_PRINCIPALS: factory,
        AuthnResourceName.PASSWORD_ACCOUNTS: factory,
        AuthnResourceName.API_KEY_ACCOUNTS: factory,
        AuthnResourceName.PASSWORD_RESETS: factory,
        AuthnResourceName.TOKEN_SESSIONS: factory,
    }


def _mock_doc_command_routes() -> dict[AuthnResourceName, object]:
    def factory(ctx: object, spec: object) -> MagicMock:
        port = MagicMock()
        port.spec = spec

        return port

    return {
        AuthnResourceName.PASSWORD_ACCOUNTS: factory,
        AuthnResourceName.API_KEY_ACCOUNTS: factory,
        AuthnResourceName.PASSWORD_RESETS: factory,
        AuthnResourceName.TOKEN_SESSIONS: factory,
    }


def _eligibility_deps() -> Deps:
    factory = ConfigurablePolicyPrincipalEligibility()
    return Deps.routed(
        {
            PrincipalEligibilityDepKey: {
                "s": factory,
                "r": factory,
                "main": factory,
                "default": factory,
                "oauth": factory,
            },
        },
    )


def _document_deps_only() -> Deps:
    return Deps.routed(
        {
            DocumentQueryDepKey: _mock_doc_query_routes(),
            DocumentCommandDepKey: _mock_doc_command_routes(),
        },
    )


def _document_deps() -> Deps:
    return _document_deps_only().merge(_eligibility_deps())


# ....................... #


class TestAuthnDepsModule:
    def test_empty_module(self) -> None:
        deps = AuthnDepsModule()()

        assert isinstance(deps, Deps)

    def test_rejects_missing_kernel_when_routes(self) -> None:
        with pytest.raises(CoreException, match="kernel"):
            AuthnDepsModule(authn={"main": frozenset({"token"})})()

    def test_rejects_method_without_required_kernel_section(self) -> None:
        with pytest.raises(CoreException, match="api_key"):
            AuthnDepsModule(
                kernel=_kernel_min_access(),
                authn={"main": frozenset({"token", "api_key"})},
            )()

    def test_registers_token_route(self) -> None:
        deps = AuthnDepsModule(
            kernel=_kernel_min_access(),
            authn={"main": frozenset({"token"})},
        )()

        assert deps.exists(AuthnDepKey, route="main")
        assert deps.exists(TokenVerifierDepKey, route="main")
        assert deps.exists(PrincipalResolverDepKey, route="main")
        assert not deps.exists(PasswordVerifierDepKey, route="main")
        assert not deps.exists(ApiKeyVerifierDepKey, route="main")

    def test_registers_all_methods_and_lifecycles(self) -> None:
        deps = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"a": frozenset({"token", "password", "api_key"})},
            token_lifecycle={"a"},
            password_lifecycle={"a"},
            api_key_lifecycle={"a"},
            password_account_provisioning={"a"},
            password_reset={"a"},
        )()

        assert deps.exists(AuthnDepKey, route="a")
        assert deps.exists(TokenVerifierDepKey, route="a")
        assert deps.exists(PasswordVerifierDepKey, route="a")
        assert deps.exists(ApiKeyVerifierDepKey, route="a")
        assert deps.exists(PrincipalResolverDepKey, route="a")
        assert deps.exists(TokenLifecycleDepKey, route="a")
        assert deps.exists(PasswordLifecycleDepKey, route="a")
        assert deps.exists(ApiKeyLifecycleDepKey, route="a")
        assert deps.exists(PasswordAccountProvisioningDepKey, route="a")
        assert deps.exists(PasswordResetDepKey, route="a")

    def test_password_reset_requires_reset_pepper(self) -> None:
        with pytest.raises(CoreException, match=r"kernel\.reset_token_pepper"):
            AuthnDepsModule(
                kernel=AuthnKernelConfig(password=_slow_password_config()),
                password_reset={"a"},
            )()

    def test_password_reset_requires_password_section(self) -> None:
        with pytest.raises(CoreException, match=r"kernel\.password"):
            AuthnDepsModule(
                kernel=AuthnKernelConfig(reset_token_pepper=b"r" * 32),
                password_reset={"a"},
            )()

    def test_token_verifier_override_without_resolver_rejected(self) -> None:
        with pytest.raises(CoreException, match="'main'"):
            AuthnDepsModule(
                kernel=_kernel_min_access(),
                authn={"main": frozenset({"token"})},
                token_verifiers={"main": MagicMock()},
            )()

    def test_token_verifier_override_with_resolver_passes(self) -> None:
        deps = AuthnDepsModule(
            kernel=AuthnKernelConfig(),
            authn={"main": frozenset({"token"})},
            token_verifiers={"main": MagicMock()},
            resolvers={"main": ConfigurableJwtNativeUuidResolver()},
        )()

        assert deps.exists(AuthnDepKey, route="main")
        assert deps.exists(TokenVerifierDepKey, route="main")
        assert deps.exists(PrincipalResolverDepKey, route="main")

    def test_resolver_override(self) -> None:
        sentinel = ConfigurableJwtNativeUuidResolver()
        deps = AuthnDepsModule(
            kernel=_kernel_min_access(),
            authn={"main": frozenset({"token"})},
            resolvers={"main": sentinel},
        )()

        ctx = context_from_deps(deps.merge(_document_deps_only()))
        resolver = ctx.deps.provide(PrincipalResolverDepKey, route="main")(
            ctx, AuthnSpec(name="main", enabled_methods=frozenset({"token"}))
        )

        assert isinstance(resolver, JwtNativeUuidResolver)

    def test_module_flags_flow_into_password_factories(self) -> None:
        deps = AuthnDepsModule(
            kernel=AuthnKernelConfig(password=_slow_password_config()),
            authn={"main": frozenset({"password"})},
            password_lifecycle={"main"},
            revoke_sessions_on_password_change=False,
            password_rehash_on_login=True,
        )().merge(_document_deps_only())

        ctx = context_from_deps(deps)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        verifier = ctx.deps.provide(PasswordVerifierDepKey, route="main")(ctx, spec)
        lifecycle = ctx.deps.provide(PasswordLifecycleDepKey, route="main")(ctx, spec)

        assert isinstance(verifier, Argon2PasswordVerifier)
        assert verifier.pa_cmd is not None
        assert isinstance(lifecycle, PasswordLifecycleAdapter)
        assert lifecycle.revoke_sessions_on_password_change is False
        assert lifecycle.session_qry is None

    def test_module_defaults_revoke_sessions_and_skip_rehash(self) -> None:
        deps = AuthnDepsModule(
            kernel=AuthnKernelConfig(password=_slow_password_config()),
            authn={"main": frozenset({"password"})},
            password_lifecycle={"main"},
        )().merge(_document_deps_only())

        ctx = context_from_deps(deps)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        verifier = ctx.deps.provide(PasswordVerifierDepKey, route="main")(ctx, spec)
        lifecycle = ctx.deps.provide(PasswordLifecycleDepKey, route="main")(ctx, spec)

        assert isinstance(verifier, Argon2PasswordVerifier)
        assert verifier.pa_cmd is None
        assert isinstance(lifecycle, PasswordLifecycleAdapter)
        assert lifecycle.revoke_sessions_on_password_change is True
        assert lifecycle.session_qry is not None
        assert lifecycle.session_cmd is not None


# ....................... #


class TestConfigurableFactories:
    def _shared_min(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_min_access())

    def _shared_full(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_full())

    def _ctx(self) -> ExecutionContext:
        merged = AuthnDepsModule()().merge(_document_deps())

        return context_from_deps(merged)

    def test_token_verifier_factory(self) -> None:
        ctx = self._ctx()

        factory = ConfigurableForzeJwtTokenVerifier(shared=self._shared_min())
        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, ForzeJwtTokenVerifier)
        assert port.session_qry is not None

    def test_token_verifier_requires_secret(self) -> None:
        ctx = self._ctx()

        empty_shared = build_authn_shared_services(AuthnKernelConfig())
        factory = ConfigurableForzeJwtTokenVerifier(shared=empty_shared)

        with pytest.raises(CoreException, match="access_token_secret"):
            factory(ctx, AuthnSpec(name="s"))

    def test_password_verifier_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurableArgon2PasswordVerifier(
            shared=build_authn_shared_services(kernel),
        )

        port = factory(
            ctx, AuthnSpec(name="s", enabled_methods=frozenset({"password"}))
        )

        assert isinstance(port, Argon2PasswordVerifier)
        assert port.pa_cmd is None

    def test_password_verifier_factory_rehash_on_login_wires_command_port(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurableArgon2PasswordVerifier(
            shared=build_authn_shared_services(kernel),
            rehash_on_login=True,
        )

        port = factory(
            ctx, AuthnSpec(name="s", enabled_methods=frozenset({"password"}))
        )

        assert isinstance(port, Argon2PasswordVerifier)
        assert port.pa_cmd is not None

    def test_api_key_verifier_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(api_key_pepper=b"a" * 32)
        factory = ConfigurableHmacApiKeyVerifier(
            shared=build_authn_shared_services(kernel),
        )

        port = factory(ctx, AuthnSpec(name="s", enabled_methods=frozenset({"api_key"})))

        assert isinstance(port, HmacApiKeyVerifier)

    def test_orchestrator_factory_resolves_through_deps(self) -> None:
        merged = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"main": frozenset({"token", "password"})},
        )().merge(_document_deps_only())

        ctx = context_from_deps(merged)

        factory = ctx.deps.provide(AuthnDepKey, route="main")
        port = factory(
            ctx,
            AuthnSpec(name="main", enabled_methods=frozenset({"token", "password"})),
        )

        assert isinstance(port, AuthnOrchestrator)
        assert port.token_verifier is not None
        assert port.password_verifier is not None
        assert port.api_key_verifier is None
        assert isinstance(port.resolver, JwtNativeUuidResolver)

    def test_orchestrator_rejects_method_set_mismatch(self) -> None:
        merged = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"main": frozenset({"token"})},
        )().merge(_document_deps_only())

        ctx = context_from_deps(merged)
        factory = ctx.deps.provide(AuthnDepKey, route="main")

        with pytest.raises(CoreException, match="enabled_methods"):
            factory(
                ctx,
                AuthnSpec(
                    name="main", enabled_methods=frozenset({"token", "password"})
                ),
            )

    def test_token_lifecycle_factory(self) -> None:
        ctx = self._ctx()

        factory = ConfigurableTokenLifecycle(shared=self._shared_full())
        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, TokenLifecycleAdapter)

    def test_password_lifecycle_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurablePasswordLifecycle(
            shared=build_authn_shared_services(kernel)
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordLifecycleAdapter)
        assert port.revoke_sessions_on_password_change is True
        assert port.session_qry is not None
        assert port.session_cmd is not None

    def test_password_lifecycle_factory_revocation_opt_out(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurablePasswordLifecycle(
            shared=build_authn_shared_services(kernel),
            revoke_sessions_on_password_change=False,
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordLifecycleAdapter)
        assert port.revoke_sessions_on_password_change is False
        assert port.session_qry is None
        assert port.session_cmd is None

    def test_api_key_lifecycle_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(api_key_pepper=b"a" * 32)
        factory = ConfigurableApiKeyLifecycle(
            shared=build_authn_shared_services(kernel)
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, ApiKeyLifecycleAdapter)

    def test_password_provisioning_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurablePasswordAccountProvisioning(
            shared=build_authn_shared_services(kernel)
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordAccountProvisioningAdapter)

    def test_password_reset_factory(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(
            password=_slow_password_config(),
            reset_token_pepper=b"r" * 32,
        )
        factory = ConfigurablePasswordReset(shared=build_authn_shared_services(kernel))

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordResetAdapter)
        assert port.revoke_sessions_on_reset is True
        assert port.session_qry is not None
        assert port.session_cmd is not None

    def test_password_reset_factory_revocation_opt_out(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(
            password=_slow_password_config(),
            reset_token_pepper=b"r" * 32,
        )
        factory = ConfigurablePasswordReset(
            shared=build_authn_shared_services(kernel),
            revoke_sessions_on_reset=False,
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordResetAdapter)
        assert port.revoke_sessions_on_reset is False
        assert port.session_qry is None
        assert port.session_cmd is None

    def test_password_reset_factory_requires_reset_service(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        factory = ConfigurablePasswordReset(shared=build_authn_shared_services(kernel))

        with pytest.raises(CoreException, match=r"kernel\.reset_token_pepper"):
            factory(ctx, AuthnSpec(name="s"))

    def test_shared_password_service_across_authn_and_password_lifecycle(self) -> None:
        kernel = AuthnKernelConfig(
            access_token_secret=b"k" * 32,
            password=_slow_password_config(),
        )

        deps = AuthnDepsModule(
            kernel=kernel,
            authn={"r": frozenset({"password"})},
            password_lifecycle={"r"},
        )().merge(_document_deps_only())

        ctx = context_from_deps(deps)

        spec = AuthnSpec(name="r", enabled_methods=frozenset({"password"}))
        auth = ctx.deps.provide(AuthnDepKey, route="r")(ctx, spec)
        pl = ctx.deps.provide(PasswordLifecycleDepKey, route="r")(ctx, spec)

        assert isinstance(auth, AuthnOrchestrator)
        assert isinstance(pl, PasswordLifecycleAdapter)
        assert isinstance(auth.password_verifier, Argon2PasswordVerifier)
        assert auth.password_verifier.password_svc is pl.password_svc

    def test_configurable_authn_post_init_validates_required_verifiers(self) -> None:
        eligibility = MagicMock()
        with pytest.raises(CoreException, match="PasswordVerifierPort"):
            AuthnOrchestrator(
                resolver=JwtNativeUuidResolver(),
                eligibility=eligibility,
                enabled_methods=frozenset({"password"}),
            )

    def test_configurable_authn_factory_construction(self) -> None:
        # ConfigurableAuthn snapshots enabled_methods at registration time.
        factory = ConfigurableAuthn(enabled_methods=frozenset({"token"}))

        assert factory.enabled_methods == frozenset({"token"})


class TestCredentialSpecsAreSensitive:
    """Credential-bearing authn specs must be marked ``sensitive`` so generated
    external surfaces (HTTP route generators, MCP) refuse to project them."""

    def test_credential_specs_marked_sensitive(self) -> None:
        from forze_identity.authn.application.specs import (
            api_key_account_spec,
            password_account_spec,
            password_invite_spec,
            password_reset_spec,
            session_spec,
        )

        assert password_account_spec.sensitive is True
        assert api_key_account_spec.sensitive is True
        assert password_invite_spec.sensitive is True
        assert password_reset_spec.sensitive is True
        assert session_spec.sensitive is True
