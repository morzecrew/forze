"""Unit tests for ``forze_authn.execution`` dependency module and factories."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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
    PasswordVerifierDepKey,
    PrincipalResolverDepKey,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze_authn import (
    Argon2PasswordVerifier,
    AuthnOrchestrator,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
    JwtNativeUuidResolver,
)
from forze_authn.adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from forze_authn.application.constants import AuthnResourceName
from forze_authn.execution import (
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
    ConfigurableTokenLifecycle,
    build_authn_shared_services,
)
from forze_authn.services import PasswordConfig

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
    )


def _mock_doc_query_routes() -> dict[AuthnResourceName, object]:
    def factory(ctx: object, spec: object) -> MagicMock:
        port = MagicMock()
        port.spec = spec

        return port

    return {
        AuthnResourceName.PRINCIPALS: factory,
        AuthnResourceName.PASSWORD_ACCOUNTS: factory,
        AuthnResourceName.API_KEY_ACCOUNTS: factory,
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
        AuthnResourceName.TOKEN_SESSIONS: factory,
    }


def _document_deps() -> Deps[str]:
    return Deps.routed(
        {
            DocumentQueryDepKey: _mock_doc_query_routes(),
            DocumentCommandDepKey: _mock_doc_command_routes(),
        },
    )


# ....................... #


class TestAuthnDepsModule:
    def test_empty_module(self) -> None:
        deps = AuthnDepsModule()()

        assert isinstance(deps, Deps)

    def test_rejects_missing_kernel_when_routes(self) -> None:
        with pytest.raises(CoreError, match="kernel"):
            AuthnDepsModule(authn={"main": frozenset({"token"})})()

    def test_rejects_method_without_required_kernel_section(self) -> None:
        with pytest.raises(CoreError, match="api_key"):
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

    def test_resolver_override(self) -> None:
        sentinel = ConfigurableJwtNativeUuidResolver()
        deps = AuthnDepsModule(
            kernel=_kernel_min_access(),
            authn={"main": frozenset({"token"})},
            resolvers={"main": sentinel},
        )()

        ctx = ExecutionContext(deps=deps.merge(_document_deps()))
        resolver = ctx.dep(PrincipalResolverDepKey, route="main")(
            ctx, AuthnSpec(name="main", enabled_methods=frozenset({"token"}))
        )

        assert isinstance(resolver, JwtNativeUuidResolver)


# ....................... #


class TestConfigurableFactories:
    def _shared_min(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_min_access())

    def _shared_full(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_full())

    def _ctx(self) -> ExecutionContext:
        merged = AuthnDepsModule()().merge(_document_deps())

        return ExecutionContext(deps=merged)

    def test_token_verifier_factory(self) -> None:
        ctx = self._ctx()

        factory = ConfigurableForzeJwtTokenVerifier(shared=self._shared_min())
        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, ForzeJwtTokenVerifier)

    def test_token_verifier_requires_secret(self) -> None:
        ctx = self._ctx()

        empty_shared = build_authn_shared_services(AuthnKernelConfig())
        factory = ConfigurableForzeJwtTokenVerifier(shared=empty_shared)

        with pytest.raises(CoreError, match="access_token_secret"):
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
        )().merge(_document_deps())

        ctx = ExecutionContext(deps=merged)

        factory = ctx.dep(AuthnDepKey, route="main")
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
        )().merge(_document_deps())

        ctx = ExecutionContext(deps=merged)
        factory = ctx.dep(AuthnDepKey, route="main")

        with pytest.raises(CoreError, match="enabled_methods"):
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

    def test_shared_password_service_across_authn_and_password_lifecycle(self) -> None:
        kernel = AuthnKernelConfig(
            access_token_secret=b"k" * 32,
            password=_slow_password_config(),
        )

        deps = AuthnDepsModule(
            kernel=kernel,
            authn={"r": frozenset({"password"})},
            password_lifecycle={"r"},
        )().merge(_document_deps())

        ctx = ExecutionContext(deps=deps)

        spec = AuthnSpec(name="r", enabled_methods=frozenset({"password"}))
        auth = ctx.dep(AuthnDepKey, route="r")(ctx, spec)
        pl = ctx.dep(PasswordLifecycleDepKey, route="r")(ctx, spec)

        assert isinstance(auth, AuthnOrchestrator)
        assert isinstance(pl, PasswordLifecycleAdapter)
        assert isinstance(auth.password_verifier, Argon2PasswordVerifier)
        assert auth.password_verifier.password_svc is pl.password_svc

    def test_configurable_authn_post_init_validates_required_verifiers(self) -> None:
        with pytest.raises(CoreError, match="PasswordVerifierPort"):
            AuthnOrchestrator(
                resolver=JwtNativeUuidResolver(),
                enabled_methods=frozenset({"password"}),
            )

    def test_configurable_authn_factory_construction(self) -> None:
        # ConfigurableAuthn snapshots enabled_methods at registration time.
        factory = ConfigurableAuthn(enabled_methods=frozenset({"token"}))

        assert factory.enabled_methods == frozenset({"token"})
