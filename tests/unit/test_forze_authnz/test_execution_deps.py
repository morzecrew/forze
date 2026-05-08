"""Unit tests for ``forze_authnz.authn.execution`` dependency module and factories."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    AuthnDepKey,
    AuthnSpec,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze_authnz.authn.adapters import (
    ApiKeyLifecycleAdapter,
    AuthnAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from forze_authnz.authn.application.constants import AuthnResourceName
from forze_authnz.authn.execution import (
    AuthnDepsModule,
    AuthnKernelConfig,
    AuthnRouteCaps,
    AuthnSharedServices,
    ConfigurableApiKeyLifecycle,
    ConfigurableAuthn,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
    build_authn_shared_services,
)
from forze_authnz.authn.services import PasswordConfig


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


class TestAuthnDepsModule:
    def test_empty_module(self) -> None:
        deps = AuthnDepsModule()()

        assert isinstance(deps, Deps)

    def test_rejects_empty_route_caps(self) -> None:
        with pytest.raises(CoreError, match="AuthnRouteCaps"):
            AuthnDepsModule(
                kernel=_kernel_min_access(),
                authn={"main": AuthnRouteCaps()},
            )()

    def test_rejects_missing_kernel_when_routes(self) -> None:
        with pytest.raises(CoreError, match="kernel"):
            AuthnDepsModule(
                authn={"main": AuthnRouteCaps(bearer=True)},
            )()

    def test_registers_authn_routes(self) -> None:
        module = AuthnDepsModule(
            kernel=_kernel_min_access(),
            authn={"main": AuthnRouteCaps(bearer=True)},
        )

        deps = module()

        assert deps.exists(AuthnDepKey, route="main")

    def test_registers_all_families(self) -> None:
        module = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"a": AuthnRouteCaps(bearer=True)},
            token_lifecycle={"a"},
            password_lifecycle={"a"},
            api_key_lifecycle={"a"},
            password_account_provisioning={"a"},
        )

        deps = module()

        assert deps.exists(AuthnDepKey, route="a")
        assert deps.exists(TokenLifecycleDepKey, route="a")
        assert deps.exists(PasswordLifecycleDepKey, route="a")
        assert deps.exists(ApiKeyLifecycleDepKey, route="a")
        assert deps.exists(PasswordAccountProvisioningDepKey, route="a")

    def test_shared_password_service_across_authn_and_password_lifecycle(self) -> None:
        kernel = AuthnKernelConfig(
            access_token_secret=b"k" * 32,
            password=_slow_password_config(),
        )

        deps = (
            AuthnDepsModule(
                kernel=kernel,
                authn={"r": AuthnRouteCaps(password=True)},
                password_lifecycle={"r"},
            )()
            .merge(_document_deps())
        )

        ctx = ExecutionContext(deps=deps)

        auth = ctx.dep(AuthnDepKey, route="r")(ctx, AuthnSpec(name="r"))
        pl = ctx.dep(PasswordLifecycleDepKey, route="r")(ctx, AuthnSpec(name="r"))

        assert isinstance(auth, AuthnAdapter)
        assert isinstance(pl, PasswordLifecycleAdapter)
        assert auth.password_svc is pl.password_svc
        assert auth.password_svc is not None


class TestConfigurableFactories:
    def _shared_min(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_min_access())

    def _shared_full(self) -> AuthnSharedServices:
        return build_authn_shared_services(_kernel_full())

    def _ctx(self) -> ExecutionContext:
        merged = AuthnDepsModule()().merge(_document_deps())

        return ExecutionContext(deps=merged)

    def test_configurable_authn(self) -> None:
        ctx = self._ctx()

        factory = ConfigurableAuthn(
            shared=self._shared_min(),
            caps=AuthnRouteCaps(bearer=True),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, AuthnAdapter)

    def test_configurable_token_lifecycle(self) -> None:
        ctx = self._ctx()

        factory = ConfigurableTokenLifecycle(shared=self._shared_full())

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, TokenLifecycleAdapter)

    def test_configurable_password_lifecycle(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        shared = build_authn_shared_services(kernel)

        factory = ConfigurablePasswordLifecycle(shared=shared)

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordLifecycleAdapter)

    def test_configurable_api_key_lifecycle(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(api_key_pepper=b"a" * 32)
        shared = build_authn_shared_services(kernel)

        factory = ConfigurableApiKeyLifecycle(shared=shared)

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, ApiKeyLifecycleAdapter)

    def test_configurable_password_provisioning(self) -> None:
        ctx = self._ctx()

        kernel = AuthnKernelConfig(password=_slow_password_config())
        shared = build_authn_shared_services(kernel)

        factory = ConfigurablePasswordAccountProvisioning(shared=shared)

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordAccountProvisioningAdapter)

    def test_end_to_end_authn_dep_resolution(self) -> None:
        authn_part = AuthnDepsModule(
            kernel=_kernel_min_access(),
            authn={"main": AuthnRouteCaps(bearer=True)},
        )()
        merged = authn_part.merge(_document_deps())
        ctx = ExecutionContext(deps=merged)

        factory = ctx.dep(AuthnDepKey, route="main")
        port = factory(ctx, AuthnSpec(name="main"))

        assert isinstance(port, AuthnAdapter)
        assert port.access_svc is not None
