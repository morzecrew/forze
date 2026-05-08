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
from forze_authnz.authn.adapters import (
    ApiKeyLifecycleAdapter,
    AuthnAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from forze_authnz.authn.application.constants import AuthnResourceName
from forze_authnz.authn.execution import (
    ApiKeyLifecycleRouteConfig,
    AuthnDepsModule,
    AuthnRouteConfig,
    ConfigurableApiKeyLifecycle,
    ConfigurableAuthn,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
    PasswordLifecycleRouteConfig,
    PasswordProvisioningRouteConfig,
    TokenLifecycleRouteConfig,
)
from forze_authnz.authn.services import (
    AccessTokenService,
    ApiKeyService,
    PasswordService,
    RefreshTokenService,
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

    def test_rejects_empty_authn_route_config(self) -> None:
        with pytest.raises(ValueError, match="password_svc"):
            AuthnDepsModule(
                authn={"main": AuthnRouteConfig()},
            )()

    def test_registers_authn_routes(self) -> None:
        access = AccessTokenService(secret_key=b"k" * 32)
        module = AuthnDepsModule(
            authn={"main": AuthnRouteConfig(access_svc=access)},
        )

        deps = module()

        assert deps.exists(AuthnDepKey, route="main")

    def test_registers_all_families(self) -> None:
        access = AccessTokenService(secret_key=b"k" * 32)
        refresh = RefreshTokenService(pepper=b"p" * 32)
        password = PasswordService()
        api_key = ApiKeyService(pepper=b"a" * 32)

        module = AuthnDepsModule(
            authn={"a": AuthnRouteConfig(access_svc=access)},
            token_lifecycle={"a": TokenLifecycleRouteConfig(access_svc=access, refresh_svc=refresh)},
            password_lifecycle={"a": PasswordLifecycleRouteConfig(password_svc=password)},
            api_key_lifecycle={"a": ApiKeyLifecycleRouteConfig(api_key_svc=api_key)},
            password_account_provisioning={
                "a": PasswordProvisioningRouteConfig(password_svc=password),
            },
        )

        deps = module()

        assert deps.exists(AuthnDepKey, route="a")
        assert deps.exists(TokenLifecycleDepKey, route="a")
        assert deps.exists(PasswordLifecycleDepKey, route="a")
        assert deps.exists(ApiKeyLifecycleDepKey, route="a")
        assert deps.exists(PasswordAccountProvisioningDepKey, route="a")


class TestConfigurableFactories:
    def _ctx(self) -> ExecutionContext:
        merged = AuthnDepsModule()().merge(_document_deps())

        return ExecutionContext(deps=merged)

    def test_configurable_authn(self) -> None:
        ctx = self._ctx()
        access = AccessTokenService(secret_key=b"k" * 32)

        factory = ConfigurableAuthn(
            config=AuthnRouteConfig(access_svc=access),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, AuthnAdapter)

    def test_configurable_token_lifecycle(self) -> None:
        ctx = self._ctx()
        access = AccessTokenService(secret_key=b"k" * 32)
        refresh = RefreshTokenService(pepper=b"p" * 32)

        factory = ConfigurableTokenLifecycle(
            config=TokenLifecycleRouteConfig(access_svc=access, refresh_svc=refresh),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, TokenLifecycleAdapter)

    def test_configurable_password_lifecycle(self) -> None:
        ctx = self._ctx()
        password = PasswordService()

        factory = ConfigurablePasswordLifecycle(
            config=PasswordLifecycleRouteConfig(password_svc=password),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordLifecycleAdapter)

    def test_configurable_api_key_lifecycle(self) -> None:
        ctx = self._ctx()
        api_key = ApiKeyService(pepper=b"a" * 32)

        factory = ConfigurableApiKeyLifecycle(
            config=ApiKeyLifecycleRouteConfig(api_key_svc=api_key),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, ApiKeyLifecycleAdapter)

    def test_configurable_password_provisioning(self) -> None:
        ctx = self._ctx()
        password = PasswordService()

        factory = ConfigurablePasswordAccountProvisioning(
            config=PasswordProvisioningRouteConfig(password_svc=password),
        )

        port = factory(ctx, AuthnSpec(name="s"))

        assert isinstance(port, PasswordAccountProvisioningAdapter)

    def test_end_to_end_authn_dep_resolution(self) -> None:
        access = AccessTokenService(secret_key=b"k" * 32)
        authn_part = AuthnDepsModule(
            authn={"main": AuthnRouteConfig(access_svc=access)},
        )()
        merged = authn_part.merge(_document_deps())
        ctx = ExecutionContext(deps=merged)

        factory = ctx.dep(AuthnDepKey, route="main")
        port = factory(ctx, AuthnSpec(name="main"))

        assert isinstance(port, AuthnAdapter)
        assert port.access_svc is access
