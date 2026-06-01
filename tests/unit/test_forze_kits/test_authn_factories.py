"""Tests for :mod:`forze_kits.aggregates.authn.factories`."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze_kits.aggregates.authn import AuthnKernelOp, build_authn_registry
from forze_kits.aggregates.authn.factories import build_authn_registry as build_registry
from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnResult,
    AuthnSpec,
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.authn.value_objects import (
    AccessTokenCredentials,
    AuthnIdentity,
    CredentialLifetime,
    RefreshTokenCredentials,
)
from forze.application.execution import ExecutionContext
from forze.application.handlers.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)
from forze.base.primitives import StrKeyNamespace

from .registry_helpers import handler_at, registry_has_handler

# ----------------------- #


def _authn_spec() -> AuthnSpec:
    return AuthnSpec(name="app", enabled_methods=frozenset({"password", "token"}))


def _issued_tokens() -> IssuedTokens:
    return IssuedTokens(
        access=IssuedAccessToken(
            token=AccessTokenCredentials(token="access"),
            lifetime=CredentialLifetime(expires_in=timedelta(seconds=60)),
        ),
        refresh=IssuedRefreshToken(
            token=RefreshTokenCredentials(token="refresh"),
            lifetime=CredentialLifetime(expires_in=timedelta(seconds=120)),
        ),
    )


def _mock_ctx(
    *,
    identity: AuthnIdentity | None = None,
) -> ExecutionContext:
    authn_port = AsyncMock()
    authn_port.authenticate_with_password = AsyncMock(
        return_value=AuthnResult(identity=identity or AuthnIdentity(principal_id=uuid4())),
    )
    token_lifecycle = AsyncMock()
    token_lifecycle.issue_tokens = AsyncMock(return_value=_issued_tokens())
    token_lifecycle.refresh_tokens = AsyncMock(return_value=_issued_tokens())
    token_lifecycle.revoke_tokens = AsyncMock(return_value=None)
    password_lifecycle = AsyncMock()
    password_lifecycle.change_password = AsyncMock(return_value=None)

    deps = MagicMock()

    def resolve_configurable(
        _ctx: ExecutionContext,
        key: object,
        _spec: AuthnSpec,
        *,
        route: str | None = None,
    ) -> object:
        _ = route
        if key is AuthnDepKey:
            return authn_port
        if key is TokenLifecycleDepKey:
            return token_lifecycle
        if key is PasswordLifecycleDepKey:
            return password_lifecycle
        raise AssertionError(f"unexpected key {key!r}")

    deps.resolve_configurable = resolve_configurable
    ctx = MagicMock(spec=ExecutionContext)
    ctx.deps = deps
    ctx.inv_ctx.get_authn = MagicMock(return_value=identity)
    return ctx


class TestBuildAuthnRegistry:
    def test_registers_all_kernel_ops(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.PASSWORD_LOGIN))
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.REFRESH_TOKENS))
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.LOGOUT))
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.CHANGE_PASSWORD))

    def test_custom_namespace(self) -> None:
        spec = _authn_spec()
        custom = StrKeyNamespace(prefix="tenant_auth")
        reg = build_registry(spec, ns=custom)
        assert registry_has_handler(reg, custom.key(AuthnKernelOp.PASSWORD_LOGIN))
        assert not registry_has_handler(reg, spec.default_namespace.key(AuthnKernelOp.PASSWORD_LOGIN))

    @pytest.mark.asyncio
    async def test_password_login_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(reg, spec.default_namespace.key(AuthnKernelOp.PASSWORD_LOGIN))
        handler = factory(_mock_ctx())
        assert isinstance(handler, AuthnPasswordLogin)

    @pytest.mark.asyncio
    async def test_refresh_tokens_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(reg, spec.default_namespace.key(AuthnKernelOp.REFRESH_TOKENS))
        handler = factory(_mock_ctx())
        assert isinstance(handler, AuthnRefreshTokens)

    @pytest.mark.asyncio
    async def test_logout_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(reg, spec.default_namespace.key(AuthnKernelOp.LOGOUT))
        handler = factory(_mock_ctx(identity=AuthnIdentity(principal_id=uuid4())))
        assert isinstance(handler, AuthnLogout)

    @pytest.mark.asyncio
    async def test_change_password_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(reg, spec.default_namespace.key(AuthnKernelOp.CHANGE_PASSWORD))
        handler = factory(_mock_ctx(identity=AuthnIdentity(principal_id=uuid4())))
        assert isinstance(handler, AuthnChangePassword)
