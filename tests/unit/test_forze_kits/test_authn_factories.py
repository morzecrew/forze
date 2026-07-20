"""Tests for :mod:`forze_kits.aggregates.authn.factories`."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnResult,
    AuthnSpec,
    IssuedAccessToken,
    IssuedRefreshToken,
    IssuedTokens,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PrincipalDeactivationDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.authn.value_objects import (
    AccessTokenCredentials,
    AuthnIdentity,
    CredentialLifetime,
    RefreshTokenCredentials,
)
from forze.application.execution import ExecutionContext
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.authn import AuthnKernelOp, build_authn_registry
from forze_kits.aggregates.authn.factories import build_authn_registry as build_registry
from forze_kits.aggregates.authn.handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    DeactivatePrincipalHandler,
)

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
    password_reset = AsyncMock()
    password_reset.request_reset = AsyncMock(return_value=None)
    password_reset.reset_password = AsyncMock(return_value=None)
    principal_deactivation = AsyncMock()
    principal_deactivation.deactivate = AsyncMock(return_value=None)

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
        if key is PasswordResetDepKey:
            return password_reset
        if key is PrincipalDeactivationDepKey:
            return principal_deactivation
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
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET))
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.RESET_PASSWORD))
        assert registry_has_handler(reg, ns.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL))

    def test_catalog_has_descriptor_for_every_op(self) -> None:
        spec = _authn_spec()
        frozen = build_authn_registry(spec).freeze()
        catalog = frozen.catalog()
        ns = spec.default_namespace
        assert set(catalog) == {ns.key(op) for op in AuthnKernelOp}
        for entry in catalog.values():
            assert entry.descriptor is not None

    def test_self_service_ops_require_authn(self) -> None:
        # Ops that act on the current identity declare AuthnRequired, so the catalog
        # flags them (FastAPI/MCP project it into auth surfaces). Body-authenticated
        # flows and the unguarded deactivate stay unflagged.
        spec = _authn_spec()
        catalog = build_authn_registry(spec).freeze().catalog()
        ns = spec.default_namespace

        flagged = {
            op.value
            for op in AuthnKernelOp
            if catalog[ns.key(op)].requires_authn
        }

        assert flagged == {
            "logout",
            "change_password",
            "issue_api_key",
            "list_api_keys",
            "revoke_api_key",
        }

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

    @pytest.mark.asyncio
    async def test_deactivate_principal_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(reg, spec.default_namespace.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL))
        handler = factory(_mock_ctx())
        assert isinstance(handler, DeactivatePrincipalHandler)

    @pytest.mark.asyncio
    async def test_request_password_reset_factory_without_outbox(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(
            reg,
            spec.default_namespace.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
        )
        handler = factory(_mock_ctx())
        assert isinstance(handler, AuthnRequestPasswordReset)
        # No reset_events spec → no outbox staging wired.
        assert handler.outbox is None

    @pytest.mark.asyncio
    async def test_request_password_reset_factory_wires_reset_events_outbox(
        self,
    ) -> None:
        spec = _authn_spec()
        outbox_spec = MagicMock()
        reg = build_registry(spec, reset_events=outbox_spec)
        factory = handler_at(
            reg,
            spec.default_namespace.key(AuthnKernelOp.REQUEST_PASSWORD_RESET),
        )

        ctx = _mock_ctx()
        outbox_port = MagicMock()
        ctx.outbox.command = MagicMock(return_value=outbox_port)

        handler = factory(ctx)
        assert isinstance(handler, AuthnRequestPasswordReset)
        ctx.outbox.command.assert_called_once_with(outbox_spec)
        assert handler.outbox is outbox_port

    @pytest.mark.asyncio
    async def test_reset_password_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec)
        factory = handler_at(
            reg,
            spec.default_namespace.key(AuthnKernelOp.RESET_PASSWORD),
        )
        handler = factory(_mock_ctx())
        assert isinstance(handler, AuthnResetPassword)
