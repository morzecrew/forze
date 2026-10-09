"""Tests for :mod:`forze_kits.aggregates.authn.factories`."""

from __future__ import annotations

import functools
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
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
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.execution import BeforeFactory, BeforeStep
from forze.application.execution import ExecutionContext
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.application.hooks.tenancy import TenantRequired
from forze.base.exceptions import CoreException, ExceptionKind
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
    AuthnRevokePrincipalApiKey,
    DeactivatePrincipalHandler,
)

from .registry_helpers import handler_at, registry_has_handler

# ----------------------- #


class _AppAuthorization:
    """An app's own authorization step: the kind of guard Forze cannot recognise."""

    def __call__(self, ctx: object) -> object:
        async def _allow(args: object) -> None:
            _ = args

        return _allow


class _ForzeShippedStep:
    """Stands for a before-step factory Forze itself ships."""

    __module__ = "forze_kits.aggregates.example"

    def __call__(self, ctx: object) -> object:
        return _AppAuthorization()(ctx)


class _AppAuthn(AuthnRequired):  # type: ignore[misc]
    """An app's subclass of a Forze step that still only authenticates."""


class _AppTenant(TenantRequired):  # type: ignore[misc]
    """An app's subclass of a Forze step that still only requires a tenant."""


class _AppGuard(BeforeFactory):
    """An app's own step written against Forze's ``BeforeFactory`` protocol."""

    def __call__(self, ctx: object) -> object:
        return _AppAuthorization()(ctx)


class _LookalikeAppStep(_AppAuthorization):
    """An app's own step in a package whose name merely starts with ``forze``."""

    __module__ = "forzeful_app.guards"


class _GuardedFn:
    """A decorator-style wrapper exposing the step it wraps as ``__wrapped__``."""

    def __init__(self, wrapped: object) -> None:
        self.__wrapped__ = wrapped

    def __call__(self, ctx: object) -> object:
        return self.__wrapped__(ctx)  # type: ignore[operator]


def _authorize() -> AuthzBeforeAuthorize:
    return AuthzBeforeAuthorize(spec=AuthzSpec(name="api"), action="principals:admin")


def _admin_guards() -> tuple[BeforeStep, ...]:
    return AuthnRequired().to_step(), BeforeStep(id="app.admin", factory=_AppAuthorization())


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
        if key is ApiKeyLifecycleDepKey:
            return AsyncMock()
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

    def test_catalog_has_descriptor_for_every_op(self) -> None:
        spec = _authn_spec()
        frozen = build_authn_registry(spec, admin_guards=_admin_guards()).freeze()
        catalog = frozen.catalog()
        ns = spec.default_namespace
        assert set(catalog) == {ns.key(op) for op in AuthnKernelOp}
        for entry in catalog.values():
            assert entry.descriptor is not None

    @pytest.mark.parametrize(
        "op",
        [
            AuthnKernelOp.DEACTIVATE_PRINCIPAL,
            AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY,
            AuthnKernelOp.LIST_PRINCIPAL_API_KEYS,
        ],
    )
    def test_an_admin_operation_exists_only_behind_the_apps_guards(
        self, op: AuthnKernelOp
    ) -> None:
        # An act on another principal is never registered unguarded, so no generated
        # surface can reach it before the app has said who may.
        spec = _authn_spec()
        key = spec.default_namespace.key(op)

        assert not registry_has_handler(build_authn_registry(spec), key)

        # An iterable a filter emptied is truthy as a generator, and still no guard.
        emptied = (step for step in (AuthnRequired().to_step(),) if False)
        assert not registry_has_handler(build_authn_registry(spec, admin_guards=emptied), key)

        guarded = build_authn_registry(spec, admin_guards=_admin_guards())
        befores = {str(step.id) for step in guarded.get_plans()[key].iter_before_steps()}

        assert guarded.freeze().catalog()[key].requires_authn
        assert {"authn.principal", "app.admin"} <= befores

    @pytest.mark.parametrize("step_id", [None, "app.signed_in"])
    def test_authentication_alone_does_not_guard_an_admin_operation(
        self, step_id: str | None
    ) -> None:
        # ``AuthnRequired`` admits every signed-in principal, under whatever step id it runs.
        authn = AuthnRequired()
        steps = (authn.to_step(),) if step_id is None else (authn.to_step(step_id=step_id),)

        with pytest.raises(CoreException) as caught:
            build_authn_registry(_authn_spec(), admin_guards=steps)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "every signed-in principal" in str(caught.value)

    @pytest.mark.parametrize(
        "guards",
        [
            pytest.param(
                lambda: (AuthnRequired().to_step(), TenantRequired().to_step(step_id="t")),
                id="authn+tenant",
            ),
            pytest.param(lambda: (TenantRequired().to_step(step_id="t"),), id="tenant"),
            # A step Forze ships that authorizes nothing, wherever under ``forze*`` it lives.
            pytest.param(
                lambda: (BeforeStep(id="kit", factory=_ForzeShippedStep()),), id="forze-kit"
            ),
            # Wrapped or subclassed, a Forze step still only authenticates or scopes.
            pytest.param(
                lambda: (
                    AuthnRequired().to_step(),
                    BeforeStep(id="t", factory=functools.partial(TenantRequired())),
                ),
                id="partial-tenant",
            ),
            pytest.param(
                lambda: (
                    BeforeStep(
                        id="p",
                        factory=functools.partial(functools.partial(AuthnRequired())),
                    ),
                ),
                id="partial-of-partial",
            ),
            pytest.param(
                lambda: (BeforeStep(id="w", factory=_GuardedFn(TenantRequired())),),
                id="wrapped-tenant",
            ),
            pytest.param(lambda: (_AppAuthn().to_step(),), id="app-subclass-authn"),
            pytest.param(
                lambda: (AuthnRequired().to_step(), _AppTenant().to_step(step_id="t")),
                id="app-subclass-tenant",
            ),
        ],
    )
    def test_guards_that_authorize_nothing_are_refused(self, guards: Any) -> None:
        with pytest.raises(CoreException) as caught:
            build_authn_registry(_authn_spec(), admin_guards=guards())

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "authorize nothing" in str(caught.value)

    @pytest.mark.parametrize(
        "guards",
        [
            pytest.param(
                lambda: (BeforeStep(id="app.admin", factory=_AppAuthorization()),), id="app"
            ),
            pytest.param(
                lambda: (BeforeStep(id="app.admin", factory=lambda ctx: None),), id="app-fn"
            ),
            # ``forze`` and ``forze_*`` are Forze's packages; ``forzeful_app`` is not.
            pytest.param(
                lambda: (BeforeStep(id="app.admin", factory=_LookalikeAppStep()),),
                id="forze-lookalike-package",
            ),
            # Implementing Forze's protocol does not make a step Forze's.
            pytest.param(
                lambda: (BeforeStep(id="app.admin", factory=_AppGuard()),), id="app-protocol"
            ),
            pytest.param(
                lambda: (BeforeStep(id="app.admin", factory=functools.partial(_AppAuthorization())),),
                id="partial-app",
            ),
            pytest.param(lambda: (AuthnRequired().to_step(), _authorize().to_step()), id="authn+authz"),
            pytest.param(
                lambda: (
                    AuthnRequired().to_step(),
                    TenantRequired().to_step(step_id="t"),
                    _authorize().to_step(),
                ),
                id="authn+tenant+authz",
            ),
        ],
    )
    def test_a_step_that_can_authorize_guards_it(self, guards: Any) -> None:
        spec = _authn_spec()
        catalog = build_authn_registry(spec, admin_guards=guards()).freeze().catalog()

        assert spec.default_namespace.key(AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY) in catalog

    @pytest.mark.asyncio
    async def test_admin_revoke_factory_returns_handler(self) -> None:
        spec = _authn_spec()
        reg = build_authn_registry(spec, admin_guards=_admin_guards())
        factory = handler_at(
            reg, spec.default_namespace.key(AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY)
        )
        handler = factory(_mock_ctx())
        assert isinstance(handler, AuthnRevokePrincipalApiKey)

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
            if ns.key(op) in catalog and catalog[ns.key(op)].requires_authn
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
        reg = build_authn_registry(spec, admin_guards=_admin_guards())
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
