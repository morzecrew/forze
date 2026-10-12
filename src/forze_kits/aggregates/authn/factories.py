"""Factories for authn usecase registries."""

import functools
import inspect
import types
from collections.abc import Iterable
from typing import Any

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PrincipalDeactivationDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.execution import BeforeStep, DeclaresAuthz
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace

from .dto import (
    AuthnApiKeyListDTO,
    AuthnChangePasswordRequestDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnIssuedApiKeyDTO,
    AuthnLoginRequestDTO,
    AuthnPasswordResetAckDTO,
    AuthnPrincipalRefDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    AuthnRevokeApiKeyRequestDTO,
    AuthnTokenResponseDTO,
)
from .handlers import (
    AuthnChangePassword,
    AuthnIssueApiKey,
    AuthnListApiKeys,
    AuthnListPrincipalApiKeys,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    AuthnRevokeApiKey,
    AuthnRevokePrincipalApiKey,
    DeactivatePrincipalHandler,
    DeactivatePrincipalRequestDTO,
)
from .operations import AuthnKernelOp

# ----------------------- #


def _declares_authorization(step: BeforeStep) -> bool:
    """Whether *step* declares the permission keys it enforces (``DeclaresAuthz``).

    Judged on the factory a ``functools.partial`` or ``__wrapped__`` wrapper stands for, so
    wrapping an ``AuthzBeforeAuthorize`` keeps what it declares.
    """

    factory: Any = inspect.unwrap(step.factory)

    while isinstance(factory, functools.partial):
        factory = inspect.unwrap(factory.func)

    if isinstance(factory, types.MethodType):
        factory = factory.__self__

    return isinstance(factory, DeclaresAuthz)


# ....................... #


def build_authn_registry(
    spec: AuthnSpec,
    *,
    ns: StrKeyNamespace | None = None,
    reset_events: OutboxSpec[Any] | None = None,
    admin_guards: Iterable[BeforeStep] = (),
    trust_admin_guards: bool = False,
) -> OperationRegistry:
    """Build authn operation registry.

    :param reset_events: Optional outbox route for the password-reset delivery
        seam. When set, ``request_password_reset`` stages an
        ``authn.password_reset_requested`` integration event (payload:
        ``login``, ``principal_id``, raw ``token``, ``expires_at``) for the app
        to relay to its notify/e-mail pipeline. The raw token transits the
        outbox row — see :mod:`forze_kits.aggregates.authn.events` for the
        exposure trade-off. When ``None`` and no custom delivery exists,
        requesting a reset mints a token nobody receives.
    :param admin_guards: Before-steps that decide who may act on *another* principal,
        typically ``AuthnRequired`` plus an ``AuthzBeforeAuthorize``; who that is is the
        app's authorization model, so Forze ships none. At least one must declare the
        permission keys it enforces (``permission_keys()``, the ``DeclaresAuthz`` marker),
        as ``AuthzBeforeAuthorize`` does, or the build is refused (``configuration``):
        ``AuthnRequired``, ``TenantRequired`` or a logging step admit every signed-in
        principal. An app's own step that declares its keys counts, and they show in the
        catalog and MCP tool descriptions; a ``functools.partial`` or ``__wrapped__``
        wrapper keeps what it wraps declares. Given, they register the operations that act
        on another principal, ``deactivate_principal``, ``list_principal_api_keys`` and
        ``revoke_principal_api_key``, behind them; without them none is registered, so no
        generated route or tool can reach one unguarded. The list is a query, so the
        guards run read-only there: one that writes fails it. All act globally
        (credential accounts are not tenant-scoped), so do not grant them to tenant-scoped
        administrators.
    :param trust_admin_guards: Skip that check, for a guard that authorizes without declaring
        its keys; the app then answers for ``admin_guards`` admitting only administrators.
    """

    ns = ns or spec.default_namespace
    # A generator is truthy even when it yields nothing: count the steps, not the object.
    admin_guards = tuple(admin_guards)

    if (
        admin_guards
        and not trust_admin_guards
        and not any(_declares_authorization(step) for step in admin_guards)
    ):
        raise exc.configuration(
            "admin_guards declare no authorization: without one, every signed-in principal "
            "can act on any principal. Add a step that declares its permission keys "
            "(permission_keys()), such as AuthzBeforeAuthorize or the app's own, or pass "
            "trust_admin_guards=True if one of the guards authorizes without declaring it.",
            code="admin_guards_unauthorized",
        )

    def _password_login(ctx: ExecutionContext) -> AuthnPasswordLogin:
        return AuthnPasswordLogin(
            authn=ctx.deps.resolve_configurable(ctx, AuthnDepKey, spec, route=spec.name),
            token_lifecycle=ctx.deps.resolve_configurable(
                ctx,
                TokenLifecycleDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _refresh_tokens(ctx: ExecutionContext) -> AuthnRefreshTokens:
        return AuthnRefreshTokens(
            token_lifecycle=ctx.deps.resolve_configurable(
                ctx,
                TokenLifecycleDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _logout(ctx: ExecutionContext) -> AuthnLogout:
        return AuthnLogout(
            resolver=ctx.inv_ctx.get_authn,
            token_lifecycle=ctx.deps.resolve_configurable(
                ctx,
                TokenLifecycleDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _change_password(ctx: ExecutionContext) -> AuthnChangePassword:
        return AuthnChangePassword(
            resolver=ctx.inv_ctx.get_authn,
            password_lifecycle=ctx.deps.resolve_configurable(
                ctx,
                PasswordLifecycleDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _request_password_reset(ctx: ExecutionContext) -> AuthnRequestPasswordReset:
        return AuthnRequestPasswordReset(
            password_reset=ctx.deps.resolve_configurable(
                ctx,
                PasswordResetDepKey,
                spec,
                route=spec.name,
            ),
            outbox=(ctx.outbox.command(reset_events) if reset_events is not None else None),
        )

    def _reset_password(ctx: ExecutionContext) -> AuthnResetPassword:
        return AuthnResetPassword(
            password_reset=ctx.deps.resolve_configurable(
                ctx,
                PasswordResetDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _deactivate_principal(ctx: ExecutionContext) -> DeactivatePrincipalHandler:
        return DeactivatePrincipalHandler(
            resolver=ctx.inv_ctx.get_authn,
            deactivation=ctx.deps.resolve_configurable(
                ctx,
                PrincipalDeactivationDepKey,
                spec,
                route=spec.name,
            ),
        )

    def _api_key_lifecycle(ctx: ExecutionContext) -> Any:
        return ctx.deps.resolve_configurable(
            ctx,
            ApiKeyLifecycleDepKey,
            spec,
            route=spec.name,
        )

    def _issue_api_key(ctx: ExecutionContext) -> AuthnIssueApiKey:
        return AuthnIssueApiKey(
            resolver=ctx.inv_ctx.get_authn,
            api_key_lifecycle=_api_key_lifecycle(ctx),
        )

    def _list_api_keys(ctx: ExecutionContext) -> AuthnListApiKeys:
        return AuthnListApiKeys(
            resolver=ctx.inv_ctx.get_authn,
            api_key_lifecycle=_api_key_lifecycle(ctx),
        )

    def _revoke_api_key(ctx: ExecutionContext) -> AuthnRevokeApiKey:
        return AuthnRevokeApiKey(
            resolver=ctx.inv_ctx.get_authn,
            api_key_lifecycle=_api_key_lifecycle(ctx),
        )

    def _list_principal_api_keys(ctx: ExecutionContext) -> AuthnListPrincipalApiKeys:
        return AuthnListPrincipalApiKeys(
            resolver=ctx.inv_ctx.get_authn,
            api_key_lifecycle=_api_key_lifecycle(ctx),
        )

    def _revoke_principal_api_key(ctx: ExecutionContext) -> AuthnRevokePrincipalApiKey:
        return AuthnRevokePrincipalApiKey(
            resolver=ctx.inv_ctx.get_authn,
            api_key_lifecycle=_api_key_lifecycle(ctx),
        )

    reg = OperationRegistry(
        handlers={
            ns.key(AuthnKernelOp.PASSWORD_LOGIN): _password_login,
            ns.key(AuthnKernelOp.REFRESH_TOKENS): _refresh_tokens,
            ns.key(AuthnKernelOp.LOGOUT): _logout,
            ns.key(AuthnKernelOp.CHANGE_PASSWORD): _change_password,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET): _request_password_reset,
            ns.key(AuthnKernelOp.RESET_PASSWORD): _reset_password,
            ns.key(AuthnKernelOp.ISSUE_API_KEY): _issue_api_key,
            ns.key(AuthnKernelOp.LIST_API_KEYS): _list_api_keys,
            ns.key(AuthnKernelOp.REVOKE_API_KEY): _revoke_api_key,
        },
    )

    # All authn operations mutate auth state (issue/rotate/revoke tokens) — kept COMMAND.
    reg = reg.set_descriptors(
        {
            AuthnKernelOp.PASSWORD_LOGIN: OperationDescriptor(
                input_type=AuthnLoginRequestDTO,
                output_type=AuthnTokenResponseDTO,
                description="Authenticate with password credentials and issue a token pair.",
            ),
            AuthnKernelOp.REFRESH_TOKENS: OperationDescriptor(
                input_type=AuthnRefreshRequestDTO,
                output_type=AuthnTokenResponseDTO,
                description="Rotate a refresh token into a fresh access/refresh pair.",
            ),
            AuthnKernelOp.LOGOUT: OperationDescriptor(
                description="Revoke all sessions for the authenticated identity.",
            ),
            AuthnKernelOp.CHANGE_PASSWORD: OperationDescriptor(
                input_type=AuthnChangePasswordRequestDTO,
                description="Change the password of the authenticated identity.",
            ),
            AuthnKernelOp.REQUEST_PASSWORD_RESET: OperationDescriptor(
                input_type=AuthnRequestPasswordResetDTO,
                output_type=AuthnPasswordResetAckDTO,
                description=(
                    "Request a self-service password reset for a login; the "
                    "response is a uniform acknowledgment regardless of "
                    "whether the login exists (no account enumeration)."
                ),
            ),
            AuthnKernelOp.RESET_PASSWORD: OperationDescriptor(
                input_type=AuthnResetPasswordDTO,
                description=(
                    "Consume a single-use reset token and set a new password; "
                    "all of the principal's sessions are revoked."
                ),
            ),
            AuthnKernelOp.ISSUE_API_KEY: OperationDescriptor(
                input_type=AuthnIssueApiKeyRequestDTO,
                output_type=AuthnIssuedApiKeyDTO,
                description=(
                    "Issue an API key for the authenticated identity; the secret "
                    "is returned once. Optionally a user→agent delegation key."
                ),
            ),
            AuthnKernelOp.LIST_API_KEYS: OperationDescriptor(
                output_type=AuthnApiKeyListDTO,
                description=(
                    "List the authenticated identity's API keys "
                    "(non-secret descriptors — never the key or its hash)."
                ),
            ),
            AuthnKernelOp.REVOKE_API_KEY: OperationDescriptor(
                input_type=AuthnRevokeApiKeyRequestDTO,
                description="Revoke one of the authenticated identity's API keys.",
            ),
        },
        namespace=ns,
    )

    # Acting on another principal is registered only behind the app's guards, so no
    # generated route, MCP tool or agent tool can reach it unguarded. The admin listing is
    # a read, classified QUERY as the self-service one is.
    if admin_guards:
        admin_handlers = {
            ns.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL): _deactivate_principal,
            ns.key(AuthnKernelOp.LIST_PRINCIPAL_API_KEYS): _list_principal_api_keys,
            ns.key(AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY): _revoke_principal_api_key,
        }
        admin = OperationRegistry(handlers=admin_handlers).set_descriptors(
            {
                AuthnKernelOp.DEACTIVATE_PRINCIPAL: OperationDescriptor(
                    input_type=DeactivatePrincipalRequestDTO,
                    description=(
                        "Deactivate a principal for the application (policy, sessions, credentials)."
                    ),
                ),
                AuthnKernelOp.LIST_PRINCIPAL_API_KEYS: OperationDescriptor(
                    input_type=AuthnPrincipalRefDTO,
                    output_type=AuthnApiKeyListDTO,
                    description=(
                        "List any principal's API keys (admin; non-secret descriptors, "
                        "revoked ones included)."
                    ),
                ),
                AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY: OperationDescriptor(
                    input_type=AuthnRevokeApiKeyRequestDTO,
                    description="Revoke any principal's API key (admin).",
                ),
            },
            namespace=ns,
        )
        reg = OperationRegistry.merge(
            reg,
            admin.bind(*admin_handlers).bind_outer().before(*admin_guards).finish(deep=True),
        )
        reg = reg.bind(ns.key(AuthnKernelOp.LIST_PRINCIPAL_API_KEYS)).as_query().finish()

    # ``list_api_keys`` is a read (no mutation) — classify it QUERY, and require a
    # bound principal (self-service: you list your own keys).
    reg = (
        reg.bind(ns.key(AuthnKernelOp.LIST_API_KEYS))
        .as_query()
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )

    # ``logout``/``change_password`` and the API-key issue/revoke ops act on the
    # *current* identity, so they require a bound principal. Declaring it as a hook
    # (rather than only the handler's own guard) makes the requirement
    # introspectable: the catalog flags ``requires_authn``, which the FastAPI/MCP
    # surfaces project into their auth descriptions. The 401 (``auth_required``) is
    # unchanged. Login/refresh and the reset pair authenticate via their bodies (no
    # bound principal); the admin operations exist only behind ``admin_guards``.
    return (
        reg.bind(
            ns.key(AuthnKernelOp.LOGOUT),
            ns.key(AuthnKernelOp.CHANGE_PASSWORD),
            ns.key(AuthnKernelOp.ISSUE_API_KEY),
            ns.key(AuthnKernelOp.REVOKE_API_KEY),
        )
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )
