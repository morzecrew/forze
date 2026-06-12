"""Factories for authn usecase registries."""

from typing import Any

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PrincipalDeactivationDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from .dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnPasswordResetAckDTO,
    AuthnRefreshRequestDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
    AuthnTokenResponseDTO,
)
from .handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
    AuthnRequestPasswordReset,
    AuthnResetPassword,
    DeactivatePrincipalHandler,
    DeactivatePrincipalRequestDTO,
)
from forze.base.primitives import StrKeyNamespace

from .operations import AuthnKernelOp

# ----------------------- #


def build_authn_registry(
    spec: AuthnSpec,
    *,
    ns: StrKeyNamespace | None = None,
    reset_events: OutboxSpec[Any] | None = None,
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
    """

    ns = ns or spec.default_namespace

    def _password_login(ctx: ExecutionContext) -> AuthnPasswordLogin:
        return AuthnPasswordLogin(
            authn=ctx.deps.resolve_configurable(
                ctx, AuthnDepKey, spec, route=spec.name
            ),
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
            outbox=(
                ctx.outbox.command(reset_events)
                if reset_events is not None
                else None
            ),
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
            deactivation=ctx.deps.resolve_configurable(
                ctx,
                PrincipalDeactivationDepKey,
                spec,
                route=spec.name,
            ),
        )

    reg = OperationRegistry(
        handlers={
            ns.key(AuthnKernelOp.PASSWORD_LOGIN): _password_login,
            ns.key(AuthnKernelOp.REFRESH_TOKENS): _refresh_tokens,
            ns.key(AuthnKernelOp.LOGOUT): _logout,
            ns.key(AuthnKernelOp.CHANGE_PASSWORD): _change_password,
            ns.key(AuthnKernelOp.REQUEST_PASSWORD_RESET): _request_password_reset,
            ns.key(AuthnKernelOp.RESET_PASSWORD): _reset_password,
            ns.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL): _deactivate_principal,
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
            AuthnKernelOp.DEACTIVATE_PRINCIPAL: OperationDescriptor(
                input_type=DeactivatePrincipalRequestDTO,
                description=(
                    "Deactivate a principal for the application "
                    "(policy, sessions, credentials)."
                ),
            ),
        },
        namespace=ns,
    )

    # ``logout`` and ``change_password`` act on the *current* identity, so they
    # require a bound principal. Declaring it as a hook (rather than only the
    # handler's own guard) makes the requirement introspectable: the catalog
    # flags ``requires_authn``, which the FastAPI/MCP surfaces project into their
    # auth descriptions. The 401 (``auth_required``) is unchanged. Login/refresh
    # and the reset pair authenticate via their bodies (no bound principal);
    # ``deactivate_principal`` ships unguarded by design (apps bind authn+authz).
    return (
        reg.bind(
            ns.key(AuthnKernelOp.LOGOUT),
            ns.key(AuthnKernelOp.CHANGE_PASSWORD),
        )
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )
