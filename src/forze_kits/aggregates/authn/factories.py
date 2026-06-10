"""Factories for authn usecase registries."""

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    PrincipalDeactivationDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from .dto import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)
from .handlers import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
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
) -> OperationRegistry:
    """Build authn operation registry."""

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
            ns.key(AuthnKernelOp.DEACTIVATE_PRINCIPAL): _deactivate_principal,
        },
    )

    # All authn operations mutate auth state (issue/rotate/revoke tokens) — kept COMMAND.
    return reg.set_descriptors(
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
