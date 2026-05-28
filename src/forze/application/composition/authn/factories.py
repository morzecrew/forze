"""Factories for authn usecase registries."""

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
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

    return OperationRegistry(
        handlers={
            ns.key(AuthnKernelOp.PASSWORD_LOGIN): _password_login,
            ns.key(AuthnKernelOp.REFRESH_TOKENS): _refresh_tokens,
            ns.key(AuthnKernelOp.LOGOUT): _logout,
            ns.key(AuthnKernelOp.CHANGE_PASSWORD): _change_password,
        },
    )
