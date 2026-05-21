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

    ns = ns or StrKeyNamespace(prefix=spec.name)

    def _password_login(ctx: ExecutionContext) -> AuthnPasswordLogin:
        return AuthnPasswordLogin(
            authn=ctx.deps.provide(AuthnDepKey, route=spec.name)(ctx, spec),
            token_lifecycle=ctx.deps.provide(TokenLifecycleDepKey, route=spec.name)(
                ctx, spec
            ),
        )

    def _refresh_tokens(ctx: ExecutionContext) -> AuthnRefreshTokens:
        return AuthnRefreshTokens(
            token_lifecycle=ctx.deps.provide(TokenLifecycleDepKey, route=spec.name)(
                ctx, spec
            ),
        )

    def _logout(ctx: ExecutionContext) -> AuthnLogout:
        return AuthnLogout(
            resolver=ctx.inv.get_authn,
            token_lifecycle=ctx.deps.provide(TokenLifecycleDepKey, route=spec.name)(
                ctx, spec
            ),
        )

    def _change_password(ctx: ExecutionContext) -> AuthnChangePassword:
        return AuthnChangePassword(
            resolver=ctx.inv.get_authn,
            password_lifecycle=ctx.deps.provide(
                PasswordLifecycleDepKey, route=spec.name
            )(
                ctx,
                spec,
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
