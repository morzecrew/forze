"""Factories for authn usecase registries."""

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import ExecutionContext, UsecaseRegistry
from forze.application.usecases.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)

from .operations import AuthnOperation

# ----------------------- #


def build_authn_registry(spec: AuthnSpec) -> UsecaseRegistry:
    """Build a usecase registry for the given :class:`AuthnSpec`.

    Each lambda resolves the matching dependency factories at runtime via the
    execution context, mirroring the document/search composition pattern.
    Operations whose underlying lifecycle is not registered for ``spec.name``
    will fail at call time with the standard missing-dep error from
    :class:`~forze.application.execution.context.ExecutionContext`; routes are
    expected to declare the lifecycle entries they intend to expose via
    :class:`~forze_authn.execution.deps.module.AuthnDepsModule`.

    :param spec: Authn specification (route name + enabled methods).
    :returns: Usecase registry with all authn operations.
    """

    def _password_login(ctx: ExecutionContext) -> AuthnPasswordLogin:
        return AuthnPasswordLogin(
            ctx=ctx,
            authn=ctx.dep(AuthnDepKey, route=spec.name)(ctx, spec),
            token_lifecycle=ctx.dep(TokenLifecycleDepKey, route=spec.name)(ctx, spec),
        )

    def _refresh_tokens(ctx: ExecutionContext) -> AuthnRefreshTokens:
        return AuthnRefreshTokens(
            ctx=ctx,
            token_lifecycle=ctx.dep(TokenLifecycleDepKey, route=spec.name)(ctx, spec),
        )

    def _logout(ctx: ExecutionContext) -> AuthnLogout:
        return AuthnLogout(
            ctx=ctx,
            token_lifecycle=ctx.dep(TokenLifecycleDepKey, route=spec.name)(ctx, spec),
        )

    def _change_password(ctx: ExecutionContext) -> AuthnChangePassword:
        return AuthnChangePassword(
            ctx=ctx,
            password_lifecycle=ctx.dep(PasswordLifecycleDepKey, route=spec.name)(
                ctx,
                spec,
            ),
        )

    return UsecaseRegistry(
        {
            AuthnOperation.PASSWORD_LOGIN: _password_login,
            AuthnOperation.REFRESH_TOKENS: _refresh_tokens,
            AuthnOperation.LOGOUT: _logout,
            AuthnOperation.CHANGE_PASSWORD: _change_password,
        }
    )
