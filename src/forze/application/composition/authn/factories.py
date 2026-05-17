"""Factories for authn usecase registries."""

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import (
    ExecutionContext,
    OperationNamespace,
    UsecaseRegistry,
    operation_namespace_for,
)
from forze.application.usecases.authn import (
    AuthnChangePassword,
    AuthnLogout,
    AuthnPasswordLogin,
    AuthnRefreshTokens,
)

from .operations import AuthnKernelOp

# ----------------------- #


def build_authn_registry(
    spec: AuthnSpec,
    *,
    namespace: OperationNamespace | None = None,
) -> UsecaseRegistry:
    """Build a usecase registry for the given :class:`AuthnSpec`.

    Each lambda resolves the matching dependency factories at runtime via the
    execution context, mirroring the document/search composition pattern.
    Operations whose underlying lifecycle is not registered for ``spec.name``
    will fail at call time with the standard missing-dep error from
    :class:`~forze.application.execution.context.ExecutionContext`; routes are
    expected to declare the lifecycle entries they intend to expose via
    :class:`~forze_authn.execution.deps.module.AuthnDepsModule`.

    :param spec: Authn specification (route name + enabled methods).
    :param namespace: Operation namespace; defaults to :func:`operation_namespace_for` ``(spec)``.
    :returns: Usecase registry with all authn operations.
    """

    ops = namespace or operation_namespace_for(spec)

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
            AuthnKernelOp.PASSWORD_LOGIN: _password_login,
            AuthnKernelOp.REFRESH_TOKENS: _refresh_tokens,
            AuthnKernelOp.LOGOUT: _logout,
            AuthnKernelOp.CHANGE_PASSWORD: _change_password,
        },
        namespace=ops,
    )
