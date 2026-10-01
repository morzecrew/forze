"""Gate a FastAPI route on a permission, with the authz hook's own decision and denial."""

from collections.abc import Awaitable, Callable

from forze.application.contracts.authz import AuthzResource, AuthzSpec
from forze.application.execution.context import ExecutionContextFactory
from forze.application.hooks.authz import authorize_action
from forze.base.exceptions import exc

# ----------------------- #


def require_permission(
    key: str,
    *,
    spec: AuthzSpec,
    ctx_dep: ExecutionContextFactory,
    resource_type: str | None = None,
) -> Callable[[], Awaitable[None]]:
    """A FastAPI dependency refusing the request unless the caller holds *key*.

    For a route that runs no operation, so no authz hook guards it:
    ``@app.post("/ledger", dependencies=[Depends(require_permission("ledger.write", ...))])``.
    It makes the decision the :class:`~forze.application.hooks.authz.AuthzBeforeAuthorize` hook
    makes — derived and config grants included, every actor of a delegated call checked too, and
    ``may_act`` when *spec* enforces delegation grants — and raises the same denial.

    Gate on permissions, never on roles: a provider's denial can close a permission, not a role.

    :param key: The permission key the caller must hold.
    :param spec: The authz spec whose decision port answers.
    :param ctx_dep: The per-request execution context factory, as the route builders take it.
    :param resource_type: The type the route is about. Its denial then names that type, so a
        non-disclosing :class:`~forze.base.exceptions.DenialPosture` covering it renders the
        denial as that type's not-found; without it a denial is an action-level 403.
    :raises CoreException: ``configuration`` for a blank *key*; per request, what
        :func:`~forze.application.hooks.authz.authorize_action` raises.
    """

    if not key.strip():
        raise exc.configuration("require_permission needs a permission key.")

    resource = AuthzResource(resource_type=resource_type) if resource_type is not None else None

    async def dependency() -> None:
        ctx = ctx_dep()

        await authorize_action(
            ctx,
            ctx.authz.decision(spec),
            key,
            delegation_port=ctx.authz.delegation(spec) if spec.enforce_delegation_grant else None,
            resource=resource,
        )

    return dependency
