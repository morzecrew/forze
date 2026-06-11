"""Generated FastAPI routes for authn flows.

Projects the authn operations of a frozen registry (built with
:func:`forze_kits.aggregates.authn.build_authn_registry`) onto a user-owned
:class:`fastapi.APIRouter`. Authn flows are RPC-shaped commands with exactly one
natural HTTP surface each — there is no resource to address, so there is no
style choice: every operation is a ``POST`` on a fixed action path (``/login``,
``/refresh``, ``/logout``, ``/change-password``, ``/password-reset/request``,
``/password-reset/confirm``, ``/deactivate``). Schemas come from the operation
descriptors and each route's ``operation_id`` is the registry operation key
verbatim.

Authentication posture (read this before exposing the router):

- ``/login``, ``/refresh``, ``/password-reset/request``, and
  ``/password-reset/confirm`` are *meant* to be reachable without a bearer
  token — the operations themselves authenticate (password / refresh-token /
  reset-token credentials in the body, or none at all for the reset request).
  :class:`SecurityContextMiddleware` with non-required ingress simply binds no
  identity and lets the request through; nothing in the generated routes
  demands one.
- ``/password-reset/request`` answers a **uniform 202 acknowledgment** for
  known and unknown logins alike (no account enumeration) and never carries the
  reset token in its response — delivery happens out of band via the
  ``reset_events`` outbox seam of ``build_authn_registry`` (or a custom
  handler). Rate-limit this route at the edge; it is an unauthenticated write.
  ``/password-reset/confirm`` answers a uniform 401 for every bad-token flavor.
- ``/logout`` and ``/change-password`` are self-guarding at the *handler* level:
  ``build_authn_registry`` binds no before-hooks, but both handlers resolve the
  bound :class:`~forze.application.contracts.authn.AuthnIdentity` and raise a
  401 (``auth_required``) when none is present. The identity comes from the
  boundary middleware verifying the caller's access token.
- ``/deactivate`` (``deactivate_principal``) has **no built-in guard at all** —
  the handler calls the deactivation port directly. It is an admin-grade
  operation: bind :class:`~forze.application.hooks.authn.AuthnRequired` and an
  authz before-hook (e.g.
  :class:`~forze.application.hooks.authz.AuthzBeforeAuthorize`) on its operation
  before exposing it, or keep it off the router via ``include=``.

Responses of ``/login`` and ``/refresh`` carry token material in the body by
design (the OAuth2-shaped :class:`~forze_kits.aggregates.authn.AuthnTokenResponseDTO`);
the stock :class:`~forze_fastapi.middlewares.LoggingMiddleware` logs only
method/path/status/duration, never bodies — keep any custom access logging
equally body-blind.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import AbstractSet, Any, Awaitable, Callable, Mapping

from fastapi import APIRouter
from pydantic import BaseModel

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.authn import AuthnKernelOp

from ._attach import (
    OperationRunner,
    RouteBinding,
    attach_operation_routes,
    body_endpoint,
)

# ----------------------- #


def _no_body_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint for an input-less operation — no request payload at all."""

    _ = input_type, op  # logout takes no input; identity comes from the context

    async def endpoint() -> Any:
        return await runner(None)

    return endpoint


# ....................... #

_AUTHN_BINDINGS: Mapping[str, RouteBinding] = {
    AuthnKernelOp.PASSWORD_LOGIN: RouteBinding(
        method="POST", path="/login", build=body_endpoint
    ),
    AuthnKernelOp.REFRESH_TOKENS: RouteBinding(
        method="POST", path="/refresh", build=body_endpoint
    ),
    # Logout takes no input (the identity is the context binding) and returns
    # nothing — no body in, 204 out.
    AuthnKernelOp.LOGOUT: RouteBinding(
        method="POST", path="/logout", build=_no_body_endpoint, status_code=204
    ),
    AuthnKernelOp.CHANGE_PASSWORD: RouteBinding(
        method="POST",
        path="/change-password",
        build=body_endpoint,
        status_code=204,
    ),
    # Request-reset answers 202: the request is *accepted* (the actual outcome —
    # token issued or login unknown — is deliberately unobservable), and the
    # uniform ack DTO is the body either way.
    AuthnKernelOp.REQUEST_PASSWORD_RESET: RouteBinding(
        method="POST",
        path="/password-reset/request",
        build=body_endpoint,
        status_code=202,
    ),
    AuthnKernelOp.RESET_PASSWORD: RouteBinding(
        method="POST",
        path="/password-reset/confirm",
        build=body_endpoint,
        status_code=204,
    ),
    AuthnKernelOp.DEACTIVATE_PRINCIPAL: RouteBinding(
        method="POST", path="/deactivate", build=body_endpoint, status_code=204
    ),
}
"""Fixed action-path bindings per authn kernel operation.

Login/refresh answer 200 with the token response DTO; request-reset answers 202
with the uniform ack DTO; the void operations (logout, change-password,
reset-confirm, deactivate) answer 204, mirroring the void document and storage
routes.
"""


# ....................... #


def attach_authn_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[AuthnKernelOp | str] | None = None,
) -> APIRouter:
    """Attach the registered authn operations under *ns* to *router*.

    One ``POST`` route per registered :class:`AuthnKernelOp`, on a fixed action
    path — authn flows are RPC-shaped with one natural surface each, so there is
    no ``style`` argument:

    - ``POST /login`` → ``password_login`` (200, token response DTO)
    - ``POST /refresh`` → ``refresh_tokens`` (200, token response DTO)
    - ``POST /logout`` → ``logout`` (204, no request body)
    - ``POST /change-password`` → ``change_password`` (204)
    - ``POST /password-reset/request`` → ``request_password_reset`` (202, uniform ack DTO)
    - ``POST /password-reset/confirm`` → ``reset_password`` (204)
    - ``POST /deactivate`` → ``deactivate_principal`` (204)

    Each route's ``operation_id`` is the operation key verbatim (e.g.
    ``main.password_login``); request/response schemas come from the operation
    descriptors, and every call dispatches through ``run_operation`` — plans and
    hooks apply, no bypass.

    The router is expected to be reachable **without** a bearer token: login,
    refresh, and both password-reset flows authenticate via their request
    bodies (or, for the reset request, not at all — it answers a uniform 202
    for known and unknown logins, see the module docstring), not the security
    context. Guarding the other flows is the operation plan's (or handler's)
    job, not the route's — ``logout`` and ``change_password`` already raise a
    401 from their handlers when no identity is bound, while
    ``deactivate_principal`` ships unguarded: bind
    :class:`~forze.application.hooks.authn.AuthnRequired` plus an authz
    before-hook on it (see the module docstring), or exclude it via
    ``include=``.

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the authn operations.
    :param ns: Namespace the operations were registered under
        (e.g. ``spec.default_namespace``).
    :param ctx_dep: Factory yielding the current execution context per request.
    :param include: Optional narrowing to a subset of kernel operations; including
        an operation the registry lacks is a configuration error.
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_AUTHN_BINDINGS,
        include=include,
    )
