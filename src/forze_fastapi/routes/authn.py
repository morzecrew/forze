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
  List their mounted paths in :class:`SecurityContextMiddleware`'s
  ``anonymous_paths``: it refuses a request no ingress authenticates everywhere
  else. On those paths a request with a missing or failing credential proceeds
  with no identity (a valid credential still binds); nothing in the generated
  routes demands one. In cookie mode list ``/logout`` too: it is reachable without
  an access cookie by design, revokes from the refresh cookie then, and expires the
  cookies either way.
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
  boundary middleware verifying the caller's access token. Cookie-mode logout
  without one names the session by the refresh cookie instead.
- The admin routes act on another principal: ``/deactivate``
  (``deactivate_principal``), ``GET /admin/principals/{id}/api-keys``
  (``list_principal_api_keys``) and ``DELETE /admin/api-keys/{id}``
  (``revoke_principal_api_key``). They exist only when the registry was built with
  ``admin_guards`` (e.g. :class:`~forze.application.hooks.authn.AuthnRequired` plus
  :class:`~forze.application.hooks.authz.AuthzBeforeAuthorize`), which they run
  behind. Guards none of which declares its permission keys (``permission_keys()``,
  as ``AuthzBeforeAuthorize`` does) are refused at build unless
  ``trust_admin_guards=True``. They are global (credential accounts are
  not tenant-scoped), so do not expose them to tenant-scoped administrators; the
  handlers also refuse a delegated caller (``delegate_denied``).

Responses of ``/login`` and ``/refresh`` carry token material in the body by
design (the OAuth2-shaped :class:`~forze_kits.aggregates.authn.AuthnTokenResponseDTO`);
the stock :class:`~forze_fastapi.middlewares.LoggingMiddleware` logs only
method/path/status/duration, never bodies — keep any custom access logging
equally body-blind.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from typing import Annotated, Any

import attrs
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, BeforeValidator

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.authn import AuthnKernelOp, AuthnRefreshRequestDTO

from ..middlewares.authn_cookies import AUTHN_COOKIE_ANONYMOUS_ATTR, AUTHN_COOKIE_LOGOUT_ATTR
from ..security.cookies import AuthnCookieCarrier
from ..security.resolvers import enforce_published_cookie_csrf
from ._attach import (
    OperationRunner,
    RouteBinding,
    _operation_runner,
    attach_operation_routes,
    body_endpoint,
    id_endpoint,
    require_input_type,
    resolve_namespace,
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
# Cookie-mode endpoint builders: same operations, token material rides cookies.


def _cookie_token_endpoint(cookies: AuthnCookieCarrier) -> Any:
    """Login-shaped builder: run the op, set/rotate cookies, strip the body.

    Setting the cookies applies the access cookie's CSRF gate first, as refresh and
    logout do, on every request.
    """

    def build(
        runner: OperationRunner,
        input_type: type[BaseModel] | None,
        op: str,
    ) -> Callable[..., Awaitable[Any]]:
        dto_type = require_input_type(input_type, op)

        async def endpoint(*, request: Request, payload: Any, response: Response) -> Any:
            enforce_published_cookie_csrf(request, cookies.access_cookie)

            return cookies.apply(response, await runner(payload))

        endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [
                inspect.Parameter("request", inspect.Parameter.KEYWORD_ONLY, annotation=Request),
                inspect.Parameter("payload", inspect.Parameter.KEYWORD_ONLY, annotation=dto_type),
                inspect.Parameter("response", inspect.Parameter.KEYWORD_ONLY, annotation=Response),
            ]
        )
        endpoint.__annotations__ = {
            "request": Request,
            "payload": dto_type,
            "response": Response,
        }
        # check_authn_cookie_paths requires the route's mounted path in anonymous_paths.
        setattr(endpoint, AUTHN_COOKIE_ANONYMOUS_ATTR, True)

        return endpoint

    return build


def _empty_object_as_none(value: Any) -> Any:
    """Read an empty JSON object as no body at all.

    Browser fetch wrappers and generated clients send ``{}`` where they mean "no
    payload"; on refresh that is the cookie-mode request, not an invalid body.
    """

    return None if isinstance(value, dict) and not value else value


def _cookie_refresh_endpoint(cookies: AuthnCookieCarrier) -> Any:
    """Refresh builder: the token may ride the cookie instead of the body.

    The body stays accepted (an API client may still send it) and wins over the
    cookie; absent one — no body, an empty body, ``null`` or ``{}`` — the refresh
    token comes from the carrier's cookie, the reason a browser can refresh
    without ever seeing token strings. A non-empty body without the token is
    still a validation error. Every request applies the
    :class:`~forze_fastapi.security.CookieCsrf` gate of the ``CookieTokenAuthn``
    reading the carrier's access cookie (the default gate when there is none),
    whichever carries the token, since either way the response sets the cookies.
    """

    def build(
        runner: OperationRunner,
        input_type: type[BaseModel] | None,
        op: str,
    ) -> Callable[..., Awaitable[Any]]:
        dto_type = require_input_type(input_type, op)
        # The validator changes no schema: OpenAPI still documents the DTO body.
        payload_type = Annotated[
            dto_type | None,  # type: ignore[valid-type]
            BeforeValidator(_empty_object_as_none),
        ]

        async def endpoint(*, request: Request, response: Response, payload: Any = None) -> Any:
            # The response sets the session cookies whichever carries the token: the same
            # gate as login, before the token is read or spent.
            enforce_published_cookie_csrf(request, cookies.access_cookie)

            body = payload

            if body is None:
                token = cookies.read_refresh(request)

                if token is None:
                    raise exc.authentication(
                        "No refresh token: the request carried neither a token in its "
                        f"body nor the {cookies.refresh_cookie!r} cookie.",
                        code="auth_required",
                    )

                body = dto_type.model_validate({"refresh_token": token})

            return cookies.apply(response, await runner(body))

        endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [
                inspect.Parameter("request", inspect.Parameter.KEYWORD_ONLY, annotation=Request),
                inspect.Parameter("response", inspect.Parameter.KEYWORD_ONLY, annotation=Response),
                inspect.Parameter(
                    "payload",
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=payload_type,
                    default=None,
                ),
            ]
        )
        endpoint.__annotations__ = {
            "request": Request,
            "response": Response,
            "payload": payload_type,
        }
        # check_authn_cookie_paths requires the route's mounted path in anonymous_paths.
        setattr(endpoint, AUTHN_COOKIE_ANONYMOUS_ATTR, True)

        return endpoint

    return build


def _cookie_logout_endpoint(
    cookies: AuthnCookieCarrier,
    logout_by_refresh_token: OperationRunner,
) -> Any:
    """Logout builder: run the op, then expire both cookies.

    With no bound identity, the refresh cookie names the sessions to revoke instead
    (*logout_by_refresh_token*). Idempotent in cookie mode: with neither there is
    nothing server-side to revoke — but the caller's goal is "end this browser
    session", and a 401 would leave the dead cookies pinned to the browser. The
    unauthenticated case clears them and answers 204; every other failure propagates.
    The access cookie's CSRF gate applies first, with or without a cookie, as on login.
    """

    def build(
        runner: OperationRunner,
        input_type: type[BaseModel] | None,
        op: str,
    ) -> Callable[..., Awaitable[Any]]:
        _ = input_type, op

        async def endpoint(*, request: Request, response: Response) -> Any:
            from forze.base.exceptions import CoreException

            enforce_published_cookie_csrf(request, cookies.access_cookie)

            try:
                result = await runner(None)

            except CoreException as error:
                if error.code != "auth_required":
                    raise

                token = cookies.read_refresh(request)

                if token is not None:
                    await logout_by_refresh_token(AuthnRefreshRequestDTO(refresh_token=token))

                result = None  # nothing else to revoke server-side; still clear below

            cookies.clear(response)
            return result

        endpoint.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
            [
                inspect.Parameter("request", inspect.Parameter.KEYWORD_ONLY, annotation=Request),
                inspect.Parameter("response", inspect.Parameter.KEYWORD_ONLY, annotation=Response),
            ]
        )
        endpoint.__annotations__ = {"request": Request, "response": Response}
        # check_authn_cookie_paths holds the route's mounted path against these, and
        # against anonymous_paths, at startup.
        setattr(endpoint, AUTHN_COOKIE_LOGOUT_ATTR, (cookies.access_path, cookies.refresh_path))
        setattr(endpoint, AUTHN_COOKIE_ANONYMOUS_ATTR, True)

        return endpoint

    return build


def _cookie_bindings(
    cookies: AuthnCookieCarrier,
    logout_by_refresh_token: OperationRunner,
) -> Mapping[str, RouteBinding]:
    """The action bindings with login/refresh/logout swapped to cookie mode."""

    bindings = dict(_AUTHN_BINDINGS)
    bindings[AuthnKernelOp.PASSWORD_LOGIN] = attrs.evolve(
        bindings[AuthnKernelOp.PASSWORD_LOGIN], build=_cookie_token_endpoint(cookies)
    )
    bindings[AuthnKernelOp.REFRESH_TOKENS] = attrs.evolve(
        bindings[AuthnKernelOp.REFRESH_TOKENS], build=_cookie_refresh_endpoint(cookies)
    )
    bindings[AuthnKernelOp.LOGOUT] = attrs.evolve(
        bindings[AuthnKernelOp.LOGOUT],
        build=_cookie_logout_endpoint(cookies, logout_by_refresh_token),
    )

    return bindings


# ....................... #

_AUTHN_BINDINGS: Mapping[str, RouteBinding] = {
    AuthnKernelOp.PASSWORD_LOGIN: RouteBinding(method="POST", path="/login", build=body_endpoint),
    AuthnKernelOp.REFRESH_TOKENS: RouteBinding(method="POST", path="/refresh", build=body_endpoint),
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
    # Self-service API-key management is a genuine resource collection (unlike the
    # auth-flow actions), so it takes resource-style verbs: create / list / delete.
    AuthnKernelOp.ISSUE_API_KEY: RouteBinding(
        method="POST", path="/api-keys", build=body_endpoint, status_code=201
    ),
    AuthnKernelOp.LIST_API_KEYS: RouteBinding(
        method="GET", path="/api-keys", build=_no_body_endpoint
    ),
    AuthnKernelOp.REVOKE_API_KEY: RouteBinding(
        method="DELETE", path="/api-keys/{id}", build=id_endpoint, status_code=204
    ),
    # Registered only when the registry was built with ``admin_guards``.
    AuthnKernelOp.LIST_PRINCIPAL_API_KEYS: RouteBinding(
        method="GET", path="/admin/principals/{id}/api-keys", build=id_endpoint
    ),
    AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY: RouteBinding(
        method="DELETE", path="/admin/api-keys/{id}", build=id_endpoint, status_code=204
    ),
}
"""Fixed action-path bindings per authn kernel operation.

Login/refresh answer 200 with the token response DTO; request-reset answers 202
with the uniform ack DTO; the void operations (logout, change-password,
reset-confirm, deactivate) answer 204, mirroring the void document and storage
routes.
"""


# ....................... #


def _authn_namespace(
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None,
    resource: str | None,
) -> StrKeyNamespace:
    """*ns* or *resource* when given; otherwise the one namespace holding the authn flows.

    ``build_authn_registry(spec)`` registers ``password_login`` and ``refresh_tokens``
    under ``spec.default_namespace``, so the inferred namespace is that one. Inference
    never guesses: no namespace holding either operation, or more than one, is a
    configuration error.
    """

    if ns is not None or resource is not None:
        return resolve_namespace(ns, resource)

    # Anchored on the operations every authn registry holds, so a foreign namespace with
    # an operation merely named like a lesser kernel op is never picked; and only under
    # the default separator, so a custom one is passed explicitly, never guessed.
    anchors = {str(AuthnKernelOp.PASSWORD_LOGIN), str(AuthnKernelOp.REFRESH_TOKENS)}
    prefixes: set[str] = set()

    for key in registry.handlers:
        prefix, _, op = str(key).partition(".")

        if prefix and op in anchors:
            prefixes.add(prefix)

    if len(prefixes) == 1:
        return StrKeyNamespace(prefix=prefixes.pop())

    found = ", ".join(sorted(prefixes)) if prefixes else "none"

    raise exc.configuration(
        "Cannot infer the authn namespace from the registry (namespaces holding "
        f"password_login or refresh_tokens: {found}). Pass ns=spec.default_namespace "
        "or resource=."
    )


def attach_authn_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None = None,
    ctx_dep: ExecutionContextFactory,
    include: AbstractSet[AuthnKernelOp | str] | None = None,
    resource: str | None = None,
    path_overrides: Mapping[AuthnKernelOp | str, str] | None = None,
    exclude_none: bool = True,
    cookies: AuthnCookieCarrier | None = None,
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
    - ``POST /deactivate`` → ``deactivate_principal`` (204), only when the registry was
      built with ``admin_guards``
    - ``POST /api-keys`` → ``issue_api_key`` (201, the secret returned once)
    - ``GET /api-keys`` → ``list_api_keys`` (non-secret descriptors)
    - ``DELETE /api-keys/{id}`` → ``revoke_api_key`` (204)
    - ``GET /admin/principals/{id}/api-keys`` → ``list_principal_api_keys`` and
      ``DELETE /admin/api-keys/{id}`` → ``revoke_principal_api_key`` (204), only when the
      registry was built with ``admin_guards``

    Self-service API-key management is a real resource collection, so it uses
    resource-style verbs (the auth-flow actions stay ``POST``). All three require a
    bound identity (``AuthnRequired`` — a 401 without one).

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
    401 from their handlers when no identity is bound, and the admin operations
    exist only behind the registry's ``admin_guards`` (see the module docstring).

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        registry (FrozenOperationRegistry): Frozen registry holding the authn
            operations.
        ns (StrKeyNamespace | None): Namespace the operations were registered under
            (e.g. ``spec.default_namespace``). Mutually exclusive with *resource*. With
            neither, the one namespace in *registry* holding ``password_login`` or
            ``refresh_tokens`` (default separator) is used — ``spec.default_namespace``
            for ``build_authn_registry(spec)``.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution
            context per request.
        include (AbstractSet | None): Optional narrowing to a subset of kernel
            operations; including an operation the registry lacks is a configuration
            error.
        resource (str | None): Convenience alternative to *ns* — a prefix string the
            namespace is built from; must equal the prefix the operations were
            registered under. Mutually exclusive with *ns*.
        path_overrides (Mapping | None): Optional per-operation route-path replacements
            (keyed like *include*); only the path changes, the ``operation_id`` stays
            verbatim. An override must bind exactly the ``{id}`` placeholder the
            default path binds.
        cookies (AuthnCookieCarrier | None): Cookie mode. When set, ``/login`` and
            ``/refresh`` set (and rotate) the carrier's HttpOnly cookies and strip
            token strings from their bodies; ``/refresh`` also accepts a request with
            no body or an empty JSON object, reading the refresh token from its
            cookie behind the access cookie's CSRF gate (a token in the body takes
            precedence); ``/logout`` revokes the session the access cookie's identity
            or, without one, the refresh cookie names (``logout_by_refresh_token``,
            which gets no route of its own), and expires both cookies.
            ``/login`` and ``/logout`` apply the same gate on every request. Point
            ``CookieTokenAuthn`` at the carrier's access cookie, and list the
            login, refresh and logout paths in the security middleware's
            ``anonymous_paths`` so a stale access cookie cannot 401 them (a 401'd
            logout revokes nothing and leaves the cookies set). The path of the
            refresh cookie must cover the logout route. Both are checked at startup by
            :func:`~forze_fastapi.middlewares.check_authn_cookie_paths`.

    Returns:
        APIRouter: The same *router*, for chaining.

    Raises:
        CoreException: On a configuration error — an unknown *include*/override
            operation, both *ns* and *resource*, neither of them with no single
            authn namespace in *registry*, a path override that drops or adds a
            placeholder, or cookie mode on a registry holding ``logout`` but not
            ``logout_by_refresh_token``.
    """

    resolved_ns = _authn_namespace(registry, ns, resource)
    bindings = _AUTHN_BINDINGS

    if cookies is not None:
        by_refresh = resolved_ns.key(AuthnKernelOp.LOGOUT_BY_REFRESH_TOKEN)

        # Without it, a logout with an expired access cookie would revoke nothing.
        if (
            resolved_ns.key(AuthnKernelOp.LOGOUT) in registry.handlers
            and by_refresh not in registry.handlers
        ):
            raise exc.configuration(
                f"Cookie-mode logout needs the '{by_refresh}' operation to revoke a "
                "session from the refresh cookie; register it (build_authn_registry does)."
            )

        bindings = _cookie_bindings(cookies, _operation_runner(registry, by_refresh, ctx_dep))

    return attach_operation_routes(
        router,
        registry=registry,
        ns=resolved_ns,
        ctx_dep=ctx_dep,
        bindings=bindings,
        include=include,
        path_overrides=path_overrides,
        exclude_none=exclude_none,
        skip_unregistered=True,
    )
