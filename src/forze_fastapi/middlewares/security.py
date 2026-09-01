from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #


from typing import Literal

import attrs
from fastapi import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from forze.application.contracts.authn import AuthnResult
from forze.application.execution.context import (
    ExecutionContext,
    ExecutionContextFactory,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc

from ..exceptions import build_core_exception_response
from ..security import AuthnRequirement, resolve_authn_ingress, resolve_tenant_identity
from .raw_websocket import refuse_raw_websocket, websocket_scope_refused

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class SecurityContextMiddleware:
    app: ASGIApp
    """The next ASGI application."""

    ctx_dep: ExecutionContextFactory = attrs.field(kw_only=True)
    """The dependency to resolve the execution context."""

    authn: AuthnRequirement
    """Authn requirement declaration"""

    when_multiple_credentials: Literal["first_in_order", "reject"]
    """Policy for handling more than one resolver returning a non-``None`` identity."""

    trust_tenant_header: bool = attrs.field(default=False, kw_only=True)
    """Trust the raw ``X-Tenant-Id`` header when no tenancy resolver validates it.

    Default ``False`` (deny): an unvalidated header tenant is unauthenticated input.
    Enable only behind a trusted gateway that sets the header authoritatively.
    """

    allow_raw_websockets: bool = attrs.field(default=False, kw_only=True)
    """Let raw ``websocket`` scopes bypass this middleware (default ``False``: refuse).

    Identity and tenancy are resolved for HTTP scopes only, so a websocket route
    would run without either. Enabling this is a declared decision that the app
    owns identity, tenancy, and error shaping on every websocket route itself.
    """

    anonymous_paths: frozenset[str] = attrs.field(
        default=frozenset(), kw_only=True, converter=frozenset
    )
    """Exact request paths where a failing credential binds no identity instead of 401ing.

    A browser holding a stale access cookie would otherwise be refused on the very
    routes that exist without an identity — ``/auth/login`` (which replaces the
    credential), ``/auth/refresh``, a public health page. On these paths an
    **authentication-kind** failure (expired/invalid credential, ambiguous
    credentials, a tenant mismatch) downgrades to an **anonymous** request: no
    authn, no tenant bound — the route authenticates from its body or serves
    anonymously, exactly as it would for a request carrying no credential at all.
    A VALID credential still binds normally, and any other failure kind
    (infrastructure, configuration, internal) still returns the error response —
    a secrets-store outage is a server fault, not a missing credential.
    Exact paths, never prefixes — a prefix is one refactor away from an ungoverned
    hole (the same stance as ``allowed_websocket_paths``)."""

    allowed_websocket_paths: frozenset[str] = attrs.field(
        default=frozenset(), kw_only=True, converter=frozenset
    )
    """Exact paths whose websocket scopes pass through (governed routes only).

    The narrow alternative to the app-wide ``allow_raw_websockets`` hatch: list the
    framework-attached websocket routes (e.g. ``attach_realtime_ws_route``'s path),
    which resolve identity at connect themselves. Exact paths, never prefixes — a
    prefix is one refactor away from an ungoverned hole.
    """

    bypass_paths: frozenset[str] = attrs.field(
        default=frozenset(), kw_only=True, converter=frozenset
    )
    """Exact HTTP paths this middleware does not run for.

    Liveness must not depend on anything: this middleware resolves the execution
    context on every request, so in front of a probe path it answers 500 while the
    runtime scope is not yet open — which is precisely the window a liveness probe
    exists to observe. Exact paths, never prefixes. A bypassed path serves with **no
    identity and no tenant bound** and no error shaping — list probe and scrape
    paths, never anything that reads or writes tenant data (``anonymous_paths`` is
    the softer tool: it still runs this middleware and still binds a *valid*
    credential). Entries are the **full mounted path** (router prefixes included),
    the same as ``allowed_websocket_paths``: this middleware runs before routing, so
    a prefix mismatch simply never matches and the probe goes on failing.
    :func:`~forze_fastapi.middlewares.check_bypass_paths` (run by ``runtime_lifespan``)
    fails the boot on that, on a bypassed path that serves a generated operation route,
    and on the two middlewares carrying different sets."""

    # ....................... #

    async def _resolve_authn(
        self,
        request: Request,
        ctx: ExecutionContext,
    ) -> tuple[AuthnResult, str] | None:
        """The winning credential, and the authn route it came in on.

        The route travels with the result because tenancy resolution needs it: the
        resolver is registered per route, and a credential's tenancy belongs to the same
        profile the credential authenticated against.
        """

        results: list[tuple[AuthnResult, str]] = []

        for x in self.authn.ingress:
            res = await resolve_authn_ingress(x, request=request, ctx=ctx)

            if res is None:
                continue

            results.append((res, str(x.authn_spec.name)))

            if self.when_multiple_credentials == "first_in_order":
                return results[-1]

        if not results:
            return None

        if self.when_multiple_credentials == "reject" and len(results) > 1:
            raise exc.authentication(
                "Multiple authentication credentials present",
                code="ambiguous_credentials",
            )

        return results[0]

    # ....................... #

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            if websocket_scope_refused(
                scope,
                allow_raw_websockets=self.allow_raw_websockets,
                allowed_websocket_paths=self.allowed_websocket_paths,
            ):
                await refuse_raw_websocket(scope, receive, send)
                return

            await self.app(scope, receive, send)
            return

        if scope.get("path") in self.bypass_paths:
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        ctx = self.ctx_dep()

        try:
            resolved = await self._resolve_authn(request, ctx)
            authn_res, authn_route = resolved if resolved is not None else (None, None)
            authn = authn_res.identity if authn_res is not None else None
            tenant = await resolve_tenant_identity(
                authn_res,
                request=request,
                ctx=ctx,
                trust_tenant_header=self.trust_tenant_header,
                tenancy_route=authn_route,
            )

        except CoreException as error:
            if (
                error.kind is ExceptionKind.AUTHENTICATION
                and request.url.path in self.anonymous_paths
            ):
                # A failing CREDENTIAL on an anonymous path downgrades to no
                # identity at all (see ``anonymous_paths``): the route is reachable
                # without one by design, and a stale cookie must not lock the
                # caller out of the very route that replaces it. Only
                # authentication-kind failures qualify — an infrastructure or
                # configuration error during resolution is a server fault, and
                # serving the route anonymously would mask the outage.
                authn = None
                tenant = None

            else:
                # This middleware runs above Starlette's ExceptionMiddleware, so the
                # registered CoreException handler never sees errors raised here.
                # Convert them to the standard JSON error response in place.
                response = build_core_exception_response(error)
                await response(scope, receive, send)
                return

        with ctx.inv_ctx.bind_identity(authn=authn, tenant=tenant):
            await self.app(scope, receive, send)
