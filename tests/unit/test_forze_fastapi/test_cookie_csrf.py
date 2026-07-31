"""The cookie ingress's server-side CSRF gate (:class:`CookieCsrf`).

The contract: an unsafe request may authenticate with the ambient cookie only after
proving a same-host or allowlisted origin — independent of the outbound carrier's
``SameSite``, which a proxy or a ``SameSite=None`` deployment can void. The gate fires
only when the cookie is present, so header-authenticated non-browser clients are never
refused by it.
"""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

import pytest
from fastapi import Request

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_fastapi.security import CookieCsrf, CookieTokenAuthn, resolve_authn_ingress
from tests.support.execution_context import context_from_deps

# ----------------------- #

_TOKEN_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"token"}))


class _TokenAuthPort:
    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return AuthnResult(
            identity=AuthnIdentity(principal_id=uuid5(NAMESPACE_URL, credentials.token))
        )

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _TokenAuthFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _TokenAuthPort:
        return _TokenAuthPort()


def _ctx() -> ExecutionContext:
    from forze.application.contracts.authn import AuthnDepKey

    return context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))


def _request(
    *,
    method: str = "POST",
    cookie: str | None = "sid=cookie-token",
    host: str | None = "app.example.com",
    origin: str | None = None,
    referer: str | None = None,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []

    if cookie is not None:
        headers.append((b"cookie", cookie.encode()))
    if host is not None:
        headers.append((b"host", host.encode()))
    if origin is not None:
        headers.append((b"origin", origin.encode()))
    if referer is not None:
        headers.append((b"referer", referer.encode()))

    return Request({"type": "http", "path": "/", "method": method, "headers": headers})


_DEFAULT_GATE = CookieCsrf()


def _ingress(csrf: CookieCsrf | None = _DEFAULT_GATE) -> CookieTokenAuthn:
    return CookieTokenAuthn(authn_spec=_TOKEN_SPEC, cookie_name="sid", csrf=csrf)


# ....................... #


class TestCookieCsrfPolicy:
    """The pure verdict: headers in, refusal (or None) out."""

    def test_safe_methods_never_require_proof(self) -> None:
        policy = CookieCsrf()

        for method in ("GET", "HEAD", "OPTIONS", "TRACE", "get"):
            assert (
                policy.rejection(method=method, host="a.example", origin=None, referer=None)
                is None
            )

    def test_unsafe_cross_origin_is_refused(self) -> None:
        refusal = CookieCsrf().rejection(
            method="POST",
            host="app.example.com",
            origin="https://evil.example.net",
            referer=None,
        )

        assert refusal is not None and "evil.example.net" in refusal

    def test_unsafe_same_host_origin_passes(self) -> None:
        policy = CookieCsrf()

        assert (
            policy.rejection(
                method="POST",
                host="app.example.com",
                origin="https://app.example.com",
                referer=None,
            )
            is None
        )
        # Host may carry a port and the scheme may differ behind a TLS-terminating
        # proxy — the same-host comparison is by hostname.
        assert (
            policy.rejection(
                method="DELETE",
                host="app.example.com:8443",
                origin="http://app.example.com",
                referer=None,
            )
            is None
        )

    def test_subdomain_is_not_same_host(self) -> None:
        assert (
            CookieCsrf().rejection(
                method="POST",
                host="app.example.com",
                origin="https://evil.app.example.com",
                referer=None,
            )
            is not None
        )

    def test_referer_falls_back_when_origin_absent(self) -> None:
        policy = CookieCsrf()

        assert (
            policy.rejection(
                method="POST",
                host="app.example.com",
                origin=None,
                referer="https://app.example.com/account/settings",
            )
            is None
        )
        assert (
            policy.rejection(
                method="POST",
                host="app.example.com",
                origin=None,
                referer="https://evil.example.net/attack.html",
            )
            is not None
        )

    def test_origin_present_wins_over_referer(self) -> None:
        # A cross-site Origin with a spoofed-looking Referer must still be refused.
        assert (
            CookieCsrf().rejection(
                method="POST",
                host="app.example.com",
                origin="https://evil.example.net",
                referer="https://app.example.com/",
            )
            is not None
        )

    def test_allowlisted_origin_passes_exactly(self) -> None:
        policy = CookieCsrf(allowed_origins={"https://spa.example.com"})

        assert (
            policy.rejection(
                method="POST",
                host="api.example.com",
                origin="https://spa.example.com",
                referer=None,
            )
            is None
        )
        # The allowlist is exact full origins — scheme and port count.
        assert (
            policy.rejection(
                method="POST",
                host="api.example.com",
                origin="http://spa.example.com",
                referer=None,
            )
            is not None
        )

    def test_missing_origin_refused_by_default_and_optable(self) -> None:
        strict = CookieCsrf()
        lenient = CookieCsrf(allow_missing_origin=True)

        assert (
            strict.rejection(method="POST", host="a.example", origin=None, referer=None)
            is not None
        )
        assert (
            lenient.rejection(method="POST", host="a.example", origin=None, referer=None) is None
        )

    def test_null_origin_is_refused_not_treated_as_missing(self) -> None:
        # "Origin: null" (sandboxed iframe, data: redirect) is attacker-adjacent —
        # allow_missing_origin must NOT wave it through.
        assert (
            CookieCsrf(allow_missing_origin=True).rejection(
                method="POST", host="a.example", origin="null", referer=None
            )
            is not None
        )


# ....................... #


class TestCookieIngressCsrfGate:
    """The gate wired into the cookie resolver."""

    @pytest.mark.asyncio
    async def test_cross_origin_post_with_cookie_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            await resolve_authn_ingress(
                _ingress(),
                request=_request(origin="https://evil.example.net"),
                ctx=_ctx(),
            )

        assert ei.value.kind is ExceptionKind.AUTHORIZATION
        assert ei.value.code == "csrf_rejected"

    @pytest.mark.asyncio
    async def test_same_origin_post_authenticates(self) -> None:
        authn = await resolve_authn_ingress(
            _ingress(),
            request=_request(origin="https://app.example.com"),
            ctx=_ctx(),
        )

        assert authn is not None
        assert authn.identity.principal_id == uuid5(NAMESPACE_URL, "cookie-token")

    @pytest.mark.asyncio
    async def test_missing_origin_post_with_cookie_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            await resolve_authn_ingress(_ingress(), request=_request(), ctx=_ctx())

        assert ei.value.kind is ExceptionKind.AUTHORIZATION

    @pytest.mark.asyncio
    async def test_cookieless_post_is_not_gated(self) -> None:
        # A header-authenticated non-browser client (no cookie, no Origin) must not
        # be refused by the cookie ingress: no ambient credential, no forgery.
        assert (
            await resolve_authn_ingress(
                _ingress(),
                request=_request(cookie=None),
                ctx=_ctx(),
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_safe_method_with_cross_origin_still_authenticates(self) -> None:
        authn = await resolve_authn_ingress(
            _ingress(),
            request=_request(method="GET", origin="https://evil.example.net"),
            ctx=_ctx(),
        )

        assert authn is not None

    @pytest.mark.asyncio
    async def test_required_ingress_still_401s_on_missing_cookie(self) -> None:
        # The gate never runs without the cookie: a required ingress keeps its
        # authentication-kind refusal (401, downgradable on anonymous_paths).
        ingress = CookieTokenAuthn(authn_spec=_TOKEN_SPEC, cookie_name="sid", required=True)

        with pytest.raises(CoreException) as ei:
            await resolve_authn_ingress(ingress, request=_request(cookie=None), ctx=_ctx())

        assert ei.value.kind is ExceptionKind.AUTHENTICATION

    @pytest.mark.asyncio
    async def test_declared_opt_out_disables_the_gate(self) -> None:
        authn = await resolve_authn_ingress(
            _ingress(csrf=None),
            request=_request(origin="https://evil.example.net"),
            ctx=_ctx(),
        )

        assert authn is not None
