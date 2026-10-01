"""A request no ingress authenticates is refused unless its path is meant to be anonymous.

The middleware used to bind no identity for such a request and pass it on, so a route with no
``AuthnRequired`` hook — any hand-written route — served it anonymously by default.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import FastAPI
import pytest
from starlette.testclient import TestClient

from forze.application.contracts.authn import AuthnDepKey, AuthnSpec
from forze.application.execution import Deps
from forze_fastapi.exceptions import ERROR_CODE_HEADER
from forze_fastapi.middlewares import SecurityContextMiddleware
from forze_fastapi.security import AuthnRequirement, CookieTokenAuthn, HeaderTokenAuthn
from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_fastapi.test_middleware_context import _TokenAuthFactory

_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))
_HEADER = HeaderTokenAuthn(authn_spec=_SPEC, header_name="Authorization")


def _client(**options: Any) -> tuple[TestClient, dict[str, object]]:
    ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
    seen: dict[str, object] = {}

    async def _app(scope: Any, receive: Any, send: Any) -> None:
        seen["authn"] = ctx.inv_ctx.get_authn()
        seen["tenant"] = ctx.inv_ctx.get_tenant()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    requirement = options.pop("requirement", AuthnRequirement(ingress=(_HEADER,)))
    middleware = SecurityContextMiddleware(
        _app,
        ctx_dep=lambda: ctx,
        authn=requirement,
        when_multiple_credentials="first_in_order",
        **options,
    )

    return TestClient(middleware), seen


class TestTheRequirement:
    def test_a_request_without_a_credential_is_refused_by_default(self) -> None:
        client, seen = _client()

        response = client.post("/orders")

        assert (response.status_code, response.headers.get(ERROR_CODE_HEADER)) == (
            401,
            "auth_required",
        )
        assert "authn" not in seen

    def test_an_anonymous_path_is_served_without_one(self) -> None:
        client, seen = _client(anonymous_paths={"/auth/login"})

        response = client.post("/auth/login")

        assert (response.status_code, seen["authn"]) == (200, None)

    def test_a_cors_preflight_is_never_refused(self) -> None:
        client, seen = _client()

        response = client.options(
            "/orders",
            headers={"Origin": "https://app.example.com", "Access-Control-Request-Method": "POST"},
        )

        assert (response.status_code, seen["authn"]) == (200, None)

    def test_an_options_request_that_is_not_a_preflight_is_refused(self) -> None:
        # A hand-written OPTIONS route is a route like any other.
        client, seen = _client()

        response = client.options("/orders")

        assert response.status_code == 401
        assert "authn" not in seen

    def test_an_anonymous_path_still_binds_the_gateway_tenant(self) -> None:
        client, seen = _client(anonymous_paths={"/auth/login"}, trust_tenant_header=True)
        tenant = uuid4()

        response = client.post("/auth/login", headers={"X-Tenant-Id": str(tenant)})

        assert response.status_code == 200
        assert seen["tenant"] is not None and seen["tenant"].tenant_id == tenant

    def test_a_credential_on_any_ingress_satisfies_it(self) -> None:
        # Asked of the requirement, not of an ingress: a header client is not refused for
        # lacking the cookie listed first.
        cookie = CookieTokenAuthn(authn_spec=_SPEC, cookie_name="access")
        client, seen = _client(requirement=AuthnRequirement(ingress=(cookie, _HEADER)))

        response = client.post("/orders", headers={"Authorization": "Bearer t-1"})

        assert response.status_code == 200
        assert seen["authn"] is not None

    def test_off_it_binds_no_identity_and_passes_the_request_on(self) -> None:
        client, seen = _client(requirement=AuthnRequirement(ingress=(_HEADER,), required=False))

        response = client.post("/orders")

        assert (response.status_code, seen["authn"]) == (200, None)


class TestTheAnonymousPathIsTheRoutedPath:
    """The check reads the path routing reads, not the URL Starlette rebuilds from it.

    The scope's path is already percent-decoded, and the request URL is re-split from it: an
    encoded ``?`` or ``#`` would end the URL's path early, and ``/%3F/reports`` would read as
    the anonymous ``/`` while routing serves ``/{org}/reports`` with ``org="?"``.
    """

    def _app(self) -> TestClient:
        ctx = context_from_deps(Deps.plain({AuthnDepKey: _TokenAuthFactory()}))
        app = FastAPI()

        @app.get("/")
        def landing() -> dict[str, bool]:
            return {"public": True}

        @app.get("/{org}/reports")
        def reports(org: str) -> dict[str, str]:
            return {"org": org}

        app.add_middleware(
            SecurityContextMiddleware,  # type: ignore[arg-type]
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(ingress=(_HEADER,)),
            when_multiple_credentials="first_in_order",
            anonymous_paths={"/"},
        )

        return TestClient(app)

    @pytest.mark.parametrize("path", ["/%3F/reports", "/%23/reports", "/%3f/reports"])
    def test_an_encoded_query_or_fragment_does_not_borrow_an_anonymous_path(
        self, path: str
    ) -> None:
        assert self._app().get(path).status_code == 401

    def test_the_anonymous_path_itself_is_served(self) -> None:
        assert self._app().get("/").status_code == 200
