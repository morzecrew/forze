"""A request no ingress authenticates is refused unless its path is meant to be anonymous.

The middleware used to bind no identity for such a request and pass it on, so a route with no
``AuthnRequired`` hook — any hand-written route — served it anonymously by default.
"""

from __future__ import annotations

from typing import Any

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

        response = client.options("/orders")

        assert (response.status_code, seen["authn"]) == (200, None)

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
