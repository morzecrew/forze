"""Credentials: env-only secrets, one refresh per principal, sign-in again on a revoked family."""

from __future__ import annotations

import asyncio
import json
import secrets

import httpx
import pytest

from forze.base.exceptions import CoreException
from forze_load import api_key, bearer, password_login
from forze_load.auth import PasswordLogin

pytestmark = pytest.mark.unit

# ----------------------- #


class _AuthServer:
    """Forze's login and refresh routes, with refresh-token reuse revoking the family."""

    def __init__(self, *, lifetime: int = 900, password: str = "") -> None:
        self.lifetime = lifetime
        self.password = password
        self.logins = 0
        self.refreshes = 0
        self.revoked = False
        self.live_refresh: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)

        if request.url.path == "/login":
            if body["password"] != self.password:
                return httpx.Response(401)

            self.logins += 1
            self.revoked = False

            return self._tokens()

        if self.revoked or body["refresh_token"] != self.live_refresh:
            self.revoked = True  # a rotated token presented again revokes the family

            return httpx.Response(401)

        self.refreshes += 1

        return self._tokens()

    def _tokens(self) -> httpx.Response:
        self.live_refresh = secrets.token_hex(8)

        return httpx.Response(
            200,
            json={
                "access_token": secrets.token_hex(8),
                "refresh_token": self.live_refresh,
                "access_expires_in": self.lifetime,
            },
        )


def _client(server: _AuthServer) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url="http://app", transport=httpx.MockTransport(server.handler))


# ....................... #


class TestPasswordLogin:
    async def test_concurrent_workers_trigger_exactly_one_refresh(self, monkeypatch) -> None:
        password = secrets.token_hex(8)
        monkeypatch.setenv("LOAD_PW", password)
        server = _AuthServer(lifetime=1, password=password)
        auth = password_login("ada", password_env="LOAD_PW")

        async with _client(server) as client:
            first = await auth.headers(client)
            auth._renew_at = 0.0  # pyright: ignore[reportPrivateUsage]  # force the renewal window

            results = await asyncio.gather(*(auth.headers(client) for _ in range(50)))

        assert server.logins == 1
        assert server.refreshes == 1
        assert not server.revoked
        assert len({r["Authorization"] for r in results}) == 1
        assert results[0] != first

    async def test_a_revoked_family_signs_in_again(self, monkeypatch) -> None:
        password = secrets.token_hex(8)
        monkeypatch.setenv("LOAD_PW", password)
        server = _AuthServer(password=password)
        auth = password_login("ada", password_env="LOAD_PW")

        async with _client(server) as client:
            await auth.headers(client)
            server.live_refresh = "rotated-elsewhere"
            auth._renew_at = 0.0  # pyright: ignore[reportPrivateUsage]
            await auth.headers(client)

        assert server.logins == 2

    async def test_a_failed_login_is_refused_with_its_status(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", "wrong")
        server = _AuthServer(password=secrets.token_hex(8))

        async with _client(server) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").headers(client)

        assert error.value.code == "load_login_failed"
        assert "401" in error.value.summary

    async def test_a_cookie_only_response_is_refused(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", "pw")

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"access_token": None, "access_expires_in": 900})

        transport = httpx.MockTransport(handler)

        async with httpx.AsyncClient(base_url="http://app", transport=transport) as client:
            with pytest.raises(CoreException, match="header carrier"):
                await password_login("ada", password_env="LOAD_PW").headers(client)

    async def test_a_failing_refresh_other_than_401_is_refused(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", "pw")

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/refresh":
                return httpx.Response(500)

            return httpx.Response(200, json={"access_token": "a", "refresh_token": "r"})

        auth = password_login("ada", password_env="LOAD_PW")
        transport = httpx.MockTransport(handler)

        async with httpx.AsyncClient(base_url="http://app", transport=transport) as client:
            await auth.headers(client)
            auth._renew_at = 0.0  # pyright: ignore[reportPrivateUsage]

            with pytest.raises(CoreException) as error:
                await auth.headers(client)

        assert error.value.code == "load_refresh_failed"

    def test_the_repr_carries_no_token(self) -> None:
        auth = PasswordLogin(login="ada", password_env="LOAD_PW")
        auth._access = "secret-access"  # pyright: ignore[reportPrivateUsage]

        assert "secret-access" not in repr(auth)

    def test_the_margin_is_checked(self) -> None:
        with pytest.raises(CoreException):
            PasswordLogin(login="ada", password_env="X", refresh_margin=1.0)


class TestEnvCredentials:
    async def test_api_key_and_bearer_read_the_environment(self, monkeypatch) -> None:
        key = secrets.token_hex(8)
        monkeypatch.setenv("LOAD_KEY", key)
        monkeypatch.setenv("LOAD_TOKEN", key)

        async with httpx.AsyncClient() as client:
            assert await api_key("LOAD_KEY").headers(client) == {"X-API-Key": key}
            assert await api_key("LOAD_KEY", header="X-Key").headers(client) == {"X-Key": key}
            assert await bearer("LOAD_TOKEN").headers(client) == {"Authorization": f"Bearer {key}"}

    async def test_a_missing_variable_is_named_and_refused(self, monkeypatch) -> None:
        monkeypatch.delenv("LOAD_KEY", raising=False)

        async with httpx.AsyncClient() as client:
            with pytest.raises(CoreException) as error:
                await api_key("LOAD_KEY").headers(client)

        assert error.value.code == "load_credential_missing"
        assert "LOAD_KEY" in error.value.summary
