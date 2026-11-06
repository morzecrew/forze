"""Credentials: env-only secrets, one session per run, one refresh per principal, backoff."""

from __future__ import annotations

import asyncio
import json
import secrets

import httpx
import pytest

import forze_load.auth as auth_module
from forze.base.exceptions import CoreException
from forze_load import api_key, bearer, password_login
from forze_load.auth import RETRY_FIRST_S, PasswordLogin, PasswordSession

pytestmark = pytest.mark.unit

# ----------------------- #


class _AuthServer:
    """Forze's login and refresh routes, with refresh-token reuse revoking the family."""

    def __init__(self, *, lifetime: int = 900, password: str = "") -> None:
        self.lifetime = lifetime
        self.password = password
        self.logins = 0
        self.refreshes = 0
        self.refresh_attempts = 0
        self.revoked = False
        self.live_refresh: str | None = None
        self.refresh_fails_with: Exception | int | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)

        if request.url.path == "/login":
            if body["password"] != self.password:
                return httpx.Response(401)

            self.logins += 1
            self.revoked = False

            return self._tokens()

        self.refresh_attempts += 1

        if isinstance(self.refresh_fails_with, Exception):
            raise self.refresh_fails_with

        if isinstance(self.refresh_fails_with, int):
            return httpx.Response(self.refresh_fails_with)

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


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _client(handler) -> httpx.AsyncClient:  # type: ignore[no-untyped-def]
    return httpx.AsyncClient(base_url="http://app", transport=httpx.MockTransport(handler))


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(auth_module, "monotonic", fake)

    return fake


@pytest.fixture
def password(monkeypatch: pytest.MonkeyPatch) -> str:
    value = secrets.token_hex(8)
    monkeypatch.setenv("LOAD_PW", value)

    return value


# ....................... #


class TestPasswordSession:
    async def test_concurrent_workers_trigger_exactly_one_refresh(
        self, password: str, clock: _Clock
    ) -> None:
        server = _AuthServer(lifetime=10, password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            first = await session.headers(client)
            clock.now += 9.5  # past the renewal point, before expiry

            results = await asyncio.gather(*(session.headers(client) for _ in range(50)))

        assert server.logins == 1
        assert server.refreshes == 1
        assert not server.revoked
        assert len({r["Authorization"] for r in results}) == 1
        assert results[0] != first

    async def test_each_run_opens_a_fresh_session(self, password: str) -> None:
        server = _AuthServer(password=password)
        credential = password_login("ada", password_env="LOAD_PW")

        async with _client(server.handler) as client:
            await credential.session().headers(client)
            await credential.session().headers(client)

        assert server.logins == 2

    async def test_a_refresh_outlives_a_caller_cancelled_mid_flight(
        self, password: str, clock: _Clock
    ) -> None:
        server = _AuthServer(lifetime=10, password=password)
        rotated, release = asyncio.Event(), asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            response = server.handler(request)  # a refresh rotates the token here

            if request.url.path != "/login":
                rotated.set()
                await release.wait()

            return response

        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(handler) as client:
            await session.headers(client)
            clock.now += 9.5
            caller = asyncio.create_task(session.headers(client))
            await rotated.wait()
            caller.cancel()  # a storm's drop, mid-refresh
            release.set()
            await asyncio.gather(caller, return_exceptions=True)
            await asyncio.sleep(0.01)  # the shielded refresh finishes and keeps the new token
            clock.now += 9.5
            await session.headers(client)

        # The second refresh presented the rotated token: no reuse, no new sign-in.
        assert server.refreshes == 2
        assert server.logins == 1

    async def test_settle_raises_a_refresh_that_failed_after_its_caller_left(
        self, password: str, clock: _Clock
    ) -> None:
        server = _AuthServer(lifetime=10, password=password)
        started, release = asyncio.Event(), asyncio.Event()

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path != "/login":
                started.set()
                await release.wait()
                server.refresh_fails_with = 503

            return server.handler(request)

        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(handler) as client:
            await session.headers(client)
            clock.now += 11  # past expiry: a failed refresh can no longer keep the token
            caller = asyncio.create_task(session.headers(client))
            await started.wait()
            caller.cancel()
            await asyncio.gather(caller, return_exceptions=True)
            release.set()
            await asyncio.sleep(0.01)  # it fails with no caller left to tell

            with pytest.raises(CoreException) as failed:
                await session.settle()

        assert failed.value.code == "load_refresh_failed"

    async def test_settle_returns_at_once_with_nothing_pending(self, password: str) -> None:
        server = _AuthServer(password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            await session.headers(client)
            await session.settle()

        assert server.logins == 1

    async def test_a_revoked_family_signs_in_again(self, password: str, clock: _Clock) -> None:
        server = _AuthServer(lifetime=10, password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            await session.headers(client)
            server.live_refresh = "rotated-elsewhere"
            clock.now += 9.5
            await session.headers(client)

        assert server.logins == 2

    @pytest.mark.parametrize("failure", [httpx.ConnectError("refused"), 503])
    async def test_a_failing_refresh_backs_off_then_stops_once_expired(
        self, password: str, clock: _Clock, failure: Exception | int
    ) -> None:
        server = _AuthServer(lifetime=10, password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            token = await session.headers(client)
            server.refresh_fails_with = failure
            clock.now += 9.0  # the renewal point

            for _ in range(20):  # the current token is still valid: keep it, no hammering
                assert await session.headers(client) == token

            assert server.refresh_attempts == 1

            clock.now += RETRY_FIRST_S
            assert await session.headers(client) == token
            assert server.refresh_attempts == 2

            clock.now += 1.0  # past expiry

            with pytest.raises(CoreException) as error:
                await session.headers(client)

        assert error.value.code == "load_refresh_failed"
        assert "expired" in error.value.summary

    async def test_a_zero_lifetime_renews_on_the_next_call(
        self, password: str, clock: _Clock
    ) -> None:
        server = _AuthServer(lifetime=0, password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            await session.headers(client)
            await session.headers(client)

        assert server.refreshes == 1  # not read as the 900 s default

    @pytest.mark.parametrize("lifetime", ["Infinity", "NaN"])
    async def test_a_lifetime_that_is_not_finite_is_refused(
        self, password: str, lifetime: str
    ) -> None:
        # Raw text: Python's json reads these non-standard literals, httpx will not write them.
        body = f'{{"access_token": "a", "refresh_token": "r", "access_expires_in": {lifetime}}}'
        response = httpx.Response(200, text=body, headers={"content-type": "application/json"})

        async with _client(lambda _request: response) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").session().headers(client)

        assert error.value.code == "load_login_failed"

    async def test_a_lifetime_that_is_not_a_number_is_refused(self, password: str) -> None:
        body = {"access_token": "a", "refresh_token": "r", "access_expires_in": "soon"}

        async with _client(lambda _request: httpx.Response(200, json=body)) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").session().headers(client)

        assert error.value.code == "load_login_failed"

    async def test_a_failed_login_is_refused_with_its_status(self, monkeypatch) -> None:
        monkeypatch.setenv("LOAD_PW", "wrong")
        server = _AuthServer(password=secrets.token_hex(8))

        async with _client(server.handler) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").session().headers(client)

        assert error.value.code == "load_login_failed"
        assert "401" in error.value.summary

    async def test_a_transport_error_at_sign_in_is_refused(self, password: str) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        async with _client(handler) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").session().headers(client)

        assert error.value.code == "load_login_failed"
        assert "ConnectError" in error.value.summary

    @pytest.mark.parametrize(
        "response",
        [
            httpx.Response(200, text="<html>not json</html>"),
            httpx.Response(200, json=["a list"]),
            httpx.Response(200, json={"access_token": None, "access_expires_in": 900}),
        ],
    )
    async def test_a_body_without_a_token_is_refused(
        self, password: str, response: httpx.Response
    ) -> None:
        async with _client(lambda _request: response) as client:
            with pytest.raises(CoreException) as error:
                await password_login("ada", password_env="LOAD_PW").session().headers(client)

        assert error.value.code == "load_login_failed"

    async def test_another_refresh_status_is_refused(self, password: str, clock: _Clock) -> None:
        server = _AuthServer(lifetime=10, password=password)
        session = password_login("ada", password_env="LOAD_PW").session()

        async with _client(server.handler) as client:
            await session.headers(client)
            server.refresh_fails_with = 400
            clock.now += 9.5

            with pytest.raises(CoreException) as error:
                await session.headers(client)

        assert error.value.code == "load_refresh_failed"

    def test_the_repr_carries_no_token(self) -> None:
        session = PasswordSession(config=PasswordLogin(login="ada", password_env="LOAD_PW"))
        session._access = "secret-access"  # pyright: ignore[reportPrivateUsage]

        assert "secret-access" not in repr(session)

    def test_the_margin_is_checked(self) -> None:
        with pytest.raises(CoreException):
            PasswordLogin(login="ada", password_env="X", refresh_margin=1.0)


class TestEnvCredentials:
    async def test_api_key_and_bearer_read_the_environment(self, monkeypatch) -> None:
        key = secrets.token_hex(8)
        monkeypatch.setenv("LOAD_KEY", key)
        monkeypatch.setenv("LOAD_TOKEN", key)

        async with httpx.AsyncClient() as client:
            assert await api_key("LOAD_KEY").session().headers(client) == {"X-API-Key": key}
            assert await api_key("LOAD_KEY", header="X-Key").headers(client) == {"X-Key": key}
            assert await bearer("LOAD_TOKEN").session().headers(client) == {
                "Authorization": f"Bearer {key}"
            }

    async def test_a_missing_variable_is_named_and_refused(self, monkeypatch) -> None:
        monkeypatch.delenv("LOAD_KEY", raising=False)

        async with httpx.AsyncClient() as client:
            with pytest.raises(CoreException) as error:
                await api_key("LOAD_KEY").headers(client)

        assert error.value.code == "load_credential_missing"
        assert "LOAD_KEY" in error.value.summary
