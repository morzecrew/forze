"""Credentials for load-test principals.

Every secret is read from an environment variable when the principal first signs in, never
from the plan's source, and never kept in a ``repr``. A credential opens one session per run
(:meth:`Auth.session`), which all of the principal's workers share: forze rotates refresh
tokens with no grace by default, so two workers refreshing the same token would revoke each
other's session family. A plan reused for a second run signs in again.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol, runtime_checkable

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.primitives import monotonic

from ._target import require_path

# ----------------------- #

type HeadersFn = Callable[[httpx.AsyncClient], Awaitable[Mapping[str, str]]]
"""An app-supplied credential: given the run's auth client, return the headers to send."""

RETRY_FIRST_S = 0.5
"""First wait before retrying a refresh that failed in transport or with a 5xx."""

RETRY_MAX_S = 30.0
"""Longest wait between refresh retries."""


@runtime_checkable
class AuthSession(Protocol):
    """One run's credential state: yields the headers a principal's requests carry."""

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        """Return the headers for the next request, signing in or refreshing when needed."""
        ...


@runtime_checkable
class Auth(Protocol):
    """A credential a principal carries across runs; each run opens its own session."""

    def session(self) -> AuthSession:
        """Open a session for one run, with no state from an earlier run."""
        ...


# ....................... #


def read_env(name: str) -> str:
    """Return the environment variable *name*, refusing a missing or empty one.

    :raises CoreException: ``configuration`` (``load_credential_missing``) — the variable is
        not set. The message names the variable, never a value.
    """

    value = os.environ.get(name)

    if not value:
        raise exc.configuration(
            f"Environment variable {name} is not set",
            code="load_credential_missing",
        )

    return value


# ....................... #


@attrs.frozen
class ApiKeyAuth:
    """An API key read from an environment variable, sent in a header (``X-API-Key``)."""

    key_env: str
    """Environment variable holding the key."""

    header: str = "X-API-Key"
    """Header that carries the key."""

    # ....................... #

    def session(self) -> AuthSession:
        return self

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        return {self.header: read_env(self.key_env)}


# ....................... #


@attrs.frozen
class BearerAuth:
    """A pre-issued bearer token read from an environment variable."""

    token_env: str
    """Environment variable holding the token."""

    # ....................... #

    def session(self) -> AuthSession:
        return self

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {read_env(self.token_env)}"}


# ....................... #


@attrs.frozen
class PasswordLogin:
    """Sign in through forze's password routes and keep the access token fresh.

    ``POST {login_path}`` with ``{login, password}`` and ``POST {refresh_path}`` with
    ``{refresh_token}``, as ``attach_authn_routes`` serves them in header mode. Holds no
    state itself: :meth:`session` opens a fresh one per run.
    """

    login: str
    """Login name."""

    password_env: str
    """Environment variable holding the password."""

    login_path: str = attrs.field(default="/login")
    """Path of the login route, relative to the plan's base URL; a URL with a host is refused,
    so the password and refresh token never leave the target."""

    refresh_path: str = attrs.field(default="/refresh")
    """Path of the refresh route, relative to the plan's base URL; same rule."""

    refresh_margin: float = attrs.field(default=0.1)
    """Share of the access token's lifetime left when it is renewed."""

    # ....................... #

    @refresh_margin.validator  # pyright: ignore[reportAttributeAccessIssue, reportUntypedFunctionDecorator]
    def _check_margin(self, _attribute: Any, value: float) -> None:
        if not 0.0 <= value < 1.0:
            raise exc.configuration("refresh_margin must be in [0, 1)")

    @login_path.validator  # pyright: ignore[reportAttributeAccessIssue, reportUntypedFunctionDecorator]
    @refresh_path.validator  # pyright: ignore[reportAttributeAccessIssue, reportUntypedFunctionDecorator]
    def _check_path(self, attribute: Any, value: str) -> None:
        require_path(value, f"PasswordLogin.{attribute.name}")

    def session(self) -> AuthSession:
        return PasswordSession(config=self)


@attrs.define
class PasswordSession:
    """One run's tokens for a :class:`PasswordLogin`.

    The access token is renewed once ``1 - refresh_margin`` of its lifetime has passed, under
    a lock, so the principal's workers trigger one refresh between them. A refresh answered
    ``401`` signs in again. A refresh that fails in transport or with a 5xx keeps the current
    token while it is valid and retries with backoff (:data:`RETRY_FIRST_S` doubling to
    :data:`RETRY_MAX_S`); once the token has expired the failure stops the run.
    """

    config: PasswordLogin = attrs.field(repr=False)
    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False, repr=False)
    _access: str | None = attrs.field(default=None, init=False, repr=False)
    _refresh: str | None = attrs.field(default=None, init=False, repr=False)
    _renew_at: float = attrs.field(default=0.0, init=False, repr=False)
    _expires_at: float = attrs.field(default=0.0, init=False, repr=False)
    _retry_at: float = attrs.field(default=0.0, init=False, repr=False)
    _backoff: float = attrs.field(default=RETRY_FIRST_S, init=False, repr=False)

    # ....................... #

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        if self._access is None or self._due():
            async with self._lock:
                # Re-checked under the lock: the worker ahead of this one may have renewed.
                if self._access is None:
                    await self._sign_in(client)

                elif self._due():
                    await self._renew(client)

        return {"Authorization": f"Bearer {self._access}"}

    def _due(self) -> bool:
        now = monotonic()

        return now >= self._renew_at and (now >= self._retry_at or now >= self._expires_at)

    # ....................... #

    async def _sign_in(self, client: httpx.AsyncClient) -> None:
        body = {"login": self.config.login, "password": read_env(self.config.password_env)}

        try:
            response = await client.post(self.config.login_path, json=body)

        except httpx.TransportError as error:
            reason = f"a transport error ({type(error).__name__})"
            raise self._failed("Signing in", reason) from error

        if response.status_code != 200:
            raise self._failed("Signing in", f"HTTP {response.status_code}")

        self._take(response)

    async def _renew(self, client: httpx.AsyncClient) -> None:
        try:
            if self._refresh is None:
                await self._sign_in(client)
                return

            response = await client.post(
                self.config.refresh_path, json={"refresh_token": self._refresh}
            )

        except httpx.TransportError as error:
            self._retry_later(f"a transport error ({type(error).__name__})")
            return

        if response.status_code == 401:
            await self._sign_in(client)
            return

        if response.status_code >= 500:
            self._retry_later(f"HTTP {response.status_code}")
            return

        if response.status_code != 200:
            raise self._failed("Refreshing", f"HTTP {response.status_code}", "load_refresh_failed")

        self._take(response)

    def _retry_later(self, reason: str) -> None:
        now = monotonic()

        if now >= self._expires_at:
            raise self._failed(
                "Refreshing", f"{reason} after the token expired", "load_refresh_failed"
            )

        self._retry_at = now + self._backoff
        self._backoff = min(self._backoff * 2.0, RETRY_MAX_S)

    def _take(self, response: httpx.Response) -> None:
        try:
            body = response.json()

        except ValueError:
            raise self._failed("Signing in", "a response that is not JSON") from None

        access = body.get("access_token") if isinstance(body, dict) else None

        if not isinstance(access, str) or not access:
            raise self._failed(
                "Signing in",
                "a token response with no access_token: password_login needs the header "
                "carrier, not cookies",
            )

        now = monotonic()
        lifetime = float(body.get("access_expires_in") or 900)
        self._access = access
        self._refresh = body.get("refresh_token")
        self._renew_at = now + lifetime * (1.0 - self.config.refresh_margin)
        self._expires_at = now + lifetime
        self._retry_at = 0.0
        self._backoff = RETRY_FIRST_S

    def _failed(self, action: str, reason: str, code: str = "load_login_failed") -> Exception:
        return exc.configuration(f"{action} as {self.config.login!r} failed: {reason}", code=code)


# ....................... #


def password_login(
    login: str,
    *,
    password_env: str,
    login_path: str = "/login",
    refresh_path: str = "/refresh",
) -> PasswordLogin:
    """Sign in through forze's ``/login`` and ``/refresh`` routes.

    :param login: Login name.
    :param password_env: Environment variable holding the password.
    :param login_path: Path of the login route, relative to the plan's base URL.
    :param refresh_path: Path of the refresh route.
    :returns: A credential to pass as :attr:`Principal.auth`.
    """

    return PasswordLogin(
        login=login,
        password_env=password_env,
        login_path=login_path,
        refresh_path=refresh_path,
    )


def api_key(key_env: str, *, header: str = "X-API-Key") -> ApiKeyAuth:
    """Send the API key held in *key_env* in *header*.

    :returns: A credential to pass as :attr:`Principal.auth`.
    """

    return ApiKeyAuth(key_env=key_env, header=header)


def bearer(token_env: str) -> BearerAuth:
    """Send the bearer token held in *token_env*.

    :returns: A credential to pass as :attr:`Principal.auth`.
    """

    return BearerAuth(token_env=token_env)
