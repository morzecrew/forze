"""Credentials for load-test principals.

Every secret is read from an environment variable when the principal first signs in, never
from the plan's source, and never kept in a ``repr``. A principal holds one session that all
of its workers share: forze rotates refresh tokens with no grace by default, so two workers
refreshing the same token would revoke each other's session family.
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

# ----------------------- #

type HeadersFn = Callable[[httpx.AsyncClient], Awaitable[Mapping[str, str]]]
"""An app-supplied credential: given the run's client, return the request headers to send."""


@runtime_checkable
class Auth(Protocol):
    """A credential that yields the headers a principal's requests carry."""

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        """Return the headers for the next request, signing in or refreshing when needed."""
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

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        return {self.header: read_env(self.key_env)}


# ....................... #


@attrs.frozen
class BearerAuth:
    """A pre-issued bearer token read from an environment variable."""

    token_env: str
    """Environment variable holding the token."""

    # ....................... #

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {read_env(self.token_env)}"}


# ....................... #


@attrs.define
class PasswordLogin:
    """Sign in through forze's password routes and keep the access token fresh.

    ``POST {login_path}`` with ``{login, password}`` and ``POST {refresh_path}`` with
    ``{refresh_token}``, as ``attach_authn_routes`` serves them in header mode. The access
    token is renewed once ``1 - refresh_margin`` of its lifetime has passed, under a lock,
    so one principal's workers trigger exactly one refresh between them. A refresh answered
    ``401`` signs in again.
    """

    login: str
    """Login name."""

    password_env: str
    """Environment variable holding the password."""

    login_path: str = "/login"
    """Path of the login route, relative to the plan's base URL."""

    refresh_path: str = "/refresh"
    """Path of the refresh route, relative to the plan's base URL."""

    refresh_margin: float = attrs.field(default=0.1)
    """Share of the access token's lifetime left when it is renewed."""

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False, repr=False)
    _access: str | None = attrs.field(default=None, init=False, repr=False)
    _refresh: str | None = attrs.field(default=None, init=False, repr=False)
    _renew_at: float = attrs.field(default=0.0, init=False, repr=False)

    # ....................... #

    @refresh_margin.validator  # pyright: ignore[reportAttributeAccessIssue, reportUntypedFunctionDecorator]
    def _check_margin(self, _attribute: Any, value: float) -> None:
        if not 0.0 <= value < 1.0:
            raise exc.configuration("refresh_margin must be in [0, 1)")

    # ....................... #

    async def headers(self, client: httpx.AsyncClient) -> Mapping[str, str]:
        if self._access is None or monotonic() >= self._renew_at:
            async with self._lock:
                # Re-checked under the lock: the worker ahead of this one may have renewed.
                if self._access is None:
                    await self._sign_in(client)

                elif monotonic() >= self._renew_at:
                    await self._renew(client)

        return {"Authorization": f"Bearer {self._access}"}

    # ....................... #

    async def _sign_in(self, client: httpx.AsyncClient) -> None:
        body = {"login": self.login, "password": read_env(self.password_env)}
        response = await client.post(self.login_path, json=body)

        if response.status_code != 200:
            raise exc.configuration(
                f"Signing in as {self.login!r} failed with HTTP {response.status_code}",
                code="load_login_failed",
            )

        self._take(response)

    async def _renew(self, client: httpx.AsyncClient) -> None:
        if self._refresh is None:
            await self._sign_in(client)
            return

        response = await client.post(self.refresh_path, json={"refresh_token": self._refresh})

        if response.status_code == 401:
            await self._sign_in(client)
            return

        if response.status_code != 200:
            raise exc.configuration(
                f"Refreshing {self.login!r} failed with HTTP {response.status_code}",
                code="load_refresh_failed",
            )

        self._take(response)

    def _take(self, response: httpx.Response) -> None:
        body = response.json()
        access = body.get("access_token")

        if not access:
            raise exc.configuration(
                "The token response carries no access_token: password_login needs the "
                "header carrier, not cookies",
                code="load_login_failed",
            )

        self._access = access
        self._refresh = body.get("refresh_token")
        lifetime = float(body.get("access_expires_in") or 900)
        self._renew_at = monotonic() + lifetime * (1.0 - self.refresh_margin)


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
