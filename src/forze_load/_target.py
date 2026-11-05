"""What keeps a run's requests, and every credential they carry, on the plan's target."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from http.cookiejar import CookieJar, DefaultCookiePolicy
from urllib.parse import urlsplit

import httpx

from forze.base.exceptions import exc

# ----------------------- #


def require_path(url: str, what: str) -> None:
    """Refuse *url* unless it is a path relative to the plan's base URL.

    :raises CoreException: ``configuration`` (``load_plan_invalid``) — *url* names a scheme
        or a host, so it could carry a credential to another host.
    """

    try:
        parts = urlsplit(url)

    except ValueError as error:  # a malformed bracketed host, say
        raise exc.configuration(
            f"{what} got a URL that cannot be parsed", code="load_plan_invalid"
        ) from error

    if parts.scheme or parts.netloc:
        raise exc.configuration(
            f"{what} takes a path relative to the plan's base URL, not a URL with a scheme "
            "or a host",
            code="load_plan_invalid",
        )


def target_guard(base_url: str) -> Callable[[httpx.Request], Awaitable[None]]:
    """An httpx request hook that refuses any request to another origin than *base_url*.

    It runs on every request a run's clients send, so a scenario or a credential callable
    that builds an absolute URL itself, or a redirect, is refused before anything is sent.
    httpx normalizes the host's case and drops a scheme's default port, so the origins
    compare as they are.
    """

    target = _origin(httpx.URL(base_url))

    async def refuse_off_target(request: httpx.Request) -> None:
        if _origin(request.url) != target:
            raise exc.configuration(
                "A request to another host than the plan's target was refused before sending",
                code="load_plan_invalid",
            )

    return refuse_off_target


def no_cookies() -> CookieJar:
    """A cookie jar that keeps nothing.

    A run's clients are shared by every principal, so a ``Set-Cookie`` kept for one would be
    sent with another's requests. A cookie-mode app gets its cookie from a credential
    callable, as an explicit ``Cookie`` header of that principal only. It is a bare
    ``CookieJar`` on purpose: httpx keeps a jar it is given, but copies an ``httpx.Cookies``
    into a fresh jar without the policy.
    """

    return CookieJar(policy=DefaultCookiePolicy(allowed_domains=[]))


def _origin(url: httpx.URL) -> tuple[str, str, int | None]:
    return url.scheme, url.host, url.port
