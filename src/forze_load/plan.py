"""What a load run sends, as whom, and the guard that keeps writes off shared environments."""

from __future__ import annotations

import ipaddress
import math
import os
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import attrs
import httpx

from forze.base.exceptions import exc

from ._target import require_path
from .auth import Auth, AuthSession, HeadersFn

# ----------------------- #

WRITE_HOSTS_ENV = "FORZE_LOAD_WRITE_HOSTS"
"""Comma-separated hosts a write-enabled run may target, beside loopback and the plan's own."""

TENANT_HEADER = "X-Tenant-Id"
"""The tenant hint header forze reads (``forze.application.contracts.tenancy``)."""


# ....................... #


@attrs.frozen(kw_only=True)
class Call:
    """One request's context, handed to a scenario's ``send``."""

    client: httpx.AsyncClient
    """The run's shared client, already pointed at the plan's base URL."""

    headers: Mapping[str, str]
    """The principal's credential and tenant headers."""

    principal: str
    """Name of the principal this request runs as."""

    rng: random.Random
    """The run's seeded random source, for picking ids or payload variants."""

    # ....................... #

    async def request(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Send one request with the principal's headers merged under *headers*.

        *url* is a path relative to the plan's base URL. An absolute URL, or one starting
        with ``//``, is refused before anything is sent: it would leave the target the write
        guard checked and carry the principal's credential to another host (a pagination
        ``next`` link, say).

        :raises CoreException: ``configuration`` (``load_plan_invalid``) — *url* names a
            scheme or a host.
        """

        require_path(url, "Call.request")
        merged = {**self.headers, **(headers or {})}

        return await self.client.request(method, url, json=json, params=params, headers=merged)


type SendFn = Callable[[Call], Awaitable[httpx.Response]]
"""A scenario's body: send one request through :class:`Call` and return its response."""


# ....................... #


@attrs.frozen
class Scenario:
    """A named request a principal's mix can pick.

    ``writes`` has no default on purpose: every scenario states whether it changes data, and
    a run refuses one that does unless the plan allows writes (:func:`validate_plan`). It is
    a declaration the kit trusts, not something it can check: forze's list and search routes
    are ``POST``, so a request does not show whether it writes.
    """

    name: str
    """Name used in mixes and in the output."""

    send: SendFn
    """Sends the request."""

    writes: bool = attrs.field(kw_only=True)
    """Whether the request changes data on the target."""


# ....................... #


@attrs.frozen(kw_only=True)
class Principal:
    """A user the run signs in as, with the share of workers and the mix it runs."""

    name: str
    """Name used in the output."""

    mix: Mapping[str, int]
    """Scenario name to weight: how often this principal's workers pick each scenario."""

    auth: Auth | AuthSession | HeadersFn | None = None
    """Credential: a helper (:func:`~forze_load.api_key`, :func:`~forze_load.password_login`,
    …), an async callable taking the auth client and returning headers, or ``None``."""

    weight: int = 1
    """Share of workers (closed model) or arrivals (open model) that run as this principal."""

    tenant: str | None = None
    """Tenant sent as ``X-Tenant-Id``; ``None`` sends no header."""


# ....................... #


@attrs.frozen(kw_only=True)
class LoadPlan:
    """A load run's target, principals and scenarios."""

    base_url: str
    """The app's base URL, ``http`` or ``https``, without credentials."""

    principals: Sequence[Principal]
    """Who the run signs in as."""

    scenarios: Sequence[Scenario]
    """What the principals' mixes pick from."""

    allow_writes: bool = False
    """Allow scenarios declared ``writes=True``. Even then the host must be allowlisted."""

    write_hosts: Sequence[str] = ()
    """Hosts beside loopback a write-enabled run may target, compared exactly."""

    timeout_s: float = 10.0
    """Per-request timeout in seconds."""

    seed: int = 0
    """Seed for scenario picks, arrivals and :attr:`Call.rng`."""


# ....................... #


def validate_plan(plan: LoadPlan) -> None:
    """Refuse a plan that cannot run safely, before anything is sent.

    :raises CoreException: ``configuration`` with one of these codes:

        - ``load_plan_invalid`` — a malformed URL, mix, weight or timeout;
        - ``load_writes_refused`` — a scenario writes and the plan does not allow writes;
        - ``load_write_host_refused`` — writes are allowed but the target host is not
          loopback, ``localhost``, in ``write_hosts`` or in ``FORZE_LOAD_WRITE_HOSTS``.
    """

    host = _check_url(plan.base_url)

    if not (math.isfinite(plan.timeout_s) and plan.timeout_s > 0):
        raise _invalid("timeout_s must be a positive, finite number of seconds")

    names = [scenario.name for scenario in plan.scenarios]

    if not names or len(set(names)) != len(names):
        raise _invalid("scenarios must be non-empty with unique names")

    _check_principals(plan.principals, set(names))

    writers = sorted(scenario.name for scenario in plan.scenarios if scenario.writes)

    if not writers:
        return

    if not plan.allow_writes:
        raise exc.configuration(
            f"Scenarios {writers} write and the plan does not allow writes "
            "(set allow_writes=True against a disposable stack)",
            code="load_writes_refused",
        )

    if not write_host_allowed(host, plan.write_hosts):
        raise exc.configuration(
            f"Writes are allowed but host {host!r} is not loopback, 'localhost', in "
            f"write_hosts or in {WRITE_HOSTS_ENV}",
            code="load_write_host_refused",
        )


def write_host_allowed(host: str, write_hosts: Sequence[str] = ()) -> bool:
    """Whether a write-enabled run may target *host*.

    Allowed: a loopback address, ``localhost``, or a host named exactly (case-insensitive)
    in *write_hosts* or in ``FORZE_LOAD_WRITE_HOSTS``. Never a substring or suffix match:
    ``localhost.example.com`` is refused.
    """

    host = host.lower()

    if host == "localhost":
        return True

    try:
        if ipaddress.ip_address(host).is_loopback:
            return True

    except ValueError:
        pass

    from_env = os.environ.get(WRITE_HOSTS_ENV, "").split(",")
    allowed = {entry.strip().lower() for entry in (*write_hosts, *from_env) if entry.strip()}

    return host in allowed


def safe_target(base_url: str) -> str:
    """*base_url* as the output records it: scheme, host and port only.

    Userinfo, path, query and fragment are dropped: a base path can carry an id
    (``/tenants/123/api``).
    """

    parts = urlsplit(base_url)
    netloc = parts.netloc.rpartition("@")[2]

    return f"{parts.scheme}://{netloc}"


# ....................... #


def _check_url(base_url: str) -> str:
    try:
        parts = urlsplit(base_url)

    except ValueError as error:  # a malformed bracketed host, say
        raise _invalid("base_url cannot be parsed as a URL") from error

    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise _invalid("base_url must be an http or https URL with a host")

    if parts.username is not None or parts.password is not None:
        raise _invalid("base_url must not carry credentials; use a principal's auth")

    return parts.hostname


def _check_principals(principals: Sequence[Principal], scenarios: set[str]) -> None:
    names = [principal.name for principal in principals]

    if not names or len(set(names)) != len(names):
        raise _invalid("principals must be non-empty with unique names")

    for principal in principals:
        if principal.weight < 1:
            raise _invalid(f"principal {principal.name!r} needs a weight of at least 1")

        if not principal.mix or any(weight < 1 for weight in principal.mix.values()):
            raise _invalid(f"principal {principal.name!r} needs a mix of positive weights")

        unknown = sorted(set(principal.mix) - scenarios)

        if unknown:
            raise _invalid(f"principal {principal.name!r} mixes unknown scenarios {unknown}")


def _invalid(summary: str) -> Exception:
    return exc.configuration(summary, code="load_plan_invalid")
