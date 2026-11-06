"""What a load run sends, as whom, and the guard that keeps writes off shared environments."""

from __future__ import annotations

import ipaddress
import math
import os
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
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

MAX_SAMPLE_SIZE = 10_000
"""The largest page forze's list routes serve, and so the largest :func:`sample`."""


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

    @staticmethod
    def op(
        operation: str,
        *,
        name: str | None = None,
        writes: bool = False,
        path: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
        body: Any = None,
    ) -> OpScenario:
        """A scenario that calls an operation by its key, resolved from the app's OpenAPI.

        At the run's start the operation's method and path are read from the document
        (:attr:`LoadPlan.openapi`). The run stops before any load when the operation is
        missing, or when its ``x-forze-kind`` disagrees with *writes*: an operation with
        no kind, or an unknown one, counts as ``command``.

        :param operation: The operation key, the route's ``operationId``.
        :param name: Name used in mixes and in the output; the operation key by default.
        :param writes: Whether the operation changes data; it must match the route's kind.
        :param path: Values for the path's placeholders, each a value or a :func:`sample`.
        :param params: Query parameters, each a value or a :func:`sample`.
        :param body: JSON body, sent as given.
        :returns: A scenario to list in :attr:`LoadPlan.scenarios`.
        """

        return OpScenario(
            operation=operation,
            name=name or operation,
            writes=writes,
            path=dict(path or {}),
            params=dict(params or {}),
            body=body,
        )

    @staticmethod
    def reads(prefix: str = "", *, name: str = "reads") -> ReadSweep:
        """One scenario per ``query`` operation whose key starts with *prefix*.

        Generated for routes whose input is empty, a body that requires no field (a
        paginated list), or an ``id`` with an optional ``rev``. Ids are sampled per
        principal from the ``list`` operation beside the route (``products.list`` for
        ``products.get``). Other routes are recorded as skipped, never guessed, and
        ``command`` routes are never generated.

        :param prefix: Operation key prefix, matched as a string (``"products."``).
        :param name: Name a mix uses for the group: its weight is split evenly between the
            generated scenarios, which the output names by operation key.
        :returns: A group to list in :attr:`LoadPlan.scenarios`.
        """

        return ReadSweep(name=name, prefix=prefix)


# ....................... #


@attrs.frozen
class Sample:
    """Values of one field, read from a list operation as each principal before load."""

    operation: str
    """The list operation's key; it must be a ``query``."""

    field: str = "id"
    """The field read from each record of the answer's ``hits``."""

    size: int = 500
    """Records asked for, at most :data:`MAX_SAMPLE_SIZE`."""

    def __attrs_post_init__(self) -> None:
        if (
            isinstance(self.size, bool)
            or not isinstance(self.size, int)
            or not 1 <= self.size <= MAX_SAMPLE_SIZE
        ):
            raise _invalid(f"sample size must be a whole number from 1 to {MAX_SAMPLE_SIZE}")


def sample(operation: str, *, field: str = "id", size: int = 500) -> Sample:
    """Ids read once per principal from *operation*, for a :meth:`Scenario.op` to call with.

    The list operation runs as each principal that uses the sample, after sign-in and
    before the first step, and keeps the values that principal can see. An empty sample
    stops the run before load. A value that later answers 404 leaves the sample; a sample
    emptied that way retires its scenarios for that principal. Sampled values never reach
    the output.

    :param operation: Key of a ``query`` list operation.
    :param field: Field read from each record.
    :param size: Records asked for, sent as the body's or the query's ``size``.
    :returns: A value to pass in :meth:`Scenario.op`'s ``path`` or ``params``.
    """

    return Sample(operation, field, size)


@attrs.frozen(kw_only=True)
class OpScenario:
    """An operation reference, built by :meth:`Scenario.op`; resolved when a run starts."""

    operation: str
    name: str
    writes: bool
    path: Mapping[str, Any]
    params: Mapping[str, Any]
    body: Any


@attrs.frozen(kw_only=True)
class ReadSweep:
    """A group of generated read scenarios, built by :meth:`Scenario.reads`."""

    name: str
    prefix: str

    @property
    def writes(self) -> bool:
        """Never: only ``query`` operations are generated."""

        return False


type PlanScenario = Scenario | OpScenario | ReadSweep
"""What :attr:`LoadPlan.scenarios` lists: app-written scenarios and operation references."""


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

    scenarios: Sequence[PlanScenario]
    """What the principals' mixes pick from: :class:`Scenario` values and the operation
    references of :meth:`Scenario.op` and :meth:`Scenario.reads`."""

    openapi: str | Path | Mapping[str, Any] = "/openapi.json"
    """Where operation references are resolved: a path on the target, a local file
    (:class:`~pathlib.Path`) for a deployment that serves no docs, or the document itself.
    Read only when the plan has an operation reference."""

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

    Operation references are checked against the app's OpenAPI document when a run starts,
    still before any sign-in (:func:`forze_load.run`).
    """

    host = _check_url(plan.base_url)

    if not (math.isfinite(plan.timeout_s) and plan.timeout_s > 0):
        raise _invalid("timeout_s must be a positive, finite number of seconds")

    if isinstance(plan.openapi, str):
        require_path(plan.openapi, "LoadPlan.openapi")

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
        # `port` is parsed lazily: read it here, or `localhost:bad` passes and httpx fails later.
        _ = parts.port

    except ValueError as error:  # a malformed bracketed host or port, say
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
