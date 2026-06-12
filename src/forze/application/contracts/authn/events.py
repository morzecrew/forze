"""Authn flow events: a best-effort observability seam over the authn contracts.

Authentication flows (login, refresh, logout, password change/reset, principal
deactivation) emit :class:`AuthnEvent` records through an optional
:class:`AuthnEventSink`. Emission is **best-effort by contract**: a sink failure
must never fail the auth flow — always emit through :func:`emit_safe` (or the
route-bound :class:`AuthnEventEmitter`), which swallows and logs sink errors and
treats "no sink wired" as a no-op.

Privacy: events flow to logs and external sinks, so they carry
:func:`login_digest` — never the raw login. The same digest keys the login
lockout counters, so a locked login correlates with its ``LOGIN_FAILED`` /
``LOGIN_LOCKED`` events without ever materializing the login itself.
"""

import hashlib
from datetime import datetime
from enum import StrEnum
from typing import Awaitable, Mapping, Protocol, final, runtime_checkable
from uuid import UUID

import attrs

from forze.application._logger import logger
from forze.base.primitives import StrKey, utcnow

# ----------------------- #


class AuthnEventKind(StrEnum):
    """The kind of an authentication flow event."""

    LOGIN_SUCCEEDED = "login_succeeded"
    """A password login passed verification, resolution, and eligibility."""

    LOGIN_FAILED = "login_failed"
    """A password login failed with the uniform authentication error."""

    LOGIN_LOCKED = "login_locked"
    """A login attempt was rejected by the lockout guard before verification."""

    TOKEN_REFRESHED = "token_refreshed"  # nosec B105
    """A refresh token rotated into a fresh access/refresh pair."""

    REFRESH_REUSE_DETECTED = "refresh_reuse_detected"
    """An already-rotated refresh token was presented; its family was revoked."""

    LOGOUT = "logout"
    """All sessions of a principal were revoked on request."""

    PASSWORD_CHANGED = "password_changed"  # nosec B105
    """A principal changed their password (re-authenticated with the current one)."""

    PASSWORD_RESET_REQUESTED = "password_reset_requested"  # nosec B105
    """A reset token was actually issued (not emitted for unknown/ineligible logins)."""

    PASSWORD_RESET_COMPLETED = "password_reset_completed"  # nosec B105
    """A reset token was consumed and a new password was set."""

    PRINCIPAL_DEACTIVATED = "principal_deactivated"
    """A principal was deactivated (policy, sessions, and credentials cascaded)."""


# ....................... #


def login_digest(login: str) -> str:
    """Stable pseudonymous digest of a login for events and lockout counter keys.

    SHA-256 over ``"lockout:" + login.lower()`` (the constant prefix
    domain-separates this digest from any other SHA-256 use of logins; lowercasing
    folds case variants of the same login together). **Unpeppered by design** —
    this is pseudonymization, not secrecy: the digest keeps raw logins out of
    logs, event sinks, and counter key spaces, but an attacker who can read those
    stores and wants to confirm a *known* login can brute-force the unsalted hash
    (and an attacker with counter-store access already has worse). A pepper would
    buy little here while forcing secret distribution into every emitter.

    The same digest intentionally keys both the lockout counters and
    :attr:`AuthnEvent.login_digest`, so locked logins correlate across the two.
    """

    return hashlib.sha256(f"lockout:{login.lower()}".encode()).hexdigest()


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class AuthnEvent:
    """A single authentication flow event (immutable record).

    Carries :func:`login_digest` instead of the raw login — events flow to logs
    and external sinks, so the login itself must never appear here.
    """

    kind: AuthnEventKind
    """What happened."""

    principal_id: UUID | None = attrs.field(default=None, kw_only=True)
    """Resolved principal, when the flow knows one (``None`` for failed logins)."""

    login_digest: str | None = attrs.field(default=None, kw_only=True)
    """Pseudonymous :func:`login_digest` of the presented login — **never** the raw login."""

    tenant_id: UUID | None = attrs.field(default=None, kw_only=True)
    """Tenant bound to the credential/session, when known."""

    route: StrKey = attrs.field(kw_only=True)
    """Authn route name (:attr:`AuthnSpec.name <forze.application.contracts.base.BaseSpec.name>`) the event originates from."""

    occurred_at: datetime = attrs.field(kw_only=True)
    """When the event happened (UTC; reads the bound time source at emit time)."""

    details: Mapping[str, str] = attrs.field(factory=dict[str, str], kw_only=True)
    """Optional extra context. Values must already be privacy-safe."""


# ....................... #


@runtime_checkable
class AuthnEventSink(Protocol):
    """Receiver of authentication flow events.

    Implementations should be cheap and non-blocking-ish (a structured log line,
    an in-memory append, a fire-and-forget enqueue). Callers always invoke sinks
    through :func:`emit_safe`, so a raising sink degrades to a logged warning —
    but well-behaved sinks avoid raising in the first place.
    """

    def record(self, event: AuthnEvent) -> Awaitable[None]:
        """Record a single event."""
        ...  # pragma: no cover


# ....................... #


async def emit_safe(sink: AuthnEventSink | None, event: AuthnEvent) -> None:
    """Emit ``event`` best-effort: no sink is a no-op, a sink failure is a warning.

    This is the single emission path for auth flows — an observability sink
    must never break login, refresh, or password flows.
    """

    if sink is None:
        return

    try:
        await sink.record(event)

    except (
        Exception
    ) as e:  # noqa: BLE001 — emission is best-effort; auth flows must not fail
        logger.warning(
            "Authn event sink failed to record '%s' on route '%s': %s",
            str(event.kind),
            str(event.route),
            e,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnEventEmitter:
    """Route-bound convenience over an :class:`AuthnEventSink`.

    Adapters hold ``events: AuthnEventEmitter | None`` (``None`` = emission off)
    instead of a ``(sink, route)`` pair; :meth:`emit` stamps the route and the
    current time and delegates to :func:`emit_safe`, so it never raises.
    """

    sink: AuthnEventSink
    """Sink receiving the events."""

    route: StrKey
    """Authn route name stamped on every emitted event."""

    # ....................... #

    async def emit(
        self,
        kind: AuthnEventKind,
        *,
        principal_id: UUID | None = None,
        login_digest: str | None = None,
        tenant_id: UUID | None = None,
        details: Mapping[str, str] | None = None,
    ) -> None:
        """Build and best-effort-emit an :class:`AuthnEvent` for this route."""

        await emit_safe(
            self.sink,
            AuthnEvent(
                kind,
                principal_id=principal_id,
                login_digest=login_digest,
                tenant_id=tenant_id,
                route=self.route,
                occurred_at=utcnow(),
                details=details if details is not None else {},
            ),
        )
