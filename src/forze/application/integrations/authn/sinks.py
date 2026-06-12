"""Shipped authn event sinks (core)."""

from typing import Final, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.authn import AuthnEvent, AuthnEventKind, AuthnEventSink

# ----------------------- #

_WARNING_KINDS: Final = frozenset(
    {
        AuthnEventKind.LOGIN_FAILED,
        AuthnEventKind.LOGIN_LOCKED,
        AuthnEventKind.REFRESH_REUSE_DETECTED,
    },
)
"""Kinds that indicate something worth an operator's attention."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LoggingAuthnEventSink(AuthnEventSink):
    """Structured-log sink: one core-logger line per event.

    ``LOGIN_FAILED`` / ``LOGIN_LOCKED`` / ``REFRESH_REUSE_DETECTED`` log at
    WARNING (failed credentials, throttled logins, and token-theft signals are
    operator-relevant); everything else logs at INFO. Only the privacy-safe
    event fields are logged — :attr:`AuthnEvent.login_digest`, never a raw
    login, reaches the log stream.

    Never raises: callers go through
    :func:`~forze.application.contracts.authn.emit_safe` anyway, but a logging
    sink failing the very flow it observes would be absurd, so any logging
    backend error is swallowed here as well.
    """

    async def record(self, event: AuthnEvent) -> None:
        try:
            log = logger.warning if event.kind in _WARNING_KINDS else logger.info

            log(
                "Authn event '%s' on route '%s'",
                str(event.kind),
                str(event.route),
                principal_id=(
                    str(event.principal_id) if event.principal_id is not None else None
                ),
                login_digest=event.login_digest,
                tenant_id=str(event.tenant_id) if event.tenant_id is not None else None,
                occurred_at=event.occurred_at.isoformat(),
                **dict(event.details),
            )

        except Exception:  # nosec B110 # noqa: BLE001 — a logging sink must never raise
            pass
