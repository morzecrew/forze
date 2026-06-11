"""Outbox payload models for authn integration events.

Security note: the password-reset payload carries the **raw reset token** —
that is the point of the seam (the app's relay/notify pipeline turns it into an
e-mail or SMS), but it means the token transits the outbox row. The short reset
TTL and single-use semantics bound the exposure; keep the outbox store as
protected as the credential stores, and prefer a dedicated outbox route with a
tight retention policy. Apps that want zero persistence of the raw token should
call :class:`~forze.application.contracts.authn.PasswordResetPort` directly from
a custom handler instead of configuring ``reset_events``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.authn import IssuedPasswordReset

# ----------------------- #

AUTHN_PASSWORD_RESET_REQUESTED: Final[str] = "authn.password_reset_requested"
"""Integration event type staged when a password reset is issued for a known login."""


# ....................... #


class AuthnPasswordResetRequestedPayload(BaseModel):
    """Outbox payload for :data:`AUTHN_PASSWORD_RESET_REQUESTED`.

    Consumers (notify/e-mail relays) build the delivery message from this —
    typically a link embedding :attr:`token` sent to the address behind
    :attr:`login`.
    """

    login: str
    """Login the reset was requested for (delivery-channel lookup key)."""

    principal_id: UUID
    """Principal whose password the reset re-keys once confirmed."""

    token: str
    """Raw single-use reset token (see the module security note)."""

    expires_at: datetime
    """Absolute expiration time of the reset token."""

    # ....................... #

    @classmethod
    def from_issued(
        cls,
        issued: IssuedPasswordReset,
    ) -> AuthnPasswordResetRequestedPayload:
        """Build the payload from a freshly issued reset."""

        return cls(
            login=issued.login,
            principal_id=issued.principal_id,
            token=issued.token,
            expires_at=issued.expires_at,
        )
