"""Self-service password reset handlers (request + confirm)."""

import attrs

from forze.application.contracts.authn import PasswordResetPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.outbox import OutboxCommandPort

from ..dto import (
    AuthnPasswordResetAckDTO,
    AuthnRequestPasswordResetDTO,
    AuthnResetPasswordDTO,
)
from ..events import (
    AUTHN_PASSWORD_RESET_REQUESTED,
    AuthnPasswordResetRequestedPayload,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnRequestPasswordReset(
    Handler[AuthnRequestPasswordResetDTO, AuthnPasswordResetAckDTO],
):
    """Request a password reset for a login — uniform ack, no enumeration.

    ALWAYS returns the same :class:`AuthnPasswordResetAckDTO` (202-shaped):
    unknown, inactive, and ineligible logins produce a response identical to a
    known login's, and no work is skipped on the unknown path that would make
    the timing trivially distinguishable (the account lookup runs either way).
    The timing posture is best-effort, not constant-time — issuing a token and
    staging the event cost more than the early ``None`` return; deployments
    needing stronger guarantees should rate-limit this operation at the edge.

    Delivery seam: the issued token must reach the account holder WITHOUT
    appearing in the response. When ``outbox`` is wired (via the
    ``reset_events`` argument of ``build_authn_registry``), the handler stages
    an :data:`AUTHN_PASSWORD_RESET_REQUESTED` integration event whose payload
    carries the raw token for the app to route to its notify/e-mail pipeline —
    see the security note on :mod:`forze_kits.aggregates.authn.events` (the raw
    token transits the outbox row; short TTL + single-use bound the exposure).
    Apps wanting zero persistence of the raw token should call
    :class:`~forze.application.contracts.authn.PasswordResetPort` directly from
    a custom handler instead.

    WARNING: when ``outbox`` is ``None`` and no custom delivery exists,
    requesting a reset is a no-op beyond the port call — a token is minted and
    its digest persisted, but nobody ever receives it.
    """

    password_reset: PasswordResetPort
    """Self-service password reset port."""

    outbox: OutboxCommandPort[AuthnPasswordResetRequestedPayload] | None = attrs.field(
        default=None,
    )
    """Optional outbox staging for the reset-requested integration event."""

    # ....................... #

    async def __call__(
        self,
        args: AuthnRequestPasswordResetDTO,
    ) -> AuthnPasswordResetAckDTO:
        issued = await self.password_reset.request_reset(args.login)

        if issued is not None and self.outbox is not None:
            await self.outbox.stage(
                AUTHN_PASSWORD_RESET_REQUESTED,
                AuthnPasswordResetRequestedPayload.from_issued(issued),
            )
            await self.outbox.flush()

        # Uniform ack regardless of the port outcome — never leak whether the
        # login exists, and never echo the token.
        return AuthnPasswordResetAckDTO()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnResetPassword(Handler[AuthnResetPasswordDTO, None]):
    """Confirm a password reset: consume the token and set the new password.

    Any invalid, expired, or already-used token surfaces as the port's uniform
    authentication error (a 401 at the HTTP boundary) — failure modes are not
    enumerated. On success the port revokes all of the principal's sessions
    ("log out everywhere"); the caller logs in with the new password.
    """

    password_reset: PasswordResetPort
    """Self-service password reset port."""

    # ....................... #

    async def __call__(self, args: AuthnResetPasswordDTO) -> None:
        await self.password_reset.reset_password(args.token, args.new_password)
