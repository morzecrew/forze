from collections.abc import Awaitable
from typing import Protocol

from ..value_objects import IssuedPasswordReset

# ----------------------- #


class PasswordResetPort(Protocol):  # pragma: no cover
    """Self-service password reset for subjects who cannot authenticate.

    The port tells the truth: :meth:`request_reset` returns ``None`` for an
    unknown or ineligible login. Keeping responses uniform toward the *caller*
    (no account enumeration) is the application handler's job — see
    ``forze_kits.aggregates.authn.AuthnRequestPasswordReset``.
    """

    def request_reset(
        self,
        login: str,
    ) -> Awaitable["IssuedPasswordReset | None"]:
        """Issue a single-use reset token for ``login``.

        Returns the freshly issued reset (carrying the raw token for out-of-band
        delivery) or ``None`` when the login is unknown or the principal is not
        eligible to authenticate. Issuing a new reset invalidates any previous
        outstanding reset for the same principal (single active reset).
        """
        ...

    def reset_password(
        self,
        token: str,
        new_password: str,
    ) -> Awaitable[None]:
        """Consume a reset token and set a new password.

        An invalid, expired, or already-used token raises a uniform
        authentication error (no failure-mode enumeration). On success the new
        password hash is persisted, the token is marked used (single-use), and
        ALL of the principal's sessions are revoked ("log out everywhere",
        matching the change-password semantics) — the caller re-authenticates
        with the new password.
        """
        ...
