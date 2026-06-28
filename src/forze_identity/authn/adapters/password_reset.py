from typing import Any, final

import attrs

from forze.application.contracts.authn import (
    AuthnEventEmitter,
    AuthnEventKind,
    IssuedPasswordReset,
    PasswordResetPort,
    PrincipalEligibilityPort,
    login_digest,
)
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    KeyedUpdate,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import utcnow
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.models.account import (
    PasswordAccount,
    ReadPasswordAccount,
    UpdatePasswordAccountCmd,
)
from ..domain.models.reset import (
    CreatePasswordResetCmd,
    PasswordReset,
    ReadPasswordReset,
    UpdatePasswordResetCmd,
)
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from ..services import PasswordService, ResetTokenService
from ._utils import (
    find_outstanding_password_resets,
    find_password_account_by_login,
    find_password_account_by_principal_id,
    find_password_reset_by_digest,
    revoke_sessions_matching,
)

# ----------------------- #

INVALID_RESET_TOKEN_MSG = "Invalid or expired reset token"  # nosec B105
"""Single uniform message for every ``reset_password`` failure mode — wrong,
expired, already-used, superseded, and garbage tokens are indistinguishable to
the caller (no failure-mode enumeration)."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordResetAdapter(PasswordResetPort):
    """Self-service password reset adapter (single-use HMAC-digest tokens).

    Mirrors the invite machinery of
    :class:`~forze_identity.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`:
    only the peppered HMAC digest of the reset token is persisted
    (``authn_password_resets``, spec marked ``sensitive``); the raw token is
    returned once at issuance for out-of-band delivery and never stored.

    Semantics:

    - :meth:`request_reset` is eligibility-gated like login and returns ``None``
      for unknown/inactive/ineligible logins (the port tells the truth — the
      no-enumeration uniformity toward HTTP callers is the kit handler's job).
      Issuing a reset supersedes any outstanding reset for the same principal
      (single active reset).
    - :meth:`reset_password` verifies the digest in constant time, enforces TTL
      and single-use, sets the new Argon2 hash, and revokes ALL of the
      principal's sessions ("log out everywhere" — the same round-5 semantics as
      change-password). Every failure mode raises the uniform
      :data:`INVALID_RESET_TOKEN_MSG` authentication error.
    """

    password_svc: PasswordService
    """Password service."""

    reset_svc: ResetTokenService
    """Reset token service (peppered HMAC digests)."""

    pa_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    pa_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        Any,
        UpdatePasswordAccountCmd,
    ]
    """Password account command port."""

    reset_qry: DocumentQueryPort[ReadPasswordReset]
    """Password reset query port."""

    reset_cmd: DocumentCommandPort[
        ReadPasswordReset,
        PasswordReset,
        CreatePasswordResetCmd,
        UpdatePasswordResetCmd,
    ]
    """Password reset command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    session_qry: DocumentQueryPort[ReadSession] | None = None
    """Session query port; required when ``revoke_sessions_on_reset``."""

    session_cmd: (
        DocumentCommandPort[
            ReadSession,
            Session,
            CreateSessionCmd,
            UpdateSessionCmd,
        ]
        | None
    ) = None
    """Session command port; required when ``revoke_sessions_on_reset``."""

    revoke_sessions_on_reset: bool = True
    """Revoke ALL of the principal's sessions after a successful reset ("log out
    everywhere") — a reset is the canonical response to a lost or compromised
    credential, so surviving sessions would defeat its purpose. Opt out only
    when sessions are managed by an external lifecycle."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (best-effort; ``None`` disables emission).

    Emits ``PASSWORD_RESET_REQUESTED`` on actual token issuance only (unknown or
    ineligible logins produce no event — the uniform 202 toward callers is the
    kit handler's job; the event stream records what really happened) and
    ``PASSWORD_RESET_COMPLETED`` after a successful reset."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        forbid_cache_and_history(
            self.pa_qry.spec,
            self.pa_cmd.spec,
            label="Password account",
        )
        forbid_cache_and_history(
            self.reset_qry.spec,
            self.reset_cmd.spec,
            label="Password reset",
        )

        if self.revoke_sessions_on_reset:
            if self.session_qry is None or self.session_cmd is None:
                raise exc.configuration(
                    "revoke_sessions_on_reset requires session_qry and "
                    "session_cmd; wire the session document ports or explicitly "
                    "opt out with revoke_sessions_on_reset=False",
                )

            forbid_cache_and_history(
                self.session_qry.spec,
                self.session_cmd.spec,
                label="Session",
            )

    # ....................... #

    async def request_reset(self, login: str) -> IssuedPasswordReset | None:
        """Issue a single-use reset token for ``login``.

        Two writes when an older reset is outstanding (supersede it, create the
        new row). Run within a transaction scope so both commit or roll back
        together — the document gateways join the ambient transaction when one
        is open. The order is recovery-safe even without one: a failed create
        leaves no usable reset behind, and a retry simply issues a fresh token.
        """

        if not login:
            return None

        account = await find_password_account_by_login(self.pa_qry, login)

        if account is None or not account.is_active:
            return None

        # Eligibility-gated like login: a deactivated principal cannot mint a
        # working reset. The gate raising maps to "ineligible" → None.
        try:
            await self.eligibility.require_authentication_allowed(
                account.principal_id,
            )

        except CoreException:
            return None

        now = utcnow()
        expires_at = now + self.reset_svc.config.expires_in

        token = self.reset_svc.generate_token()
        digest = self.reset_svc.calculate_token_digest(token)

        # Single active reset: supersede every outstanding reset of the
        # principal before persisting the fresh digest.
        outstanding = await find_outstanding_password_resets(
            self.reset_qry,
            account.principal_id,
        )

        if outstanding:
            await self.reset_cmd.update_many(
                [
                    KeyedUpdate(
                        id=x.id,
                        rev=x.rev,
                        dto=UpdatePasswordResetCmd(used_at=now),
                    )
                    for x in outstanding
                ],
                return_new=False,
            )

        await self.reset_cmd.create(
            CreatePasswordResetCmd(
                principal_id=account.principal_id,
                token_digest=digest,
                expires_at=expires_at,
            ),
            return_new=False,
        )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_RESET_REQUESTED,
                principal_id=account.principal_id,
                login_digest=login_digest(login),
            )

        return IssuedPasswordReset(
            token=token,
            principal_id=account.principal_id,
            login=login,
            expires_at=expires_at,
        )

    # ....................... #

    async def reset_password(self, token: str, new_password: str) -> None:
        """Consume a reset token and set a new password.

        Three writes (new hash, consume the reset, revoke sessions). Run within
        a transaction scope so they commit or roll back together. The order is
        recovery-safe without one: the hash lands first, so a crash before the
        consume leaves a token that can only re-apply the same password, and the
        consume is rev-conditional (optimistic concurrency) against double use.
        """

        if not token:
            raise exc.authentication(INVALID_RESET_TOKEN_MSG)

        try:
            digest = self.reset_svc.calculate_token_digest(token)

        except Exception as e:
            raise exc.authentication(INVALID_RESET_TOKEN_MSG) from e

        reset = await find_password_reset_by_digest(self.reset_qry, digest)

        if reset is None or reset.used_at is not None:
            raise exc.authentication(INVALID_RESET_TOKEN_MSG)

        if reset.expires_at <= utcnow():
            raise exc.authentication(INVALID_RESET_TOKEN_MSG)

        if not self.reset_svc.verify_token(token, reset.token_digest):
            raise exc.authentication(INVALID_RESET_TOKEN_MSG)

        try:
            await self.eligibility.require_authentication_allowed(
                reset.principal_id,
            )

        except CoreException as e:
            raise exc.authentication(INVALID_RESET_TOKEN_MSG) from e

        account = await find_password_account_by_principal_id(
            self.pa_qry,
            reset.principal_id,
        )

        if account is None or not account.is_active:
            raise exc.authentication(INVALID_RESET_TOKEN_MSG)

        new_hash = await self.password_svc.hash_password(new_password)

        await self.pa_cmd.update(
            account.id,
            account.rev,
            UpdatePasswordAccountCmd(password_hash=new_hash),
            return_new=False,
        )

        # Single-use: consume in the same transaction scope as the hash update.
        await self.reset_cmd.update(
            reset.id,
            reset.rev,
            UpdatePasswordResetCmd(used_at=utcnow()),
            return_new=False,
        )

        # "Log out everywhere": a reset implies the old credential (and any
        # session minted with it) can no longer be trusted.
        if self.revoke_sessions_on_reset:
            # Ports are guaranteed non-None by __attrs_post_init__ when the flag is set.
            await revoke_sessions_matching(
                self.session_qry,  # type: ignore[arg-type]
                self.session_cmd,  # type: ignore[arg-type]
                {"principal_id": reset.principal_id},
            )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_RESET_COMPLETED,
                principal_id=reset.principal_id,
            )
