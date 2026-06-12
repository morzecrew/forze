from typing import Any, final

import attrs

from forze.application.contracts.authn import (
    AuthnEventEmitter,
    AuthnEventKind,
    AuthnIdentity,
    PasswordLifecyclePort,
    PrincipalEligibilityPort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.models.account import (
    PasswordAccount,
    ReadPasswordAccount,
    UpdatePasswordAccountCmd,
)
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)
from ..services import PasswordService
from ._utils import find_password_account_by_authn_identity, revoke_sessions_matching

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordLifecycleAdapter(PasswordLifecyclePort):
    """Password lifecycle adapter."""

    password_svc: PasswordService
    """Password service."""

    pa_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    pa_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        Any,
        UpdatePasswordAccountCmd,
    ]
    """Password account command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    session_qry: DocumentQueryPort[ReadSession] | None = None
    """Session query port; required when ``revoke_sessions_on_password_change``."""

    session_cmd: (
        DocumentCommandPort[
            ReadSession,
            Session,
            CreateSessionCmd,
            UpdateSessionCmd,
        ]
        | None
    ) = None
    """Session command port; required when ``revoke_sessions_on_password_change``."""

    revoke_sessions_on_password_change: bool = True
    """Revoke ALL of the principal's sessions (refresh families and their ``sid``-bound
    access tokens) after a successful password change ("log out everywhere"); the caller
    must re-authenticate with the new password. Opt out to keep existing sessions alive."""

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (best-effort; ``None`` disables emission).

    Emits ``PASSWORD_CHANGED`` after a successful change."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.pa_qry.spec
        cmd_spec = self.pa_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="Password account")

        if self.revoke_sessions_on_password_change:
            if self.session_qry is None or self.session_cmd is None:
                raise exc.configuration(
                    "revoke_sessions_on_password_change requires session_qry and "
                    "session_cmd; wire the session document ports or explicitly "
                    "opt out with revoke_sessions_on_password_change=False",
                )

            forbid_cache_and_history(
                self.session_qry.spec,
                self.session_cmd.spec,
                label="Session",
            )

    # ....................... #

    async def change_password(
        self,
        identity: AuthnIdentity,
        current_password: str,
        new_password: str,
    ) -> None:
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        pa = await find_password_account_by_authn_identity(
            self.pa_qry,
            identity,
        )

        if pa is None or not pa.is_active:
            raise exc.authentication("Password account not found")

        # Re-authenticate with the current password before allowing the change, so a
        # hijacked session (a valid bearer identity) cannot escalate to a full account
        # takeover by silently resetting the password.
        if not await self.password_svc.verify_password(
            password_hash=pa.password_hash,
            password=current_password,
        ):
            raise exc.authentication(
                "Current password is incorrect",
                code="invalid_credentials",
            )

        new_pwd_hash = await self.password_svc.hash_password(new_password)
        upd_cmd = UpdatePasswordAccountCmd(password_hash=new_pwd_hash)

        await self.pa_cmd.update(pa.id, pa.rev, upd_cmd, return_new=False)

        # "Log out everywhere": a password change is the canonical response to a
        # suspected credential compromise, so a hijacked session must not survive it.
        # The current session id is not part of AuthnIdentity, so all sessions are
        # revoked and the caller re-authenticates with the new password.
        if self.revoke_sessions_on_password_change:
            # Ports are guaranteed non-None by __attrs_post_init__ when the flag is set.
            await revoke_sessions_matching(
                self.session_qry,  # type: ignore[arg-type]
                self.session_cmd,  # type: ignore[arg-type]
                {"principal_id": identity.principal_id},
            )

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PASSWORD_CHANGED,
                principal_id=identity.principal_id,
            )
