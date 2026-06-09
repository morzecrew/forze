from typing import Any, final

import attrs

from forze.application.contracts.authn import (
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
from ..services import PasswordService
from ._utils import find_password_account_by_authn_identity

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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.pa_qry.spec
        cmd_spec = self.pa_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="Password account")

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
        if not self.password_svc.verify_password(
            password_hash=pa.password_hash,
            password=current_password,
        ):
            raise exc.authentication(
                "Current password is incorrect",
                code="invalid_credentials",
            )

        new_pwd_hash = self.password_svc.hash_password(new_password)
        upd_cmd = UpdatePasswordAccountCmd(password_hash=new_pwd_hash)

        await self.pa_cmd.update(pa.id, pa.rev, upd_cmd, return_new=False)
