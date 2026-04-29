from typing import Any, final

import attrs

from forze.application.contracts.authn import AuthnIdentity, PasswordLifecyclePort
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError

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

    password_account_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    password_account_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        Any,
        UpdatePasswordAccountCmd,
    ]
    """Password account command port."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.password_account_qry.spec
        cmd_spec = self.password_account_cmd.spec

        if qry_spec.cache is not None:
            raise CoreError("Password account caching is forbidden by security reasons")

        if cmd_spec.cache is not None:
            raise CoreError("Password account caching is forbidden by security reasons")

        if qry_spec.history_enabled:
            raise CoreError("Password account history is forbidden by security reasons")

        if cmd_spec.history_enabled:
            raise CoreError("Password account history is forbidden by security reasons")

    # ....................... #

    async def change_password(
        self,
        identity: AuthnIdentity,
        new_password: str,
    ) -> None:
        pa = await find_password_account_by_authn_identity(
            self.password_account_qry,
            identity,
        )

        if pa is None or not pa.is_active:
            raise AuthenticationError("Password account not found")

        new_pwd_hash = self.password_svc.hash_password(new_password)
        upd_cmd = UpdatePasswordAccountCmd(password_hash=new_pwd_hash)

        await self.password_account_cmd.update(pa.id, pa.rev, upd_cmd, return_new=False)
