from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PasswordAccountProvisioningPort,
    PasswordCredentials,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import CoreError

from ..domain.models.account import (
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadPasswordAccount,
    ReadPrincipal,
)
from ..services import PasswordService
from ._utils import validate_principal

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordAccountProvisioningAdapter(PasswordAccountProvisioningPort):
    """Password account provisioning adapter."""

    password_svc: PasswordService
    """Password service."""

    # ....................... #

    password_account_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    password_account_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        CreatePasswordAccountCmd,
        Any,
    ]
    """Password account command port."""

    principal_qry: DocumentQueryPort[ReadPrincipal]
    """Principal query port."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.password_account_qry.spec
        cmd_spec = self.password_account_cmd.spec
        principal_spec = self.principal_qry.spec

        if qry_spec.cache is not None:
            raise CoreError("Password account caching is forbidden by security reasons")

        if cmd_spec.cache is not None:
            raise CoreError("Password account caching is forbidden by security reasons")

        if qry_spec.history_enabled:
            raise CoreError("Password account history is forbidden by security reasons")

        if cmd_spec.history_enabled:
            raise CoreError("Password account history is forbidden by security reasons")

        if principal_spec.cache is not None:
            raise CoreError("Principal caching is forbidden by security reasons")

        if principal_spec.history_enabled:
            raise CoreError("Principal history is forbidden by security reasons")

    # ....................... #

    async def accept_invite_with_password(
        self,
        invite_token: str,  # noqa: F841 # would need to verify the invite token outside?
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        raise NotImplementedError("Invite token verification is not implemented")

    # ....................... #
    #! Maybe make it configurable or something like that

    async def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await validate_principal(self.principal_qry, operator.principal_id)
        await self.register_with_password(principal_id, credentials)

    # ....................... #

    async def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await validate_principal(self.principal_qry, principal_id)

        pwd_hash = self.password_svc.hash_password(credentials.password)

        create_cmd = CreatePasswordAccountCmd(
            username=credentials.login,
            principal_id=principal_id,
            password_hash=pwd_hash,
        )

        await self.password_account_cmd.create(create_cmd, return_new=False)
