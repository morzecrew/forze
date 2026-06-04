from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PasswordAccountProvisioningPort,
    PasswordCredentials,
    PrincipalEligibilityPort,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history

from ..domain.models.account import (
    CreatePasswordAccountCmd,
    PasswordAccount,
    ReadPasswordAccount,
)
from ..services import PasswordService
from ._utils import find_password_account_by_login

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PasswordAccountProvisioningAdapter(PasswordAccountProvisioningPort):
    """Password account provisioning adapter."""

    password_svc: PasswordService
    """Password service."""

    password_account_qry: DocumentQueryPort[ReadPasswordAccount]
    """Password account query port."""

    password_account_cmd: DocumentCommandPort[
        ReadPasswordAccount,
        PasswordAccount,
        CreatePasswordAccountCmd,
        Any,
    ]
    """Password account command port."""

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        qry_spec = self.password_account_qry.spec
        cmd_spec = self.password_account_cmd.spec

        forbid_cache_and_history(qry_spec, cmd_spec, label="Password account")

    # ....................... #

    async def accept_invite_with_password(
        self,
        invite_token: str,  # noqa: F841
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        raise NotImplementedError("Invite token verification is not implemented")

    # ....................... #

    async def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await self.eligibility.require_authentication_allowed(operator.principal_id)
        await self.register_with_password(principal_id, credentials)

    # ....................... #

    async def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        await self.eligibility.require_authentication_allowed(principal_id)

        existing = await find_password_account_by_login(
            self.password_account_qry,
            credentials.login,
        )
        if existing is not None:
            raise exc.conflict(
                "Password account already exists for this login",
                code="password_account_exists",
            )

        pwd_hash = self.password_svc.hash_password(credentials.password)

        create_cmd = CreatePasswordAccountCmd(
            username=credentials.login,
            principal_id=principal_id,
            password_hash=pwd_hash,
        )

        await self.password_account_cmd.create(create_cmd, return_new=False)
