from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    KeyedUpdate,
)
from forze.base.exceptions import exc

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    PasswordAccount,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)
from ._utils import find_password_account_by_principal_id

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnCredentialDeactivationHelper:
    """Deactivate password and API key credential rows for a principal.

    Each store is optional: a deployment that never wired password or API-key authn has no
    route for that store, and its leg is skipped. Pass both ports of a store or neither.
    """

    pa_qry: DocumentQueryPort[ReadPasswordAccount] | None = None
    pa_cmd: (
        DocumentCommandPort[
            ReadPasswordAccount,
            PasswordAccount,
            Any,
            UpdatePasswordAccountCmd,
        ]
        | None
    ) = None
    ak_qry: DocumentQueryPort[ReadApiKeyAccount] | None = None
    ak_cmd: (
        DocumentCommandPort[
            ReadApiKeyAccount,
            ApiKeyAccount,
            CreateApiKeyAccountCmd,
            UpdateApiKeyAccountCmd,
        ]
        | None
    ) = None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Half a store would skip its leg silently, leaving credentials active.
        if (self.pa_qry is None) != (self.pa_cmd is None) or (self.ak_qry is None) != (
            self.ak_cmd is None
        ):
            raise exc.configuration(
                "AuthnCredentialDeactivationHelper takes both ports of a credential store or "
                "neither."
            )

    # ....................... #

    async def deactivate_all(self, principal_id: UUID) -> None:
        await self._deactivate_password_account(principal_id)
        await self._deactivate_api_keys(principal_id)

    # ....................... #

    async def _deactivate_password_account(self, principal_id: UUID) -> None:
        if self.pa_qry is None or self.pa_cmd is None:
            return

        account = await find_password_account_by_principal_id(self.pa_qry, principal_id)

        if account is None or not account.is_active:
            return

        await self.pa_cmd.update(
            account.id,
            account.rev,
            UpdatePasswordAccountCmd(is_active=False),
            return_new=False,
        )

    # ....................... #

    async def _deactivate_api_keys(self, principal_id: UUID) -> None:
        if self.ak_qry is None or self.ak_cmd is None:
            return

        result = await self.ak_qry.find_many(
            filters={
                "$values": {
                    "principal_id": principal_id,
                },
            }
        )

        upds = [
            KeyedUpdate(
                id=row.id,
                rev=row.rev,
                dto=UpdateApiKeyAccountCmd(is_active=False),
            )
            for row in result.hits
            if row.is_active
        ]

        if upds:
            await self.ak_cmd.update_many(upds, return_new=False)
