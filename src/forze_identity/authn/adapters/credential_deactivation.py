from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    KeyedUpdate,
)
from forze.base.exceptions import CoreException, exc

from ..domain.models.account import (
    ApiKeyAccount,
    CreateApiKeyAccountCmd,
    PasswordAccount,
    ReadApiKeyAccount,
    ReadPasswordAccount,
    UpdateApiKeyAccountCmd,
    UpdatePasswordAccountCmd,
)
from ..services import ApiKeyRevocation
from ._utils import STALE_RETRIES, detected_stale, find_password_account_by_principal_id

# ----------------------- #

_DEACTIVATE_BATCH = 1_000
"""API keys deactivated per round trip, below every store's implicit read limit."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnCredentialDeactivationHelper:
    """Deactivate password and API key credential rows for a principal.

    Each store is optional: a deployment that never wired password or API-key authn has no
    route for that store, and its leg is skipped. Pass both ports of a store or neither.

    A write a concurrent password change or key rotation made stale is read again and closed
    again, a few times in a row at most, so a deactivation does not leave a credential open for
    losing one write. A conflict the backend reports has aborted the transaction and propagates.
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

    key_revocation: ApiKeyRevocation | None = None
    """When set, every key of the principal, deactivated now or before, has ``revoked`` written
    to the revocation states once the deactivation commits."""

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

        for attempt in range(STALE_RETRIES + 1):
            account = await find_password_account_by_principal_id(
                self.pa_qry,
                principal_id,
                lock=True,
            )

            if account is None or not account.is_active:
                return

            try:
                await self.pa_cmd.update(
                    account.id,
                    account.rev,
                    UpdatePasswordAccountCmd(is_active=False),
                    return_new=False,
                )
                return

            except CoreException as error:
                if not detected_stale(error) or attempt == STALE_RETRIES:
                    raise

    # ....................... #

    async def _deactivate_api_keys(self, principal_id: UUID) -> None:
        if self.ak_qry is None or self.ak_cmd is None:
            return

        offset = 0
        stale = 0

        # A read naming no limit stops at the store's implicit cap; page through every key.
        # ponytail: offset pages; keyset by id if principals ever hold many thousands of keys.
        while True:
            page = await self.ak_qry.find_many(
                filters={"$values": {"principal_id": principal_id}},
                pagination={"limit": _DEACTIVATE_BATCH, "offset": offset},
                sorts={"id": "asc"},
            )
            upds = [
                KeyedUpdate(id=row.id, rev=row.rev, dto=UpdateApiKeyAccountCmd(is_active=False))
                for row in page.hits
                if row.is_active
            ]

            if upds:
                try:
                    await self.ak_cmd.update_many(upds, return_new=False)

                except CoreException as error:
                    if not detected_stale(error) or stale >= STALE_RETRIES:
                        raise

                    # The page is read again, with the key a rotation minted if it committed.
                    stale += 1
                    continue

            stale = 0

            # Every key, active or not: a retry then repairs a write-through that failed.
            if self.key_revocation is not None:
                await self.key_revocation.revoke(page.hits)

            # A deactivated key stays under the filter, so the next page starts past this one.
            if len(page.hits) < _DEACTIVATE_BATCH:
                return

            offset += len(page.hits)
