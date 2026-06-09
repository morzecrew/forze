from typing import Awaitable, Protocol
from uuid import UUID

from ..value_objects import (
    AuthnIdentity,
    IssuedInvite,
    PasswordCredentials,
)

# ----------------------- #


class PasswordAccountProvisioningPort(Protocol):  # pragma: no cover
    """Provision password accounts for a known internal principal id."""

    def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> Awaitable[None]: ...

    def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> Awaitable[None]: ...

    def issue_password_invite(
        self,
        operator: AuthnIdentity,  # noqa: F841
        principal_id: UUID,
    ) -> Awaitable[IssuedInvite]: ...

    def accept_invite_with_password(
        self,
        invite_token: str,  # noqa: F841
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> Awaitable[None]: ...
