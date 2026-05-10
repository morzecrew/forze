from typing import Awaitable, Protocol
from uuid import UUID

from ..value_objects import (
    AuthnIdentity,
    PasswordCredentials,
    TokenCredentials,
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

    def accept_invite_with_password(
        self,
        invite: TokenCredentials,  # noqa: F841
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> Awaitable[None]: ...
