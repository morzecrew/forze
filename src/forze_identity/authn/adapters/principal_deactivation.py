from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PrincipalDeactivationPort,
    TokenLifecyclePort,
)
from forze.application.contracts.authz import PrincipalRegistryPort

from .credential_deactivation import AuthnCredentialDeactivationHelper

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalDeactivationAdapter(PrincipalDeactivationPort):
    """Cascade principal deactivation across policy, sessions, and credentials."""

    principal_registry: PrincipalRegistryPort
    token_lifecycle: TokenLifecyclePort
    credentials: AuthnCredentialDeactivationHelper

    # ....................... #

    async def deactivate(self, principal_id: UUID) -> None:
        await self.principal_registry.deactivate_principal(principal_id)
        await self.token_lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal_id))
        await self.credentials.deactivate_all(principal_id)
