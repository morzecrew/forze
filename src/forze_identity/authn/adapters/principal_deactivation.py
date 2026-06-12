from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnEventEmitter,
    AuthnEventKind,
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

    events: AuthnEventEmitter | None = None
    """Optional authn event emitter (best-effort; ``None`` disables emission).

    Emits ``PRINCIPAL_DEACTIVATED`` after the cascade completes. The session
    revocation leg may additionally emit ``LOGOUT`` when the token lifecycle has
    its own emitter wired — both are true statements about what happened."""

    # ....................... #

    async def deactivate(self, principal_id: UUID) -> None:
        await self.principal_registry.deactivate_principal(principal_id)
        await self.token_lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal_id))
        await self.credentials.deactivate_all(principal_id)

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PRINCIPAL_DEACTIVATED,
                principal_id=principal_id,
            )
