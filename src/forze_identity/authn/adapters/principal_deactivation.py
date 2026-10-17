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

from ..services import SessionRevocation
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

    revocation: SessionRevocation | None = None
    """When set, refuse before the first write outside a transaction, which the session
    revocation would refuse after the principal and its credentials were already written."""

    # ....................... #

    async def deactivate(self, principal_id: UUID) -> None:
        if self.revocation is not None:
            self.revocation.require_transaction()

        # Credentials first: a password change and reset write the account and then the
        # principal's epoch and sessions, and the policy principal's write may raise that epoch
        # (with revocation states on the authz kernel). One order keeps them from deadlocking.
        await self.credentials.deactivate_all(principal_id)
        await self.principal_registry.deactivate_principal(principal_id)
        await self.token_lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal_id))

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PRINCIPAL_DEACTIVATED,
                principal_id=principal_id,
            )
