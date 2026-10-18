from typing import Any, final
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

    stores: tuple[Any, ...] = ()
    """The command ports the cascade writes through (credentials, principal, sessions). With
    :attr:`revocation`, each must write in the open transaction, checked before the first
    write; one that cannot report it is left unchecked."""

    # ....................... #

    async def deactivate(self, principal_id: UUID) -> None:
        if self.revocation is not None:
            self.revocation.require_transaction(*self.stores)

        if self.revocation is not None:
            # Credentials first: a password change or reset then writes the account and raises
            # the principal's epoch, and the principal's write may raise it too (revocation
            # states on the authz kernel). One order keeps the two from deadlocking; the
            # transaction required above makes the order invisible otherwise.
            await self.credentials.deactivate_all(principal_id)
            await self.principal_registry.deactivate_principal(principal_id)

        else:
            # Without revocation states nothing raises an epoch here, and a failure part-way
            # outside a transaction leaves the principal inactive.
            await self.principal_registry.deactivate_principal(principal_id)
            await self.credentials.deactivate_all(principal_id)

        await self.token_lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal_id))

        if self.events is not None:
            await self.events.emit(
                AuthnEventKind.PRINCIPAL_DEACTIVATED,
                principal_id=principal_id,
            )
