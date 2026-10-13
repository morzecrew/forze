from collections.abc import Callable
from contextlib import nullcontext
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity, PrincipalDeactivationPort
from forze.application.contracts.execution import Handler
from forze.domain.models import BaseDTO

from ._utils import TxScope, require_admin_identity

# ----------------------- #


class DeactivatePrincipalRequestDTO(BaseDTO):
    """Request to deactivate a principal for the application."""

    principal_id: UUID
    """Principal to deactivate."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeactivatePrincipalHandler(Handler[DeactivatePrincipalRequestDTO, None]):
    """Admin: deactivate policy principal, sessions, and credential accounts.

    Refuses a caller with no identity (401) or a delegated one (``delegate_denied``); whether
    the caller may administer at all is the guards' to decide.
    """

    resolver: Callable[[], AuthnIdentity | None]
    """Callable that resolves the current authenticated identity."""

    deactivation: PrincipalDeactivationPort
    """Cascaded deactivation port."""

    tx: TxScope = nullcontext
    """Opens the transaction the policy, sessions and credentials commit in together."""

    # ....................... #

    async def __call__(self, args: DeactivatePrincipalRequestDTO) -> None:
        require_admin_identity(self.resolver)

        async with self.tx():
            await self.deactivation.deactivate(args.principal_id)
