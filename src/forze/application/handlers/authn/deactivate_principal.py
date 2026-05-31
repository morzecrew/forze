from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnSpec, PrincipalDeactivationPort
from forze.application.contracts.execution import Handler
from forze.domain.models import BaseDTO

# ----------------------- #


class DeactivatePrincipalRequestDTO(BaseDTO):
    """Request to deactivate a principal for the application."""

    principal_id: UUID
    """Principal to deactivate."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DeactivatePrincipalHandler(Handler[DeactivatePrincipalRequestDTO, None]):
    """Deactivate policy principal, sessions, and credential accounts."""

    spec: AuthnSpec
    """Authn route for deactivation port resolution."""

    deactivation: PrincipalDeactivationPort
    """Cascaded deactivation port."""

    # ....................... #

    async def __call__(self, args: DeactivatePrincipalRequestDTO) -> None:
        await self.deactivation.deactivate(args.principal_id)
