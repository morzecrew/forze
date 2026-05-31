from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import PrincipalEligibilityPort
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PolicyPrincipalEligibilityAdapter(PrincipalEligibilityPort):
    """Require an active policy principal before authentication or credential mutation."""

    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    """Query port for policy principals."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.principal_qry.spec

        if spec.cache is not None:
            raise exc.internal(
                "Policy principal caching is forbidden by security reasons",
            )

        if spec.history_enabled:
            raise exc.internal(
                "Policy principal history is forbidden by security reasons",
            )

    # ....................... #

    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        principal = await self.principal_qry.find(
            filters={
                "$values": {
                    "id": principal_id,
                },
            }
        )

        if principal is None or not principal.is_active:
            raise exc.authentication("Principal not found")
