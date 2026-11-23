"""The local preset's principal registry check, where no authz plane is wired."""

from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import PrincipalEligibilityPort
from forze.base.exceptions import exc

from .config import LocalIdentityConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class LocalPrincipalEligibility(PrincipalEligibilityPort):
    """Eligible while the config names the principal on an API key: the config is the registry.

    Removing a principal's last key and restarting deactivates it. A principal that appears
    only in ``principal_tenants`` holds no credential, and is not eligible.
    """

    principals: frozenset[UUID]
    """The principals the config names on an API key."""

    # ....................... #

    @classmethod
    def of(cls, config: LocalIdentityConfig) -> "LocalPrincipalEligibility":
        """The gate over the principals *config* names on its API keys."""

        return cls(principals=frozenset(entry.principal_id for entry in config.api_keys.values()))

    # ....................... #

    async def require_authentication_allowed(
        self,
        principal_id: UUID,
        *,
        for_update: bool = False,
    ) -> None:
        del for_update  # the config has no row to lock

        if principal_id not in self.principals:
            raise exc.authentication("Principal not found")
