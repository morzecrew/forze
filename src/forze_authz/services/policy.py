"""Permission evaluation against resolved :class:`EffectiveGrants`."""

from collections.abc import Mapping
from typing import Any, final

import attrs

from forze.application.contracts.authz import EffectiveGrants

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzPolicyService:
    """Evaluate ``permits`` using catalog permission keys on resolved grants."""

    # ....................... #

    def permits(
        self,
        grants: EffectiveGrants,
        permission_key: str,
        *,
        resource: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> bool:
        """Return whether ``permission_key`` matches any effective permission ref.

        ``resource`` and ``context`` are reserved for future ABAC-style matchers.
        """

        _ = resource, context

        return any(p.permission_key == permission_key for p in grants.permissions)
