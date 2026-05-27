"""Policy evaluation for document-backed grants."""

from typing import final

import attrs

from forze.application.contracts.authz import AuthzDecision, AuthzRequest, EffectiveGrants

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzPolicyService:
    """Evaluate authorization using catalog permission keys and optional ABAC hints."""

    def decide(
        self,
        grants: EffectiveGrants,
        request: AuthzRequest,
        *,
        principal_active: bool = True,
    ) -> AuthzDecision:
        """Return an :class:`AuthzDecision` for ``request``."""

        if not principal_active:
            return AuthzDecision(
                allowed=False,
                reason="Policy principal is inactive",
            )

        action = request.action

        matched = next(
            (p for p in grants.permissions if p.permission_key == action),
            None,
        )

        if matched is None:
            return AuthzDecision(
                allowed=False,
                reason=f"No grant for permission {action!r}",
            )

        resource = request.resource

        if resource is not None:
            owner_id = resource.attributes.get("owner_id")

            if owner_id is not None:
                subject_id = request.subject.principal_id

                if str(owner_id) != str(subject_id):
                    admin_keys = {f"{resource.resource_type}.admin", "admin"}
                    if not any(
                        p.permission_key in admin_keys for p in grants.permissions
                    ):
                        return AuthzDecision(
                            allowed=False,
                            reason="Resource owner does not match subject",
                        )

        return AuthzDecision(
            allowed=True,
            matched_permission_key=matched.permission_key,
        )
