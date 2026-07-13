"""Policy evaluation for document-backed grants."""

from collections.abc import Iterable
from typing import Final, final

import attrs

from forze.application.contracts.authz import AuthzDecision, AuthzRequest, EffectiveGrants

# ----------------------- #

DEFAULT_OWNER_OVERRIDE_PERMISSIONS: Final[frozenset[str]] = frozenset(
    {"admin", "{resource_type}.admin"},
)
"""Default owner-override permission key templates.

A principal holding any of these permissions bypasses the ``owner_id`` ABAC
check in :meth:`AuthzPolicyService.decide`. The literal ``{resource_type}``
placeholder is substituted with the request's resource type at evaluation
time, so the defaults reserve the catalog keys ``admin`` (global override)
and ``<resource_type>.admin`` (per-type override, e.g. ``invoice.admin``).
"""

# ....................... #


def freeze_permission_keys(value: Iterable[str]) -> frozenset[str]:
    """Normalize an iterable of permission keys to a frozenset (attrs converter)."""

    return frozenset(value)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzPolicyService:
    """Evaluate authorization using catalog permission keys and optional ABAC hints.

    .. warning:: **Reserved owner-override keys.** By default the catalog
        permission keys ``admin`` and ``<resource_type>.admin`` (e.g.
        ``invoice.admin``) are *owner-override* permissions: holding either
        bypasses the ``owner_id`` ownership check on resources. Do not define
        unrelated app permissions under those names, or rename/disable the
        override set via :attr:`owner_override_permissions` (wired through
        ``AuthzKernelConfig(owner_override_permissions=...)``).
    """

    owner_override_permissions: frozenset[str] = attrs.field(
        default=DEFAULT_OWNER_OVERRIDE_PERMISSIONS,
        converter=freeze_permission_keys,
    )
    """Permission key templates that bypass the ``owner_id`` ABAC check.

    The literal ``{resource_type}`` placeholder is substituted with the
    request's resource type at evaluation time. Defaults to
    :data:`DEFAULT_OWNER_OVERRIDE_PERMISSIONS` (``admin`` and
    ``{resource_type}.admin``); pass an empty set to always enforce the
    ownership check, or different keys to rename the override convention.
    """

    # ....................... #

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
                    override_keys = {
                        key.replace("{resource_type}", resource.resource_type)
                        for key in self.owner_override_permissions
                    }
                    if not any(p.permission_key in override_keys for p in grants.permissions):
                        return AuthzDecision(
                            allowed=False,
                            reason="Resource owner does not match subject",
                        )

        return AuthzDecision(
            allowed=True,
            matched_permission_key=matched.permission_key,
        )
