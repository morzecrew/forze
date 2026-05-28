"""Decision request/result value objects."""

from typing import Any, Mapping
from uuid import UUID

import attrs

from ..types import PrincipalKind

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzSubject:
    """Caller subject for a decision or scoping call."""

    principal_id: UUID
    kind: PrincipalKind | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzScope:
    """Policy partition (typically tenant) for one authz call."""

    tenant_id: UUID | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzResource:
    """Structured resource target for attribute-aware checks."""

    resource_type: str
    resource_id: UUID | None = None
    attributes: Mapping[str, Any] = attrs.field(factory=dict[str, Any])


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzRequest:
    """Input to :meth:`~forze.application.contracts.authz.ports.AuthzDecisionPort.authorize`."""

    subject: AuthzSubject
    action: str
    scope: AuthzScope = attrs.field(factory=AuthzScope)
    resource: AuthzResource | None = None
    context: Mapping[str, Any] = attrs.field(factory=dict[str, Any])


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzDecision:
    """Result of an authorization decision."""

    allowed: bool
    reason: str | None = None
    matched_permission_key: str | None = None
