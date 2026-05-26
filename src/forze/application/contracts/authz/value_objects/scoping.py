"""Data-scoping value objects."""

from uuid import UUID

import attrs

from forze.application.contracts.querying import QueryFilterExpression

from .decision import AuthzScope, AuthzSubject

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzDocumentScopeRequest:
    """Request to derive query constraints for a document-backed operation."""

    subject: AuthzSubject
    scope: AuthzScope
    document_name: str
    operation: str
    action: str | None = None
    base_filters: QueryFilterExpression | None = None  # type: ignore[valid-type]


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzDocumentScope:
    """Merged filters and metadata for scoped document access."""

    filters: QueryFilterExpression | None = None  # type: ignore[valid-type]
    deny_all: bool = False
    reason: str | None = None


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzSensitiveAccessRequest:
    """Explicit check before loading a secondary resource by id."""

    subject: AuthzSubject
    scope: AuthzScope
    resource_type: str
    resource_id: UUID
    action: str
