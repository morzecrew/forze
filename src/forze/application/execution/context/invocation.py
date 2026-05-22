from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, final
from uuid import UUID

import attrs
from structlog.contextvars import bound_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InvocationMetadata:
    """Metadata for a single application invocation."""

    execution_id: UUID
    """The ID of the execution."""

    correlation_id: UUID
    """The correlation ID of the invocation."""

    causation_id: UUID | None = attrs.field(default=None)
    """The causation ID of the invocation."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InvocationContext:
    """Context for a single application invocation."""

    __metadata: ContextVar[InvocationMetadata | None] = attrs.field(
        factory=lambda: ContextVar("metadata", default=None),
        init=False,
        repr=False,
    )
    """Current invocation metadata."""

    __authn: ContextVar[AuthnIdentity | None] = attrs.field(
        factory=lambda: ContextVar("authn", default=None),
        init=False,
        repr=False,
    )
    """Current authenticated identity."""

    __tenant: ContextVar[TenantIdentity | None] = attrs.field(
        factory=lambda: ContextVar("tenant", default=None),
        init=False,
        repr=False,
    )
    """Current tenant identity."""

    # ....................... #

    def get_metadata(self) -> InvocationMetadata | None:
        """Return the current invocation metadata."""

        return self.__metadata.get()

    # ....................... #

    def get_authn(self) -> AuthnIdentity | None:
        """Return the current authenticated identity."""

        return self.__authn.get()

    # ....................... #

    def get_tenant(self) -> TenantIdentity | None:
        """Return the current tenant identity."""

        return self.__tenant.get()

    # ....................... #

    @contextmanager
    def bind(
        self,
        *,
        metadata: InvocationMetadata,
        authn: AuthnIdentity | None = None,
        tenant: TenantIdentity | None = None,
    ) -> Iterator[None]:
        """Bind the invocation context."""

        metadata_token = self.__metadata.set(metadata)
        authn_token = self.__authn.set(authn)
        tenant_token = self.__tenant.set(tenant)

        bound: dict[str, Any] = {
            "execution_id": str(metadata.execution_id),
            "correlation_id": str(metadata.correlation_id),
        }

        if metadata.causation_id is not None:
            bound["causation_id"] = str(metadata.causation_id)

        if authn is not None:
            bound["principal_id"] = authn.principal_id

        if tenant is not None:
            bound["tenant_id"] = str(tenant.tenant_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__metadata.reset(metadata_token)
            self.__authn.reset(authn_token)
            self.__tenant.reset(tenant_token)
