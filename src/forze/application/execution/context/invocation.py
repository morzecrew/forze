from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Final, Iterator, final
from uuid import UUID

import attrs
from structlog.contextvars import bound_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #

EXEC_ID_KEY: Final = "execution_id"
CORR_ID_KEY: Final = "correlation_id"
CAUS_ID_KEY: Final = "causation_id"
PRINCIPAL_ID_KEY: Final = "principal_id"
ACTOR_ID_KEY: Final = "actor_id"
TENANT_ID_KEY: Final = "tenant_id"
IDEMPOTENCY_KEY_KEY: Final = "idempotency_key"

# ....................... #


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

    __idempotency_key: ContextVar[str | None] = attrs.field(
        factory=lambda: ContextVar("idempotency_key", default=None),
        init=False,
        repr=False,
    )
    """Idempotency key supplied by the boundary for the current invocation."""

    __read_only: ContextVar[bool] = attrs.field(
        factory=lambda: ContextVar("read_only", default=False),
        init=False,
        repr=False,
    )
    """Whether the current operation is read-only (a ``QUERY`` operation)."""

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

    def get_idempotency_key(self) -> str | None:
        """Return the idempotency key bound by the boundary, if any."""

        return self.__idempotency_key.get()

    # ....................... #

    def is_read_only(self) -> bool:
        """Return whether the current operation is read-only (a ``QUERY`` operation)."""

        return self.__read_only.get()

    # ....................... #

    @contextmanager
    def bind_read_only(self) -> Iterator[None]:
        """Bind the current operation as read-only for its duration (a ``QUERY`` op)."""

        token = self.__read_only.set(True)

        try:
            yield

        finally:
            self.__read_only.reset(token)

    # ....................... #

    @contextmanager
    def bind_idempotency(self, key: str | None) -> Iterator[None]:
        """Bind the idempotency key for the current invocation."""

        token = self.__idempotency_key.set(key)

        bound: dict[str, Any] = {}

        if key is not None:
            bound[IDEMPOTENCY_KEY_KEY] = key

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__idempotency_key.reset(token)

    # ....................... #

    @contextmanager
    def bind_metadata(self, *, metadata: InvocationMetadata) -> Iterator[None]:
        """Bind the invocation metadata."""

        metadata_token = self.__metadata.set(metadata)

        bound: dict[str, Any] = {
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
        }

        if metadata.causation_id is not None:
            bound[CAUS_ID_KEY] = str(metadata.causation_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__metadata.reset(metadata_token)

    # ....................... #

    @contextmanager
    def bind_identity(
        self,
        *,
        authn: AuthnIdentity | None = None,
        tenant: TenantIdentity | None = None,
    ) -> Iterator[None]:
        """Bind the invocation context."""

        authn_token = self.__authn.set(authn)
        tenant_token = self.__tenant.set(tenant)

        bound: dict[str, Any] = {}

        if authn is not None:
            bound[PRINCIPAL_ID_KEY] = authn.principal_id

            if authn.actor is not None:
                bound[ACTOR_ID_KEY] = authn.actor.principal_id

        if tenant is not None:
            bound[TENANT_ID_KEY] = str(tenant.tenant_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__authn.reset(authn_token)
            self.__tenant.reset(tenant_token)

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
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
        }

        if metadata.causation_id is not None:
            bound[CAUS_ID_KEY] = str(metadata.causation_id)

        if authn is not None:
            bound[PRINCIPAL_ID_KEY] = authn.principal_id

            if authn.actor is not None:
                bound[ACTOR_ID_KEY] = authn.actor.principal_id

        if tenant is not None:
            bound[TENANT_ID_KEY] = str(tenant.tenant_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__metadata.reset(metadata_token)
            self.__authn.reset(authn_token)
            self.__tenant.reset(tenant_token)
