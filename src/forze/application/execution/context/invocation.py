from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Final, Iterator, final
from uuid import UUID

import attrs
from structlog.contextvars import bind_contextvars, reset_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity

from .deadline import bind_deadline, current_deadline, remaining_time

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

    def set_read_only(self) -> Token[bool]:
        """Mark the current operation read-only; reset with :meth:`reset_read_only`.

        Engine fast path: a raw ContextVar set/reset pair avoids the
        ``@contextmanager`` generator overhead on the per-operation hot path.
        Prefer :meth:`bind_read_only` outside the engine.
        """

        return self.__read_only.set(True)

    # ....................... #

    def reset_read_only(self, token: Token[bool]) -> None:
        """Reset the read-only flag to its state before :meth:`set_read_only`."""

        self.__read_only.reset(token)

    # ....................... #

    @contextmanager
    def bind_read_only(self) -> Iterator[None]:
        """Bind the current operation as read-only for its duration (a ``QUERY`` op)."""

        token = self.set_read_only()

        try:
            yield

        finally:
            self.reset_read_only(token)

    # ....................... #

    def get_deadline(self) -> float | None:
        """The absolute monotonic deadline for the current invocation, if any.

        The deadline is task-scoped module-level state (see
        :mod:`~forze.application.execution.context.deadline`) so the engine and
        the resilience executor can read it without wiring; this accessor is
        the boundary-facing surface.
        """

        return current_deadline()

    # ....................... #

    def remaining_time(self) -> float | None:
        """Seconds left until the invocation deadline (clamped at ``0.0``), or ``None``."""

        return remaining_time()

    # ....................... #

    @contextmanager
    def bind_deadline(self, timeout: float | None) -> Iterator[None]:
        """Bind an invocation deadline of *timeout* seconds from now (tighten-only).

        ``None`` is a no-op passthrough. See
        :func:`~forze.application.execution.context.deadline.bind_deadline`.
        """

        with bind_deadline(timeout):
            yield

    # ....................... #

    @contextmanager
    def bind_idempotency(self, key: str | None) -> Iterator[None]:
        """Bind the idempotency key for the current invocation."""

        token = self.__idempotency_key.set(key)

        try:
            if key is not None:
                log_tokens = bind_contextvars(**{IDEMPOTENCY_KEY_KEY: key})

                try:
                    yield

                finally:
                    reset_contextvars(**log_tokens)

            else:
                # Nothing to bind: skip the structlog save/restore entirely.
                yield

        finally:
            self.__idempotency_key.reset(token)

    # ....................... #

    @staticmethod
    def _metadata_log_fields(metadata: InvocationMetadata) -> dict[str, Any]:
        """Log fields contributed by *metadata* (shared by ``bind*`` variants)."""

        bound: dict[str, Any] = {
            EXEC_ID_KEY: str(metadata.execution_id),
            CORR_ID_KEY: str(metadata.correlation_id),
        }

        if metadata.causation_id is not None:
            bound[CAUS_ID_KEY] = str(metadata.causation_id)

        return bound

    # ....................... #

    @staticmethod
    def _identity_log_fields(
        *,
        authn: AuthnIdentity | None,
        tenant: TenantIdentity | None,
    ) -> dict[str, Any]:
        """Log fields contributed by the identity pair (shared by ``bind*`` variants)."""

        bound: dict[str, Any] = {}

        if authn is not None:
            bound[PRINCIPAL_ID_KEY] = authn.principal_id

            if authn.actor is not None:
                bound[ACTOR_ID_KEY] = authn.actor.principal_id

        if tenant is not None:
            bound[TENANT_ID_KEY] = str(tenant.tenant_id)

        return bound

    # ....................... #

    @contextmanager
    def bind_metadata(self, *, metadata: InvocationMetadata) -> Iterator[None]:
        """Bind the invocation metadata."""

        metadata_token = self.__metadata.set(metadata)

        log_tokens = bind_contextvars(**self._metadata_log_fields(metadata))

        try:
            yield

        finally:
            reset_contextvars(**log_tokens)
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

        bound = self._identity_log_fields(authn=authn, tenant=tenant)

        try:
            if bound:
                log_tokens = bind_contextvars(**bound)

                try:
                    yield

                finally:
                    reset_contextvars(**log_tokens)

            else:
                # Nothing to bind: skip the structlog save/restore entirely.
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
        """Bind the invocation context.

        Observably equivalent to :meth:`bind_metadata` composed with
        :meth:`bind_identity` — same ContextVars and log fields bound for the
        scope, token-reset on exit (exception-safe) — but implemented as a single
        token-based structlog bind over the merged log fields, so a boundary pays
        one structlog save/restore instead of two.
        """

        metadata_token = self.__metadata.set(metadata)
        authn_token = self.__authn.set(authn)
        tenant_token = self.__tenant.set(tenant)

        bound = self._metadata_log_fields(metadata)
        bound.update(self._identity_log_fields(authn=authn, tenant=tenant))

        log_tokens = bind_contextvars(**bound)

        try:
            yield

        finally:
            # Reverse order of the sets above (mirrors the CM composition).
            reset_contextvars(**log_tokens)
            self.__tenant.reset(tenant_token)
            self.__authn.reset(authn_token)
            self.__metadata.reset(metadata_token)
