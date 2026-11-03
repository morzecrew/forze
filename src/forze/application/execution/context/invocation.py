import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from enum import StrEnum
from typing import Any, Final, final
from uuid import UUID

import attrs
from structlog.contextvars import bind_contextvars, reset_contextvars

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc

from .deadline import bind_deadline, current_deadline, remaining_time

# ----------------------- #

EXEC_ID_KEY: Final = "execution_id"
CORR_ID_KEY: Final = "correlation_id"
CAUS_ID_KEY: Final = "causation_id"
PRINCIPAL_ID_KEY: Final = "principal_id"
ACTOR_ID_KEY: Final = "actor_id"
TENANT_ID_KEY: Final = "tenant_id"
IDEMPOTENCY_KEY_KEY: Final = "idempotency_key"
SURFACE_KEY: Final = "invocation_surface"
"""Not bare ``surface``: that is the port-call log key, and an event's own key wins over a
bound one, so a bare key would vanish from exactly the port-call lines."""

# ....................... #


class InvocationSurface(StrEnum):
    """The framework boundaries that mint invocation metadata.

    Attribution, never authority: the value says which boundary minted the metadata, so a
    reader knows which rule produced the bound identity. Nothing may branch on it, since any
    code in the process can construct metadata with any valid token.
    """

    HTTP = "http"
    """``forze_fastapi``'s ``InvocationMetadataMiddleware``: generated routes and SSE."""

    WEBSOCKET = "websocket"
    """``forze_fastapi`` realtime WebSocket command frames."""

    SOCKETIO = "socketio"
    """``forze_socketio`` command events."""

    MCP = "mcp"
    """``forze_mcp`` tool calls."""

    CLI = "cli"
    """``forze ops call``."""

    INBOX = "inbox"
    """``process_with_inbox``, once per delivery."""

    TEMPORAL = "temporal"
    """``forze_temporal``'s interceptors, for workflows and activities."""

    INNGEST = "inngest"
    """``forze_inngest`` function calls."""

    DURABLE = "durable"
    """The self-hosted durable runner in ``forze_kits.integrations.durable``, per attempt."""

    UNKNOWN = "unknown"
    """No boundary said: the default, and what an operation run outside any boundary logs."""


_KNOWN_SURFACES: Final = frozenset(InvocationSurface)
_SURFACE_TOKEN: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")


def _check_surface(_instance: object, _attribute: object, value: object) -> None:
    # The set lookup is the hot path: every framework boundary passes a well-known value.
    if isinstance(value, str) and (value in _KNOWN_SURFACES or _SURFACE_TOKEN.fullmatch(value)):
        return

    raise exc.configuration(
        "An invocation surface must be a short token matching ^[a-z][a-z0-9_]{0,31}$, "
        "a literal in the boundary's own source",
        code="invocation_surface_invalid",
    )


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

    surface: str = attrs.field(default=InvocationSurface.UNKNOWN, validator=_check_surface)
    """The boundary that minted this metadata (see :class:`InvocationSurface`).

    Set by boundary code from a literal, never from anything the caller sent. An
    application's own boundary passes its own token, matching ``^[a-z][a-z0-9_]{0,31}$``;
    anything else raises ``configuration`` with code ``invocation_surface_invalid``. A token
    must not name a person, a tenant or a customer: it reaches logs and spans.
    """


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

    def get_execution_id(self) -> UUID | None:
        """Return the current invocation's execution id, or ``None`` outside one.

        A bound accessor so wiring can hand it to an adapter the way ``get_tenant`` is
        handed over — the idempotency stores fence a claim on it, and two duplicates of
        one request are two invocations, which is exactly the pair their arguments cannot
        tell apart. The *execution* id and not the correlation id: this one is minted
        server-side per invocation, where the correlation id arrives in a client header
        and could be forged into a way to steal someone else's claim.
        """

        metadata = self.__metadata.get()

        return metadata.execution_id if metadata is not None else None

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
            SURFACE_KEY: metadata.surface,
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
        """Log fields contributed by the identity pair (shared by ``bind*`` variants).

        Every key is set: an absent one to ``Ellipsis``, which structlog treats as unbound, so
        a nested scope never logs the principal, actor or tenant of the scope around it.
        """

        actor = None if authn is None else authn.actor

        return {
            PRINCIPAL_ID_KEY: ... if authn is None else authn.principal_id,
            ACTOR_ID_KEY: ... if actor is None else actor.principal_id,
            TENANT_ID_KEY: ... if tenant is None else str(tenant.tenant_id),
        }

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

        log_tokens = bind_contextvars(**self._identity_log_fields(authn=authn, tenant=tenant))

        try:
            yield

        finally:
            reset_contextvars(**log_tokens)
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
