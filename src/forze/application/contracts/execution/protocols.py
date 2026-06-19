from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

    from .value_objects import Outcome

# ----------------------- #

type MiddlewareNextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or operation handler in the chain."""

# ....................... #


class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain."""

    def __call__(
        self,
        next: MiddlewareNextCall[Args, R],
        args: Args,
    ) -> Awaitable[R]: ...


# ....................... #


class Before[Args](Protocol):  # pragma: no cover
    """Protocol for a hook that runs before the operation handler."""

    def __call__(self, args: Args) -> Awaitable[None]: ...


# ....................... #


class OnSuccess[Args, R](Protocol):  # pragma: no cover
    """Protocol for a hook that runs after the operation handler succeeds."""

    def __call__(self, args: Args, result: R) -> Awaitable[None]: ...


# ....................... #


class OnFailure[Args](Protocol):  # pragma: no cover
    """Protocol for a hook that runs when the operation fails past its guards.

    Two triggers: the wrap chain / handler raises, **or** an ``on_success`` /
    dispatch hook raises after the handler already succeeded (the operation
    still fails as a whole, so failure observers fire even though the
    handler's own work completed). Never runs when a ``before`` guard
    (authn/authz/tenancy) denies the operation.
    """

    def __call__(self, args: Args, exc: Exception) -> Awaitable[None]: ...


# ....................... #


class Finally[Args, R](Protocol):  # pragma: no cover
    """Protocol for a hook that runs after the operation handler finishes (success or failure).

    Always runs once the scope is entered — including when a ``before`` hook raises,
    in which case the outcome is a ``Failure`` carrying the guard error.
    """

    def __call__(
        self,
        args: Args,
        outcome: "Outcome[R]",  # noqa: F841
    ) -> Awaitable[None]: ...


# ....................... #


class Handler[Args, R](Protocol):  # pragma: no cover
    """Protocol for an operation handler that can be executed."""

    def __call__(self, args: Args) -> Awaitable[R]: ...


# ....................... #


class TwoPhaseHandler[Args, Payload, R](Protocol):  # pragma: no cover
    """Protocol for a two-phase handler: ``prepare`` outside the transaction,
    ``apply`` inside it.

    ``prepare`` runs in the outer scope **before** the transaction opens — the
    place for parsing, CPU work, or external calls — and returns a ``Payload`` the
    engine threads into ``apply``, which runs **inside** the transaction and does
    the writes. The transaction therefore wraps only ``apply``, not the pre-work.

    ``prepare`` runs under the read-only flag and must not acquire a command
    (write) port; its database reads run outside ``apply``'s transaction (no
    read/write atomicity — validate on write in ``apply``). ``prepare`` may run
    more than once if the operation carries a retry/hedge wrap (see
    ``prepare_rerun_safe`` on the plan), so keep it free of non-idempotent
    external effects.
    """

    def prepare(self, args: Args) -> Awaitable[Payload]: ...

    def apply(self, args: Args, payload: Payload) -> Awaitable[R]: ...


# ....................... #


class LifecycleHook(Protocol):
    """Protocol for a lifecycle hook that can be executed."""

    def __call__(self, ctx: "ExecutionContext") -> Awaitable[None]: ...


# ....................... #
# Factories


class MiddlewareFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a middleware."""

    def __call__(self, ctx: "ExecutionContext") -> Middleware[Any, Any]: ...


# ....................... #


@runtime_checkable
class ProvidesIdempotency(Protocol):  # pragma: no cover
    """Marker: a middleware factory that deduplicates an operation's effects.

    Detected structurally at freeze time (the validator is contracts-only and cannot
    import the hook classes) to satisfy the hedging safety gate.
    """

    def provides_idempotency(self) -> bool: ...


# ....................... #


@runtime_checkable
class DeclaresHedge(Protocol):  # pragma: no cover
    """Marker: a middleware factory that hedges an operation (concurrent duplicates).

    ``hedge_safety_declared`` reports whether an explicit safety basis was given; the
    freeze-time gate requires that or a sibling :class:`ProvidesIdempotency`.
    """

    def hedge_safety_declared(self) -> bool: ...


# ....................... #


@runtime_checkable
class MayReplayHandler(Protocol):  # pragma: no cover
    """Marker: a middleware factory that may run the wrapped operation more than
    once (retry) or concurrently (hedge).

    Detected structurally at freeze time (like :class:`DeclaresHedge`) so a
    two-phase operation can be gated: if such a wrap is present, the operation's
    ``prepare`` phase may re-run, so the plan must declare ``prepare`` safe to
    re-run (``OperationPlan.prepare_rerun_safe``).
    """

    def may_replay_handler(self) -> bool: ...


# ....................... #


@runtime_checkable
class DeclaresAuthz(Protocol):  # pragma: no cover
    """Marker: a hook factory that declares the permission keys it enforces.

    Detected structurally at freeze time (like :class:`ProvidesIdempotency`) so the
    operation catalog can surface the union of declared permission keys per operation.

    Honesty caveat: this is declared-hook introspection, **not** a security statement.
    It only sees hooks attached to the plan that opt into this protocol — an operation
    may enforce authorization inside its handler (or via an undeclared hook) invisibly,
    and a hook may declare no named key while still scoping/denying access.
    """

    def permission_keys(self) -> tuple[str, ...]: ...


# ....................... #


@runtime_checkable
class DeclaresAuthn(Protocol):  # pragma: no cover
    """Marker: a hook factory that declares it requires an authenticated principal.

    Detected structurally at freeze time (like :class:`DeclaresAuthz`) so the operation
    catalog can surface, per operation, whether a bound principal is required — which
    transports project into their auth descriptions (OpenAPI ``security``, MCP tool
    text). An authorization hook (:class:`DeclaresAuthz`) implies this too: you cannot
    check a principal's grants without a principal.

    Honesty caveat: declared-hook introspection, **not** a security statement. A
    ``False`` result does not prove the operation is open — its handler may enforce
    authentication invisibly.
    """

    def requires_authn(self) -> bool: ...


# ....................... #


class BeforeFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a before hook."""

    def __call__(self, ctx: "ExecutionContext") -> Before[Any]: ...


# ....................... #


class OnSuccessFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a on success hook."""

    def __call__(self, ctx: "ExecutionContext") -> OnSuccess[Any, Any]: ...


# ....................... #


class OnFailureFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a on failure hook."""

    def __call__(self, ctx: "ExecutionContext") -> OnFailure[Any]: ...


# ....................... #


class FinallyFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a finally hook."""

    def __call__(self, ctx: "ExecutionContext") -> Finally[Any, Any]: ...


# ....................... #


class HandlerFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a handler."""

    def __call__(self, ctx: "ExecutionContext") -> Handler[Any, Any]: ...
