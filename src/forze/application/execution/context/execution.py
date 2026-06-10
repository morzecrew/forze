from typing import Any, Callable, final

import attrs

from forze.base.primitives import StrKey

from forze.application.contracts.analytics import AnalyticsDeps
from forze.application.contracts.authn import AuthnDeps
from forze.application.contracts.authz import AuthzDeps
from forze.application.contracts.cache import CacheDeps
from forze.application.contracts.counter import CounterDeps
from forze.application.contracts.domain import DomainDeps
from forze.application.contracts.dlock import DistributedLockDeps
from forze.application.contracts.document import DocumentDeps
from forze.application.contracts.embeddings import EmbeddingsDeps
from forze.application.contracts.graph import GraphDeps
from forze.application.contracts.http import HttpServiceDeps
from forze.application.contracts.idempotency import IdempotencyDeps
from forze.application.contracts.inbox import InboxDeps
from forze.application.contracts.resilience import ResilienceDeps
from forze.application.contracts.search import SearchDeps
from forze.application.contracts.storage import StorageDeps
from forze.application.contracts.tenancy import TenancyDeps
from forze.application.contracts.outbox import OutboxDeps
from forze.application.contracts.transaction import TransactionDeps

from ..deps import FrozenDeps
from ..deps.tx_tracer import tx_tracer_from_runtime
from ..tracing import bind_active_deps, init_runtime_tracing
from .invocation import InvocationContext
from .outbox_staging import OutboxStagingContext
from .transaction import TransactionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionContext:
    """Execution context."""

    deps: FrozenDeps
    """Dependencies container."""

    cache_operations: bool = attrs.field(default=True)
    """Whether resolved operations are memoized for this scope (see
    :attr:`~forze.application.execution.runtime.ExecutionRuntime.cache_resolved_operations`)."""

    cache_ports: bool = attrs.field(default=True)
    """Whether resolved configurable ports are memoized for this scope (see
    :attr:`~forze.application.execution.runtime.ExecutionRuntime.cache_resolved_ports`)."""

    # ....................... #

    _resolved_op_cache: dict[StrKey, Any] | None = attrs.field(
        default=attrs.Factory(
            lambda self: {} if self.cache_operations else None,
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Per-scope resolved-operation memo (``None`` when caching is disabled)."""

    _resolved_port_cache: dict[Any, tuple[Any, Any]] | None = attrs.field(
        default=attrs.Factory(
            lambda self: {} if self.cache_ports else None,
            takes_self=True,
        ),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Per-scope resolved-port memo: ``(dep key, route) -> (spec, port)`` (``None`` when
    caching is disabled)."""

    lifecycle_started: set[StrKey] = attrs.field(
        factory=set,
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )
    """Lifecycle step ids that completed startup and were not yet shut down.

    Managed by the lifecycle runner so each step's shutdown runs at most once per
    successful startup and never without one (e.g. after a partial startup failure)."""

    # ....................... #

    tx_ctx: TransactionContext = attrs.field(factory=TransactionContext, init=False)
    """Transaction context."""

    inv_ctx: InvocationContext = attrs.field(factory=InvocationContext, init=False)
    """Invocation context."""

    outbox_staging: OutboxStagingContext = attrs.field(
        factory=OutboxStagingContext,
        init=False,
    )
    """Outbox staging buffer for the current request."""

    # ....................... #

    outbox: OutboxDeps = attrs.field(factory=OutboxDeps, init=False)
    """Outbox dependencies."""

    document: DocumentDeps = attrs.field(factory=DocumentDeps, init=False)
    """Document dependencies."""

    search: SearchDeps = attrs.field(factory=SearchDeps, init=False)
    """Search dependencies."""

    http: HttpServiceDeps = attrs.field(factory=HttpServiceDeps, init=False)
    """Outbound HTTP service dependencies."""

    analytics: AnalyticsDeps = attrs.field(factory=AnalyticsDeps, init=False)
    """Analytics dependencies."""

    cache: CacheDeps = attrs.field(factory=CacheDeps, init=False)
    """Cache dependencies."""

    counter: CounterDeps = attrs.field(factory=CounterDeps, init=False)
    """Counter dependencies."""

    storage: StorageDeps = attrs.field(factory=StorageDeps, init=False)
    """Storage dependencies."""

    graph: GraphDeps = attrs.field(factory=GraphDeps, init=False)
    """Graph database dependencies."""

    embeddings: EmbeddingsDeps = attrs.field(factory=EmbeddingsDeps, init=False)
    """Embeddings dependencies."""

    dlock: DistributedLockDeps = attrs.field(factory=DistributedLockDeps, init=False)
    """Distributed lock dependencies."""

    tenancy: TenancyDeps = attrs.field(factory=TenancyDeps, init=False)
    """Tenancy dependencies."""

    authz: AuthzDeps = attrs.field(factory=AuthzDeps, init=False)
    """Authorization dependencies."""

    authn: AuthnDeps = attrs.field(factory=AuthnDeps, init=False)
    """Authentication dependencies."""

    transaction: TransactionDeps = attrs.field(factory=TransactionDeps, init=False)
    """Transaction dependencies."""

    resilience: ResilienceDeps = attrs.field(factory=ResilienceDeps, init=False)
    """Resilience policy pipeline dependencies."""

    idempotency: IdempotencyDeps = attrs.field(factory=IdempotencyDeps, init=False)
    """Idempotency dependencies."""

    domain: DomainDeps = attrs.field(factory=DomainDeps, init=False)
    """Domain-event dispatch dependencies."""

    inbox: InboxDeps = attrs.field(factory=InboxDeps, init=False)
    """Inbox (consumer-side dedup) dependencies."""

    # ....................... #

    @property
    def doc(self) -> DocumentDeps:
        """Document dependencies (alias for :attr:`document`)."""

        return self.document

    # ....................... #

    def cached_operation(self, op: StrKey) -> Any | None:
        """Return a memoized resolved operation for this scope, or ``None``.

        ``None`` means either a cache miss or caching disabled; callers resolve
        and then call :meth:`store_operation`.
        """

        cache = self._resolved_op_cache

        return cache.get(op) if cache is not None else None

    # ....................... #

    def store_operation(self, op: StrKey, resolved: Any) -> None:
        """Memoize a resolved operation for this scope (no-op when disabled)."""

        cache = self._resolved_op_cache

        if cache is not None:
            cache[op] = resolved

    # ....................... #

    def cached_port(self, key: Any, spec: Any) -> Any | None:
        """Return a memoized port for ``key`` if cached for an *equal* ``spec``.

        Spec match uses an identity fast-path, then value equality — spec types
        are frozen attrs classes, so per-call-constructed but structurally equal
        specs still hit the cache. Returns ``None`` on a miss, a spec mismatch,
        or when caching is disabled; callers resolve and then call
        :meth:`store_port`.
        """

        cache = self._resolved_port_cache

        if cache is None:
            return None

        entry = cache.get(key)

        if entry is not None and (entry[0] is spec or entry[0] == spec):
            return entry[1]

        return None

    # ....................... #

    def store_port(self, key: Any, spec: Any, port: Any) -> None:
        """Memoize a resolved port for this scope (no-op when disabled).

        Stores ``(spec, port)`` keyed by ``(dep key, route)``; a later resolve with a
        non-equal spec on the same key rebuilds and replaces the entry.
        """

        cache = self._resolved_port_cache

        if cache is not None:
            cache[key] = (spec, port)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        bind_active_deps(self.deps)
        init_runtime_tracing(self.deps)

        self.outbox.lock(self)
        self.document.lock(self)
        self.search.lock(self)
        self.http.lock(self)
        self.analytics.lock(self)
        self.cache.lock(self)
        self.counter.lock(self)
        self.storage.lock(self)
        self.graph.lock(self)
        self.embeddings.lock(self)
        self.dlock.lock(self)
        self.tenancy.lock(self)
        self.authz.lock(self)
        self.authn.lock(self)
        self.transaction.lock(self)
        self.resilience.lock(self)
        self.idempotency.lock(self)
        self.domain.lock(self)
        self.inbox.lock(self)

        self.tx_ctx.lock(
            self.transaction,
            tx_tracer=tx_tracer_from_runtime(self.deps.runtime_tracer),
        )


# ....................... #

ExecutionContextFactory = Callable[[], ExecutionContext]
"""Factory callable for creating :class:`ExecutionContext` instances."""
