from typing import Callable, final

import attrs

from forze.application.contracts.analytics import AnalyticsDeps
from forze.application.contracts.authn import AuthnDeps
from forze.application.contracts.authz import AuthzDeps
from forze.application.contracts.cache import CacheDeps
from forze.application.contracts.counter import CounterDeps
from forze.application.contracts.dlock import DistributedLockDeps
from forze.application.contracts.document import DocumentDeps
from forze.application.contracts.embeddings import EmbeddingsDeps
from forze.application.contracts.http import HttpServiceDeps
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

    embeddings: EmbeddingsDeps = attrs.field(factory=EmbeddingsDeps, init=False)
    """Embeddings dependencies."""

    dlock: DistributedLockDeps = attrs.field(factory=DistributedLockDeps, init=False)
    """Distributed lock dependencies."""

    #! maybe rename to TenancyDeps, TenancyResolver, TenancyManager
    tenancy: TenancyDeps = attrs.field(factory=TenancyDeps, init=False)
    """Tenancy dependencies."""

    authz: AuthzDeps = attrs.field(factory=AuthzDeps, init=False)
    """Authorization dependencies."""

    authn: AuthnDeps = attrs.field(factory=AuthnDeps, init=False)
    """Authentication dependencies."""

    transaction: TransactionDeps = attrs.field(factory=TransactionDeps, init=False)
    """Transaction dependencies."""

    # ....................... #

    @property
    def doc(self) -> DocumentDeps:
        """Document dependencies (alias for :attr:`document`)."""

        return self.document

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
        self.embeddings.lock(self)
        self.dlock.lock(self)
        self.tenancy.lock(self)
        self.authz.lock(self)
        self.authn.lock(self)
        self.transaction.lock(self)

        self.tx_ctx.lock(
            self.transaction,
            tx_tracer=tx_tracer_from_runtime(self.deps.runtime_tracer),
        )


# ....................... #

ExecutionContextFactory = Callable[[], ExecutionContext]
"""Factory callable for creating :class:`ExecutionContext` instances."""
