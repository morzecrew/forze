"""Execution wiring for the in-memory mock integration."""

from __future__ import annotations

from typing import Any, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
    AnalyticsSpec,
)
from forze.application.contracts.cache import CacheDepKey, CachePort, CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
    IdempotencyPort,
    IdempotencySpec,
)
from forze.application.contracts.outbox import (
    OutboxCommandDepKey,
    OutboxQueryDepKey,
    OutboxSpec,
)
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
    PubSubSpec,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.contracts.storage import StorageDepKey, StoragePort, StorageSpec
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamGroupQueryDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.contracts.transaction import (
    TransactionManagerDepKey,
    TransactionManagerPort,
)
from forze.application.execution import Deps, DepsModule, ExecutionContext

from .outbox_adapter import MockOutboxAdapter
from .adapters import (
    MockAnalyticsAdapter,
    MockCacheAdapter,
    MockCounterAdapter,
    MockDocumentAdapter,
    MockIdempotencyAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStreamGroupAdapter,
    MockTxManagerAdapter,
)

# ----------------------- #

DocSpec = DocumentSpec[Any, Any, Any, Any]

MockStateDepKey: DepKey[MockState] = DepKey("mock_state")
"""Dependency key used to register the shared :class:`MockState`."""


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockDocument:
    """Build a :class:`MockDocumentAdapter` for any document spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocSpec,
    ) -> MockDocumentAdapter[Any, Any, Any, Any]:
        state = context.deps.provide(MockStateDepKey)
        domain_model = None
        if spec.write is not None:
            domain_model = spec.write["domain"]

        return MockDocumentAdapter[Any, Any, Any, Any](
            spec=spec,
            state=state,
            namespace=spec.name,
            read_model=spec.read,
            domain_model=domain_model,
        )


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockAnalytics:
    """Build a :class:`MockAnalyticsAdapter` for any analytics spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: AnalyticsSpec[Any, Any],
    ) -> MockAnalyticsAdapter[Any, Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockAnalyticsAdapter(state=state, spec=spec)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockSearch:
    """Build a :class:`MockSearchAdapter` for any search spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> MockSearchAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockSearchAdapter(state=state, spec=spec)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockCounter:
    """Build a :class:`MockCounterAdapter` for any counter spec route."""

    def __call__(self, context: ExecutionContext, spec: CounterSpec) -> CounterPort:
        state = context.deps.provide(MockStateDepKey)
        return MockCounterAdapter(state=state, namespace=spec.name)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockCache:
    """Build a :class:`MockCacheAdapter` for any cache spec route."""

    def __call__(self, context: ExecutionContext, spec: CacheSpec) -> CachePort:
        state = context.deps.provide(MockStateDepKey)
        return MockCacheAdapter(state=state, namespace=spec.name)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockIdempotency:
    """Build a :class:`MockIdempotencyAdapter` for any idempotency spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: IdempotencySpec,
    ) -> IdempotencyPort:
        state = context.deps.provide(MockStateDepKey)
        return MockIdempotencyAdapter(state=state, namespace=spec.name)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockStorage:
    """Build a :class:`MockStorageAdapter` for any storage spec route."""

    def __call__(self, context: ExecutionContext, spec: StorageSpec) -> StoragePort:
        state = context.deps.provide(MockStateDepKey)
        return MockStorageAdapter(state=state, bucket=spec.name)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockQueue:
    """Build a :class:`MockQueueAdapter` for any queue spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: QueueSpec[Any],
    ) -> MockQueueAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockQueueAdapter(state=state, namespace=spec.name, codec=spec.codec)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockPubSub:
    """Build a :class:`MockPubSubAdapter` for any pub/sub spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: PubSubSpec[Any],
    ) -> MockPubSubAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockPubSubAdapter(state=state, namespace=spec.name, codec=spec.codec)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockStream:
    """Build a :class:`MockStreamAdapter` for any stream spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockStreamAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockStreamAdapter(state=state, namespace=spec.name, codec=spec.codec)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockStreamGroup:
    """Build a :class:`MockStreamGroupAdapter` for any stream spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: StreamSpec[Any],
    ) -> MockStreamGroupAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        stream = MockStreamAdapter(state=state, namespace=spec.name, codec=spec.codec)
        return MockStreamGroupAdapter(stream=stream, state=state, namespace=spec.name)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMockOutbox:
    """Build a :class:`MockOutboxAdapter` for any outbox spec route."""

    def __call__(
        self,
        context: ExecutionContext,
        spec: OutboxSpec[Any],
    ) -> MockOutboxAdapter[Any]:
        state = context.deps.provide(MockStateDepKey)
        return MockOutboxAdapter(ctx=context, spec=spec, state=state)


def mock_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    """Build a no-op transaction manager for mock environments."""
    del context
    return MockTxManagerAdapter()


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockDepsModule(DepsModule):
    """Dependency module that registers all in-memory mock adapters."""

    state: MockState = attrs.field(factory=MockState)

    # ....................... #

    def __call__(self) -> Deps:
        document = ConfigurableMockDocument()
        return Deps.plain(
            {
                MockStateDepKey: self.state,
                DocumentQueryDepKey: document,
                DocumentCommandDepKey: document,
                SearchQueryDepKey: ConfigurableMockSearch(),
                AnalyticsQueryDepKey: ConfigurableMockAnalytics(),
                AnalyticsIngestDepKey: ConfigurableMockAnalytics(),
                CounterDepKey: ConfigurableMockCounter(),
                CacheDepKey: ConfigurableMockCache(),
                IdempotencyDepKey: ConfigurableMockIdempotency(),
                StorageDepKey: ConfigurableMockStorage(),
                TransactionManagerDepKey: mock_txmanager,
                QueueQueryDepKey: ConfigurableMockQueue(),
                QueueCommandDepKey: ConfigurableMockQueue(),
                PubSubCommandDepKey: ConfigurableMockPubSub(),
                PubSubQueryDepKey: ConfigurableMockPubSub(),
                StreamQueryDepKey: ConfigurableMockStream(),
                StreamCommandDepKey: ConfigurableMockStream(),
                StreamGroupQueryDepKey: ConfigurableMockStreamGroup(),
                OutboxCommandDepKey: ConfigurableMockOutbox(),
                OutboxQueryDepKey: ConfigurableMockOutbox(),
            }
        )
