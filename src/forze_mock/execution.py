"""Execution wiring for the in-memory mock integration."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Optional, final

import attrs

from forze.application.contracts.cache import (
    CacheDepKey,
    CacheDepPort,
    CachePort,
    CacheSpec,
)
from forze.application.contracts.counter import (
    CounterDepKey,
    CounterDepPort,
    CounterPort,
)
from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import (
    DocumentConformity,
    DocumentDepConformity,
    DocumentReadDepKey,
    DocumentSpec,
    DocumentWriteDepKey,
)
from forze.application.contracts.idempotency import (
    IdempotencyDepKey,
    IdempotencyDepPort,
    IdempotencyPort,
)
from forze.application.contracts.pubsub import (
    PubSubConformity,
    PubSubDepConformity,
    PubSubPublishDepKey,
    PubSubSpec,
    PubSubSubscribeDepKey,
)
from forze.application.contracts.queue import (
    QueueConformity,
    QueueDepConformity,
    QueueReadDepKey,
    QueueSpec,
    QueueWriteDepKey,
)
from forze.application.contracts.search import (
    SearchReadDepKey,
    SearchReadDepPort,
    SearchReadPort,
    SearchSpec,
)
from forze.application.contracts.storage import (
    StorageDepKey,
    StorageDepPort,
    StoragePort,
)
from forze.application.contracts.stream import (
    StreamConformity,
    StreamDepConformity,
    StreamGroupDepKey,
    StreamGroupDepPort,
    StreamGroupPort,
    StreamReadDepKey,
    StreamWriteDepKey,
)
from forze.application.contracts.stream.specs import StreamSpec
from forze.application.contracts.tx import (
    TxManagerDepKey,
    TxManagerDepPort,
    TxManagerPort,
)
from forze.application.execution import Deps, DepsModule, ExecutionContext
from forze.base.typing import conforms_to

from .adapters import (
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


@conforms_to(DocumentDepConformity)
def mock_document(
    context: ExecutionContext,
    spec: DocSpec,
    cache: Optional[CachePort] = None,
) -> DocumentConformity:
    del cache
    state = context.dep(MockStateDepKey)
    domain_model = None
    if spec.write is not None:
        domain_model = spec.write["models"]["domain"]

    return MockDocumentAdapter[Any, Any, Any, Any](
        state=state,
        namespace=spec.namespace,
        read_model=spec.read["model"],
        domain_model=domain_model,
    )


@conforms_to(SearchReadDepPort)
def mock_search(
    context: ExecutionContext,
    spec: SearchSpec[Any],
) -> SearchReadPort[Any]:
    state = context.dep(MockStateDepKey)
    return MockSearchAdapter(state=state, spec=spec)


@conforms_to(CounterDepPort)
def mock_counter(context: ExecutionContext, namespace: str) -> CounterPort:
    state = context.dep(MockStateDepKey)
    return MockCounterAdapter(state=state, namespace=namespace)


@conforms_to(CacheDepPort)
def mock_cache(context: ExecutionContext, spec: CacheSpec) -> CachePort:
    state = context.dep(MockStateDepKey)
    return MockCacheAdapter(state=state, namespace=spec.namespace)


@conforms_to(IdempotencyDepPort)
def mock_idempotency(
    context: ExecutionContext,
    ttl: timedelta = timedelta(seconds=30),
) -> IdempotencyPort:
    del ttl
    state = context.dep(MockStateDepKey)
    return MockIdempotencyAdapter(state=state, namespace="idempotency")


@conforms_to(StorageDepPort)
def mock_storage(context: ExecutionContext, bucket: str) -> StoragePort:
    state = context.dep(MockStateDepKey)
    return MockStorageAdapter(state=state, bucket=bucket)


@conforms_to(TxManagerDepPort)
def mock_txmanager(context: ExecutionContext) -> TxManagerPort:
    del context
    return MockTxManagerAdapter()


@conforms_to(QueueDepConformity)
def mock_queue(
    context: ExecutionContext,
    spec: QueueSpec[Any],
) -> QueueConformity:
    state = context.dep(MockStateDepKey)
    return MockQueueAdapter(state=state, namespace=spec.namespace, model=spec.model)


@conforms_to(PubSubDepConformity)
def mock_pubsub(
    context: ExecutionContext,
    spec: PubSubSpec[Any],
) -> PubSubConformity:
    state = context.dep(MockStateDepKey)
    return MockPubSubAdapter(state=state, namespace=spec.namespace, model=spec.model)


@conforms_to(StreamDepConformity)
def mock_stream(
    context: ExecutionContext,
    spec: StreamSpec[Any],
) -> StreamConformity:
    state = context.dep(MockStateDepKey)
    return MockStreamAdapter(state=state, namespace=spec.namespace, model=spec.model)


@conforms_to(StreamGroupDepPort)
def mock_stream_group(
    context: ExecutionContext,
    spec: StreamSpec[Any],
) -> StreamGroupPort[Any]:
    state = context.dep(MockStateDepKey)
    stream = MockStreamAdapter(state=state, namespace=spec.namespace, model=spec.model)
    return MockStreamGroupAdapter(stream=stream, state=state, namespace=spec.namespace)


# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MockDepsModule(DepsModule):
    """Dependency module that registers all in-memory mock adapters."""

    state: MockState = attrs.field(factory=MockState)

    # ....................... #

    def __call__(self) -> Deps:
        return Deps(
            {
                MockStateDepKey: self.state,
                DocumentReadDepKey: mock_document,
                DocumentWriteDepKey: mock_document,
                SearchReadDepKey: mock_search,
                CounterDepKey: mock_counter,
                CacheDepKey: mock_cache,
                IdempotencyDepKey: mock_idempotency,
                StorageDepKey: mock_storage,
                TxManagerDepKey: mock_txmanager,
                QueueReadDepKey: mock_queue,
                QueueWriteDepKey: mock_queue,
                PubSubPublishDepKey: mock_pubsub,
                PubSubSubscribeDepKey: mock_pubsub,
                StreamReadDepKey: mock_stream,
                StreamWriteDepKey: mock_stream,
                StreamGroupDepKey: mock_stream_group,
            }
        )
