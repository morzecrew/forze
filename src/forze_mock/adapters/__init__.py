"""In-memory adapters implementing Forze application contracts."""

from __future__ import annotations

from forze_mock.adapters.analytics import MockAnalyticsAdapter
from forze_mock.adapters.cache import MockCacheAdapter
from forze_mock.adapters.counter import MockCounterAdapter
from forze_mock.adapters.crypto import MockKeyManagement
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mock.adapters.embeddings import MockHashEmbeddingsProvider
from forze_mock.adapters.durable import (
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
)
from forze_mock.adapters.events import RecordingAuthnEventSink
from forze_mock.adapters.graph import MockGraphAdapter
from forze_mock.adapters.http import MockHttpRegistry, MockHttpServiceAdapter
from forze_mock.adapters.idempotency import MockIdempotencyAdapter
from forze_mock.adapters.inbox import MockInboxAdapter
from forze_mock.adapters.outbox import MockOutboxRow, MockOutboxStore
from forze_mock.adapters.procedure import (
    MockProcedureHandler,
    MockProcedureRegistry,
    MockProcedureAdapter,
)
from forze_mock.adapters.query_params import (
    MockQueryParamsRegistry,
    MockQueryParamsSource,
)
from forze_mock.adapters.pubsub import MockPubSubAdapter
from forze_mock.adapters.queue import MockQueueAdapter
from forze_mock.adapters.resilience import PassthroughResilienceExecutor
from forze_mock.adapters.search import (
    MockFederatedSearchAdapter,
    MockHubSearchAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchManagementAdapter,
    MockSearchResultSnapshotAdapter,
)
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.adapters.stream import (
    MockStreamAdapter,
    MockAckStreamGroupAdapter,
    MockAckStreamGroupAdminAdapter,
    MockCommitStreamGroupAdapter,
    MockCommitStreamGroupAdminAdapter,
)
from forze_mock.adapters.tx import (
    MockJournalTxManagerAdapter,
    MockStrictTxManagerAdapter,
    MockTxManagerAdapter,
)
from forze_mock.state import MockState

__all__ = [
    "MockState",
    "RecordingAuthnEventSink",
    "MockDocumentAdapter",
    "MockSearchAdapter",
    "MockSearchCommandAdapter",
    "MockSearchManagementAdapter",
    "MockSearchResultSnapshotAdapter",
    "MockHubSearchAdapter",
    "MockFederatedSearchAdapter",
    "MockGraphAdapter",
    "MockHttpServiceAdapter",
    "MockHttpRegistry",
    "MockCounterAdapter",
    "MockKeyManagement",
    "MockCacheAdapter",
    "MockIdempotencyAdapter",
    "MockInboxAdapter",
    "MockStorageAdapter",
    "MockTxManagerAdapter",
    "MockStrictTxManagerAdapter",
    "MockJournalTxManagerAdapter",
    "MockQueueAdapter",
    "MockPubSubAdapter",
    "MockStreamAdapter",
    "MockAckStreamGroupAdapter",
    "MockAckStreamGroupAdminAdapter",
    "MockCommitStreamGroupAdapter",
    "MockCommitStreamGroupAdminAdapter",
    "MockAnalyticsAdapter",
    "MockProcedureAdapter",
    "MockProcedureRegistry",
    "MockProcedureHandler",
    "MockQueryParamsRegistry",
    "MockQueryParamsSource",
    "MockDistributedLockAdapter",
    "MockDurableWorkflowCommandAdapter",
    "MockDurableWorkflowQueryAdapter",
    "MockDurableWorkflowScheduleCommandAdapter",
    "MockDurableWorkflowScheduleQueryAdapter",
    "MockDurableFunctionEventAdapter",
    "MockDurableFunctionStepAdapter",
    "MockOutboxStore",
    "MockOutboxRow",
    "MockHashEmbeddingsProvider",
    "PassthroughResilienceExecutor",
]
