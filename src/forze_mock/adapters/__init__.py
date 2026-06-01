"""In-memory adapters implementing Forze application contracts."""

from __future__ import annotations

from forze_mock.adapters.analytics import MockAnalyticsAdapter
from forze_mock.adapters.cache import MockCacheAdapter
from forze_mock.adapters.counter import MockCounterAdapter
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mock.adapters.durable import (
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
)
from forze_mock.adapters.idempotency import MockIdempotencyAdapter
from forze_mock.adapters.pubsub import MockPubSubAdapter
from forze_mock.adapters.queue import MockQueueAdapter
from forze_mock.adapters.search import (
    MockFederatedSearchAdapter,
    MockHubSearchAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
)
from forze_mock.adapters.storage import MockStorageAdapter
from forze_mock.adapters.stream import MockStreamAdapter, MockStreamGroupAdapter
from forze_mock.adapters.tx import MockTxManagerAdapter
from forze_mock.state import MockState

__all__ = [
    "MockState",
    "MockDocumentAdapter",
    "MockSearchAdapter",
    "MockSearchCommandAdapter",
    "MockSearchResultSnapshotAdapter",
    "MockHubSearchAdapter",
    "MockFederatedSearchAdapter",
    "MockCounterAdapter",
    "MockCacheAdapter",
    "MockIdempotencyAdapter",
    "MockStorageAdapter",
    "MockTxManagerAdapter",
    "MockQueueAdapter",
    "MockPubSubAdapter",
    "MockStreamAdapter",
    "MockStreamGroupAdapter",
    "MockAnalyticsAdapter",
    "MockDistributedLockAdapter",
    "MockDurableWorkflowCommandAdapter",
    "MockDurableWorkflowQueryAdapter",
    "MockDurableWorkflowScheduleCommandAdapter",
    "MockDurableWorkflowScheduleQueryAdapter",
    "MockDurableFunctionEventAdapter",
    "MockDurableFunctionStepAdapter",
]
