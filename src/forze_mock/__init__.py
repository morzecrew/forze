"""In-memory mock integration for Forze contracts.

`forze_mock` provides dependency wiring and adapters for running Forze without
external infrastructure. It is intended for development and tests.
"""

from .adapters import (
    MockAnalyticsAdapter,
    MockCacheAdapter,
    MockCounterAdapter,
    MockDistributedLockAdapter,
    MockDocumentAdapter,
    MockDurableFunctionEventAdapter,
    MockDurableFunctionStepAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
    MockFederatedSearchAdapter,
    MockHubSearchAdapter,
    MockIdempotencyAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
    MockState,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStreamGroupAdapter,
    MockTxManagerAdapter,
)
from .adapters.identity import (
    MockSecretsPort,
    MockTenantManagementPort,
    MockTenantResolverPort,
)
from .embeddings import MockHashEmbeddingsProvider
from .execution import (
    MockDepsModule,
    MockRouteConfig,
    MockRoutedStateDepKey,
    MockStateDepKey,
)
from .tenancy import (
    MockRoutedStateRegistry,
    MockTenancyMixin,
    mock_routed_state_lifecycle_step,
    partition_namespace,
    resolve_mock_namespace,
)

# ----------------------- #

__all__ = [
    "MockState",
    "MockStateDepKey",
    "MockRoutedStateDepKey",
    "MockRouteConfig",
    "MockDepsModule",
    "MockRoutedStateRegistry",
    "mock_routed_state_lifecycle_step",
    "MockTenancyMixin",
    "partition_namespace",
    "resolve_mock_namespace",
    "MockDocumentAdapter",
    "MockAnalyticsAdapter",
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
    "MockDistributedLockAdapter",
    "MockDurableWorkflowCommandAdapter",
    "MockDurableWorkflowQueryAdapter",
    "MockDurableWorkflowScheduleCommandAdapter",
    "MockDurableWorkflowScheduleQueryAdapter",
    "MockDurableFunctionEventAdapter",
    "MockDurableFunctionStepAdapter",
    "MockSecretsPort",
    "MockTenantResolverPort",
    "MockTenantManagementPort",
    "MockHashEmbeddingsProvider",
]
