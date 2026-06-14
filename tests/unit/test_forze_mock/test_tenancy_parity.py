"""Meta-test: every tenant-relevant mock adapter must opt into tenant partitioning.

This guards the bug class where a mock adapter (graph, durable) silently shared one store
across tenants — so unit tests could not catch cross-tenant leaks. Each adapter that holds
per-tenant state must mix in :class:`MockTenancyMixin` (which partitions its namespace and
fails closed when ``tenant_aware`` without a bound tenant). Adapters that are tenancy-N/A
by design must NOT, so the classification stays explicit.
"""

from __future__ import annotations

from forze_mock.adapters import (
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
    MockGraphAdapter,
    MockHubSearchAdapter,
    MockIdempotencyAdapter,
    MockInboxAdapter,
    MockPubSubAdapter,
    MockQueueAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
    MockStorageAdapter,
    MockStreamAdapter,
    MockStrictTxManagerAdapter,
    MockTxManagerAdapter,
)
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #

# Adapters that hold per-tenant state — must partition by tenant.
_TENANT_PARTITIONED = [
    MockDocumentAdapter,
    MockSearchAdapter,
    MockSearchCommandAdapter,
    MockSearchResultSnapshotAdapter,
    MockGraphAdapter,
    MockCacheAdapter,
    MockCounterAdapter,
    MockIdempotencyAdapter,
    MockInboxAdapter,
    MockStorageAdapter,
    MockQueueAdapter,
    MockPubSubAdapter,
    MockStreamAdapter,
    MockDistributedLockAdapter,
    MockAnalyticsAdapter,
    MockDurableWorkflowCommandAdapter,
    MockDurableWorkflowQueryAdapter,
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
    MockDurableFunctionEventAdapter,
]

# Tenancy-N/A by design: transaction managers (no stored state); hub/federated search
# (compose tenant-aware legs); function-step memo (keyed by a unique run id).
_TENANCY_NA = [
    MockTxManagerAdapter,
    MockStrictTxManagerAdapter,
    MockHubSearchAdapter,
    MockFederatedSearchAdapter,
    MockDurableFunctionStepAdapter,
]


def test_stateful_mock_adapters_partition_by_tenant() -> None:
    for adapter in _TENANT_PARTITIONED:
        assert issubclass(adapter, MockTenancyMixin), (
            f"{adapter.__name__} holds per-tenant state but does not mix in "
            "MockTenancyMixin — its store would be shared across tenants (a cross-tenant "
            "leak in the test double). Partition it like the other mocks."
        )


def test_tenancy_na_mock_adapters_do_not_partition() -> None:
    for adapter in _TENANCY_NA:
        assert not issubclass(adapter, MockTenancyMixin), (
            f"{adapter.__name__} is classified tenancy-N/A but mixes in MockTenancyMixin; "
            "reclassify it (move to the partitioned list) or drop the mixin."
        )
