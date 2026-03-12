"""In-memory mock integration for Forze contracts.

`forze_mock` provides dependency wiring and adapters for running Forze without
external infrastructure. It is intended for development and tests.
"""

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
from .execution import MockDepsModule, MockStateDepKey

# ----------------------- #

__all__ = [
    "MockState",
    "MockStateDepKey",
    "MockDepsModule",
    "MockDocumentAdapter",
    "MockSearchAdapter",
    "MockCounterAdapter",
    "MockCacheAdapter",
    "MockIdempotencyAdapter",
    "MockStorageAdapter",
    "MockTxManagerAdapter",
    "MockQueueAdapter",
    "MockPubSubAdapter",
    "MockStreamAdapter",
    "MockStreamGroupAdapter",
]
