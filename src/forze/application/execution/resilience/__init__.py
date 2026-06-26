"""Built-in in-process resilience executor and registration module.

The store *contracts* (protocols, keys, ``Transition``) live in
:mod:`forze.application.contracts.resilience` — import them from there. This package
ships only the in-process default implementations.
"""

from .executor import InProcessResilienceExecutor
from .module import ResilienceDepsModule
from .occ import OCC_POLICY, occ_retry
from .policies import builtin_default_policies
from .read_retry import DEFAULT_READ_RETRY_EXC, retry_read
from .resolve import default_resilience_executor, resolve_resilience_executor
from .store import (
    InMemoryCircuitBreakerStore,
    InMemoryLatencyDigestStore,
    InMemoryRateLimitStore,
)

# ----------------------- #

__all__ = [
    "InMemoryCircuitBreakerStore",
    "InMemoryLatencyDigestStore",
    "InMemoryRateLimitStore",
    "InProcessResilienceExecutor",
    "OCC_POLICY",
    "ResilienceDepsModule",
    "builtin_default_policies",
    "DEFAULT_READ_RETRY_EXC",
    "default_resilience_executor",
    "occ_retry",
    "resolve_resilience_executor",
    "retry_read",
]
