"""Built-in in-process resilience executor and registration module."""

# The store seams (protocols, keys, ``Transition``) are contracts; this package
# re-exports them next to the in-process default impls it ships, for back-compat.
from forze.application.contracts.resilience import (
    BreakerKey,
    CircuitBreakerStore,
    LatencyDigestKey,
    LatencyDigestStore,
    RateLimitKey,
    RateLimitStore,
    Transition,
)

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
    "BreakerKey",
    "CircuitBreakerStore",
    "InMemoryCircuitBreakerStore",
    "InMemoryLatencyDigestStore",
    "InMemoryRateLimitStore",
    "LatencyDigestKey",
    "LatencyDigestStore",
    "RateLimitKey",
    "RateLimitStore",
    "Transition",
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
