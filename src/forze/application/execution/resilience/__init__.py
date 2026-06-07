"""Built-in in-process resilience executor and registration module."""

from .executor import InProcessResilienceExecutor
from .module import ResilienceDepsModule
from .occ import OCC_POLICY, occ_retry
from .policies import builtin_default_policies
from .resolve import default_resilience_executor, resolve_resilience_executor
from .state import Transition
from .store import BreakerKey, CircuitBreakerStore, InMemoryCircuitBreakerStore

# ----------------------- #

__all__ = [
    "BreakerKey",
    "CircuitBreakerStore",
    "InMemoryCircuitBreakerStore",
    "Transition",
    "InProcessResilienceExecutor",
    "OCC_POLICY",
    "ResilienceDepsModule",
    "builtin_default_policies",
    "default_resilience_executor",
    "occ_retry",
    "resolve_resilience_executor",
]
