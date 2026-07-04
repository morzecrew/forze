"""Port interception seam — a public, composable middleware chain around port calls.

Production registers no interceptors (the resolved port is returned bare — zero cost).
Simulation registers them — deps-scoped via
:meth:`~forze.application.execution.deps.registry.DepsRegistry.with_interceptors` or
run-scoped via :func:`bind_interceptors` — for cooperative yielding, I/O latency, and
(via the DST layer) fault injection, all at the seam rather than in application handlers.
"""

from .builtin import CooperativeInterceptor, LatencyModel
from .logging import LoggingInterceptor
from .protocol import (
    PortCall,
    PortInterceptor,
    PortInterceptorChain,
    PortNext,
    StreamPortInterceptor,
    StreamPortNext,
    bind_interceptors,
    current_interceptors,
)
from .proxy import (
    InterceptingPortProxy,
    compose_stream_chain,
    run_chain,
    wrap_intercepted,
)

# ----------------------- #

__all__ = [
    "CooperativeInterceptor",
    "InterceptingPortProxy",
    "LatencyModel",
    "LoggingInterceptor",
    "PortCall",
    "PortInterceptor",
    "PortInterceptorChain",
    "PortNext",
    "StreamPortInterceptor",
    "StreamPortNext",
    "bind_interceptors",
    "compose_stream_chain",
    "current_interceptors",
    "run_chain",
    "wrap_intercepted",
]
