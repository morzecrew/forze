"""Self-hosted durable execution: run store runner, registry, and durable saga executor.

Provider-agnostic: drives durable functions and sagas over the framework's
``DurableFunctionStepPort`` + ``DurableRunStorePort`` contracts (Postgres self-hosted, or
the mock under tests / simulation).
"""

from ._resolve import (
    resolve_durable_run_store,
    resolve_durable_schedule_store,
    resolve_durable_step,
)
from .lifecycle import (
    durable_recovery_background_lifecycle_step,
    durable_scheduler_background_lifecycle_step,
)
from .registry import DurableFunctionHandler, DurableFunctionRegistry
from .runner import DurableFunctionRunner
from .saga_executor import DurableSagaExecutor, durable_saga_handler
from .scheduler import DurableScheduler
from .telemetry import (
    DURABLE_RECOVERED_COUNTER,
    DURABLE_RUN_DURATION_HISTOGRAM,
    DURABLE_RUNS_COUNTER,
    DURABLE_SCHEDULE_FIRES_COUNTER,
    DurableTelemetry,
)

# ----------------------- #

__all__ = [
    "DURABLE_RECOVERED_COUNTER",
    "DURABLE_RUNS_COUNTER",
    "DURABLE_RUN_DURATION_HISTOGRAM",
    "DURABLE_SCHEDULE_FIRES_COUNTER",
    "DurableFunctionHandler",
    "DurableFunctionRegistry",
    "DurableFunctionRunner",
    "DurableSagaExecutor",
    "DurableScheduler",
    "DurableTelemetry",
    "durable_recovery_background_lifecycle_step",
    "durable_saga_handler",
    "durable_scheduler_background_lifecycle_step",
    "resolve_durable_run_store",
    "resolve_durable_schedule_store",
    "resolve_durable_step",
]
