from typing import Any

from forze.application.contracts.deps import ConfigurableDepPort, DepKey, SimpleDepPort

from .ports import DurableFunctionEventCommandPort, DurableFunctionStepPort
from .run_store import DurableRunStorePort
from .schedule_store import DurableScheduleStorePort
from .specs import DurableFunctionEventSpec

# ----------------------- #

DurableFunctionEventCommandDepPort = ConfigurableDepPort[
    DurableFunctionEventSpec[Any],
    DurableFunctionEventCommandPort[Any],
]
"""Durable function event command dependency port."""

DurableFunctionStepDepPort = SimpleDepPort[DurableFunctionStepPort]
"""Durable function step dependency port (execution-scoped, not spec-routed)."""

DurableRunStoreDepPort = SimpleDepPort[DurableRunStorePort]
"""Durable run store dependency port (execution-scoped, not spec-routed)."""

DurableScheduleStoreDepPort = SimpleDepPort[DurableScheduleStorePort]
"""Durable schedule store dependency port (execution-scoped, not spec-routed)."""

# ....................... #

DurableFunctionEventCommandDepKey = DepKey[DurableFunctionEventCommandDepPort](
    "durable_function_event_command",
)
"""Key used to register the :class:`DurableFunctionEventCommandDepPort` implementation."""

DurableFunctionStepDepKey = DepKey[DurableFunctionStepDepPort](
    "durable_function_step",
)
"""Key used to register the :class:`DurableFunctionStepPort` implementation."""

DurableRunStoreDepKey = DepKey[DurableRunStoreDepPort](
    "durable_function_run_store",
)
"""Key used to register the :class:`DurableRunStorePort` implementation."""

DurableScheduleStoreDepKey = DepKey[DurableScheduleStoreDepPort](
    "durable_function_schedule_store",
)
"""Key used to register the :class:`DurableScheduleStorePort` implementation."""
