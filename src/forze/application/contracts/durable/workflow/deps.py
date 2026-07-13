from typing import Any

from forze.application.contracts.deps import ConfigurableDepPort, DepKey

from .ports import DurableWorkflowCommandPort, DurableWorkflowQueryPort
from .schedule_ports import (
    DurableWorkflowScheduleCommandPort,
    DurableWorkflowScheduleQueryPort,
)
from .specs import DurableWorkflowSpec

# ----------------------- #

WfSpec = DurableWorkflowSpec[Any, Any]
"""Type-erased workflow specification."""

WfCommandPort = DurableWorkflowCommandPort[Any, Any]
"""Type-erased workflow command port."""

WfQueryPort = DurableWorkflowQueryPort[Any, Any]
"""Type-erased workflow query port."""

# ....................... #


DurableWorkflowCommandDepPort = ConfigurableDepPort[WfSpec, WfCommandPort]
"""Workflow command dependency port."""

DurableWorkflowQueryDepPort = ConfigurableDepPort[WfSpec, WfQueryPort]
"""Workflow query dependency port."""

# ....................... #

DurableWorkflowCommandDepKey = DepKey[DurableWorkflowCommandDepPort]("durable_workflow_command")
"""Key used to register the :class:`DurableWorkflowCommandDepPort` implementation."""

DurableWorkflowQueryDepKey = DepKey[DurableWorkflowQueryDepPort]("durable_workflow_query")
"""Key used to register the :class:`DurableWorkflowQueryDepPort` implementation."""

# ....................... #

WfScheduleCommandPort = DurableWorkflowScheduleCommandPort[Any]
"""Type-erased workflow schedule command port."""

WfScheduleQueryPort = DurableWorkflowScheduleQueryPort[Any]
"""Type-erased workflow schedule query port."""

# ....................... #

DurableWorkflowScheduleCommandDepPort = ConfigurableDepPort[WfSpec, WfScheduleCommandPort]
"""Workflow schedule command dependency port."""

DurableWorkflowScheduleQueryDepPort = ConfigurableDepPort[WfSpec, WfScheduleQueryPort]
"""Workflow schedule query dependency port."""

# ....................... #

DurableWorkflowScheduleCommandDepKey = DepKey[DurableWorkflowScheduleCommandDepPort](
    "durable_workflow_schedule_command",
)
"""Key used to register the :class:`DurableWorkflowScheduleCommandDepPort` implementation."""

DurableWorkflowScheduleQueryDepKey = DepKey[DurableWorkflowScheduleQueryDepPort](
    "durable_workflow_schedule_query",
)
"""Key used to register the :class:`DurableWorkflowScheduleQueryDepPort` implementation."""
