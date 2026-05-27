from typing import Any

from ..deps import ConfigurableDepPort, DepKey
from .ports import WorkflowCommandPort, WorkflowQueryPort
from .schedule_ports import WorkflowScheduleCommandPort, WorkflowScheduleQueryPort
from .specs import WorkflowSpec

# ----------------------- #

WfSpec = WorkflowSpec[Any, Any]
"""Type-erased workflow specification."""

WfCommandPort = WorkflowCommandPort[Any, Any]
"""Type-erased workflow command port."""

WfQueryPort = WorkflowQueryPort[Any, Any]
"""Type-erased workflow query port."""

# ....................... #


WorkflowCommandDepPort = ConfigurableDepPort[WfSpec, WfCommandPort]
"""Workflow command dependency port."""

WorkflowQueryDepPort = ConfigurableDepPort[WfSpec, WfQueryPort]
"""Workflow query dependency port."""

# ....................... #

WorkflowCommandDepKey = DepKey[WorkflowCommandDepPort]("workflow_command")
"""Key used to register the :class:`WorkflowCommandDepPort` implementation."""

WorkflowQueryDepKey = DepKey[WorkflowQueryDepPort]("workflow_query")
"""Key used to register the :class:`WorkflowQueryDepPort` implementation."""

# ....................... #

WfScheduleCommandPort = WorkflowScheduleCommandPort[Any]
"""Type-erased workflow schedule command port."""

WfScheduleQueryPort = WorkflowScheduleQueryPort[Any]
"""Type-erased workflow schedule query port."""

# ....................... #

WorkflowScheduleCommandDepPort = ConfigurableDepPort[WfSpec, WfScheduleCommandPort]
"""Workflow schedule command dependency port."""

WorkflowScheduleQueryDepPort = ConfigurableDepPort[WfSpec, WfScheduleQueryPort]
"""Workflow schedule query dependency port."""

# ....................... #

WorkflowScheduleCommandDepKey = DepKey[WorkflowScheduleCommandDepPort](
    "workflow_schedule_command",
)
"""Key used to register the :class:`WorkflowScheduleCommandDepPort` implementation."""

WorkflowScheduleQueryDepKey = DepKey[WorkflowScheduleQueryDepPort](
    "workflow_schedule_query",
)
"""Key used to register the :class:`WorkflowScheduleQueryDepPort` implementation."""
