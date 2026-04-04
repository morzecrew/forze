from typing import Any

from ..base import BaseDepPort, DepKey
from .ports import WorkflowCommandPort, WorkflowQueryPort
from .specs import WorkflowSpec

# ----------------------- #

WfSpec = WorkflowSpec[Any, Any]
"""Type-erased workflow specification."""

WfCommandPort = WorkflowCommandPort[Any, Any]
"""Type-erased workflow command port."""

WfQueryPort = WorkflowQueryPort[Any, Any]
"""Type-erased workflow query port."""

# ....................... #


WorkflowCommandDepPort = BaseDepPort[WfSpec, WfCommandPort]
"""Workflow command dependency port."""

WorkflowQueryDepPort = BaseDepPort[WfSpec, WfQueryPort]
"""Workflow query dependency port."""

# ....................... #

WorkflowCommandDepKey = DepKey[WorkflowCommandDepPort]("workflow_command")
"""Key used to register the :class:`WorkflowCommandDepPort` implementation."""

WorkflowQueryDepKey = DepKey[WorkflowQueryDepPort]("workflow_query")
"""Key used to register the :class:`WorkflowQueryDepPort` implementation."""
