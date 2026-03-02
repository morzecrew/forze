"""Workflow contracts for long-running orchestration engines.

Provides :class:`WorkflowPort` for starting and signalling workflows
(e.g. Temporal, Inngest).
"""

from .ports import WorkflowPort

# ----------------------- #

__all__ = ["WorkflowPort"]
