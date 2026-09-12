"""Forze operations as governed agent tools.

An in-process agent loop's tools *are* operations: project the ones an app allowlists into
tool definitions, hand those to whichever SDK owns the conversation, and dispatch the
model's tool calls back through ``run_operation`` on the live context. Tenancy,
permissions, deadlines, resilience and audit apply to every tool call, unchanged, because
a tool call is an ordinary governed invocation rather than a side door.

The loop stays the application's. This bridge turns operations into tools and dispatches
them one at a time; it is not an orchestrator, not a tool runtime, and not a chat surface.

.. code-block:: python

    tools = operation_tools(registry, include=["catalog.list", "catalog.search"])
    # ... the app's SDK chooses a tool and returns its native tool-use block ...
    result = await dispatch_tool_use(
        ToolUse(id=block.id, name=block.name, input=block.input), ctx=ctx, tools=tools
    )
"""

from .dispatch import dispatch_tool_use
from .projection import operation_tools
from .value_objects import OperationToolset, ToolDef, ToolResult, ToolUse

# ----------------------- #

__all__ = [
    "OperationToolset",
    "ToolDef",
    "ToolResult",
    "ToolUse",
    "dispatch_tool_use",
    "operation_tools",
]
