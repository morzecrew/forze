"""Running one agent tool call as a governed operation invocation.

Dispatch goes through the front door: :func:`run_operation` on the caller's live execution
context, so tenancy, permission keys, ownership checks, the deadline, resilience,
interceptors and audit all apply exactly as they do to an HTTP or MCP call of the same
operation. This module enforces nothing of its own — an agent that cannot reach an
operation is an agent whose bound principal cannot reach it.

Two failure kinds are kept apart on purpose. A **governed** failure (validation,
authorization, a precondition) is something the model can act on, so it comes back as a
``ToolResult`` carrying the egress-masked code and the agent takes another turn. An
**infrastructure** failure is not the agent's to correct, so it propagates to the loop and
the application decides whether the turn is retried — the same split
:func:`run_operation` already draws.
"""

from typing import Any

from pydantic import BaseModel, ValidationError

from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations import (
    OperationCatalogEntry,
    run_operation,
)
from forze.base.exceptions import CoreException, error_envelope, exc
from forze.base.primitives import JsonDict
from forze.base.scrubbing import sanitize_pydantic_errors

from .value_objects import OperationToolset, ToolResult, ToolUse

# ----------------------- #


def _error_result(tool_use: ToolUse, error: CoreException) -> ToolResult:
    """Project a governed failure into a caller-safe :class:`ToolResult`.

    The envelope applies the per-kind egress policy — a caller-caused kind keeps its
    message and code, anything internal is masked to a generic detail — so the agent is
    told what it can fix and nothing about the inside of the process. The sanitized
    context rides along when the policy exposed one, because *which* argument failed is
    the part a retry depends on.
    """

    envelope = error_envelope(error)
    content: JsonDict = {"code": envelope.code, "detail": envelope.detail}

    if envelope.context:
        content["context"] = envelope.context

    return ToolResult(tool_use_id=tool_use.id, content=content, is_error=True)


# ....................... #


def _validated_args(entry: OperationCatalogEntry, raw: JsonDict) -> Any:
    """Validate the agent's arguments into the operation's input DTO (``None`` if it has no input).

    Validation happens **before** dispatch, so a malformed argument never reaches a
    half-executed call. Pydantic's own error text embeds the offending value — echoing
    back whatever the model produced, including a secret it put in the wrong field — so
    the errors are sanitized here rather than passed through.
    """

    descriptor = entry.descriptor

    if descriptor is None or descriptor.input_type is None:
        return None

    try:
        return descriptor.input_type.model_validate(dict(raw))

    except ValidationError as error:
        raise exc.validation(
            "Invalid tool arguments",
            code="agent_tools_invalid_arguments",
            details={"errors": sanitize_pydantic_errors(error.errors())},
        ) from error


# ....................... #


def _content(result: Any) -> str | JsonDict:
    """Project an operation's result into tool-result content.

    An operation's output type is a Pydantic DTO or nothing, so the JSON projection is the
    DTO's own. A void operation still owes the agent a reply — an empty object, which says
    "it ran and returned nothing" without inventing a message the model might read as data.
    """

    if result is None:
        return {}

    if isinstance(result, BaseModel):
        return result.model_dump(mode="json")

    return str(result)


# ....................... #


async def dispatch_tool_use(
    tool_use: ToolUse,
    *,
    ctx: ExecutionContext,
    tools: OperationToolset,
) -> ToolResult:
    """Run *tool_use* as a governed operation invocation and answer with a :class:`ToolResult`.

    :param tool_use: What the agent asked to run, adapted from its SDK's tool-call type.
    :param ctx: The live execution context — already bound by whichever boundary is
        running the agent loop. Its principal and tenant are what the operation is
        governed against.
    :param tools: The palette. A name outside it is refused *here*, so an operation nobody
        allowlisted stays unreachable even though the registry holds it.
    :returns: The operation's result, or a caller-safe error the agent can act on.
    :raises Exception: Infrastructure failures propagate unchanged; they are the
        application loop's to handle, not the model's.
    """

    entry = tools.entry_for(tool_use.name)

    if entry is None:
        # Not found rather than "forbidden": the palette is the agent's whole world, and
        # telling a model which operations exist outside it is a disclosure with no
        # upside. The bound principal's own permissions are enforced a layer down anyway.
        return _error_result(
            tool_use,
            exc.validation(
                f"Unknown tool: {tool_use.name!r}",
                code="agent_tools_unknown_tool",
            ),
        )

    try:
        args = _validated_args(entry, tool_use.input)
        result = await run_operation(tools.registry, entry.op, args, ctx)

    except CoreException as error:
        return _error_result(tool_use, error)

    return ToolResult(tool_use_id=tool_use.id, content=_content(result))
