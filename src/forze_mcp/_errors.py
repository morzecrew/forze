"""Boundary error translation shared by the generated MCP surfaces.

Tool calls and resource-template reads dispatch through the same governed pipeline
(:mod:`forze_mcp.dispatch`), so both translate a boundary :class:`CoreException` the same
way: project it through the egress-masked ``error_envelope`` (the envelope the HTTP edge
renders — a caller-caused kind keeps its message + code, internal/infrastructure detail
is masked) and raise the FastMCP error type native to the surface (``ToolError`` for
tools, ``ResourceError`` for resources), which FastMCP passes through to the client.
"""

import json

from fastmcp.exceptions import FastMCPError

from forze.base.exceptions import CoreException, error_envelope
from forze.base.logging import Logger

from ._logging import ForzeMCPLogger

# ----------------------- #

_error_logger = Logger(ForzeMCPLogger.ERRORS)
"""Server-side error diagnostics (mirrors the HTTP edge's error logging)."""

# ....................... #


def client_safe_error[E: FastMCPError](e: CoreException, error_type: type[E]) -> E:
    """Project *e* through the egress-masked envelope into a client-safe *error_type*.

    ``error_envelope`` applies the per-kind egress policy — a caller-caused kind keeps its
    message, an internal/server error is masked to a generic detail — so the agent gets an
    actionable error without any internal specifics. A server-side error is logged (with
    its cause's traceback when chained) before the masked error reaches the agent, so
    operators still see the real failure instead of only a generic client-facing message.
    The caller raises the returned error ``from e``.
    """

    envelope = error_envelope(e)

    if envelope.server_error:
        if e.__cause__ is not None:
            _error_logger.critical_exception(
                "MCP server error",
                exc=e.__cause__,
                error_code=e.code,
                error_kind=e.kind.value,
            )
        else:
            _error_logger.error(
                "MCP server error",
                error_code=e.code,
                error_kind=e.kind.value,
                detail=e.summary,
            )

    rendered = f"{envelope.code}: {envelope.detail}"

    # Append the envelope's context when the kind's egress policy exposed one. It is
    # already sanitized (raw values stripped, app-authored messages replaced), and it
    # carries the part an agent can act on: *which* argument failed and which rule it
    # broke. Without it a rejected tool call says only that something was invalid, and the
    # agent has nothing to correct on a retry.
    if envelope.context:
        rendered = f"{rendered} {json.dumps(envelope.context, default=str, sort_keys=True)}"

    return error_type(rendered)
