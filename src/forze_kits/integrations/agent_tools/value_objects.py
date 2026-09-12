"""The tool-dispatch vocabulary: what an agent may run, what it asked for, what came back.

Deliberately **tool** vocabulary and not chat vocabulary. A def/use/result triple describes
"run this operation as a tool", which is this bridge's whole job; it says nothing about
messages, turns or conversations, so nothing here commits the framework to owning an agent
loop or a chat surface. The application adapts its own SDK's native tool types to and from
these three at the loop boundary — a few lines it owns — which is what keeps the bridge
provider-neutral and leaves the conversation with the SDK.

:class:`OperationToolset` is the projection's product: the defs an app hands its SDK, plus
the bindings back to the operations they name. The bindings are not a convenience. A tool
name is attacker-influenced input (the model chooses it), so dispatch resolves names
*against the toolset* rather than against the registry — an operation nobody allowlisted is
unreachable through this path even when it exists in the catalog.
"""

from collections.abc import Mapping
from typing import final

import attrs

from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
)
from forze.base.primitives import JsonDict

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ToolDef:
    """One operation, projected as a tool an agent may be offered."""

    name: str
    """Tool name: the operation key, so it is stable across restarts and legible to a model."""

    description: str | None = None
    """What the operation does, taken from its descriptor — not authored here."""

    input_schema: JsonDict = attrs.field(factory=dict)
    """JSON Schema of the operation's input DTO; empty for an input-less operation."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ToolUse:
    """What the agent asked to run, adapted from the SDK's native tool-call type."""

    id: str
    """The SDK's id for this call, echoed back on the result so the loop can pair them."""

    name: str
    """The tool the agent chose."""

    input: JsonDict = attrs.field(factory=dict)
    """Raw arguments as the model produced them — unvalidated by construction."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ToolResult:
    """The outcome of one tool call, in the shape an agent loop feeds back to the model."""

    tool_use_id: str
    """The :attr:`ToolUse.id` this answers."""

    content: str | JsonDict
    """The operation's result, JSON-projected — or a caller-safe error message."""

    is_error: bool = False
    """A governed failure the agent can act on. Infrastructure failures never arrive here:
    they propagate to the loop, which is the app's decision to retry or abandon a turn."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationToolset:
    """A projected palette: the tool defs, and the operations they are bound to.

    Built by :func:`forze_kits.integrations.agent_tools.operation_tools` and passed to
    :func:`forze_kits.integrations.agent_tools.dispatch_tool_use`. Reviewing one is
    reviewing a capability grant, which is what an agent's tool list is.
    """

    registry: FrozenOperationRegistry
    """The registry the palette was projected from, carried so dispatch cannot be handed a
    toolset and a *different* registry — a pairing that would resolve tool names against
    one catalog and run operations from another."""

    defs: tuple[ToolDef, ...] = attrs.field(factory=tuple)
    """The defs to hand the agent's SDK, in the order the allowlist named them."""

    bindings: Mapping[str, OperationCatalogEntry] = attrs.field(factory=dict)
    """Tool name → the catalog entry it dispatches to. Keyed by the *name* the agent sends,
    which is ``str(entry.op)`` — the same projection the MCP surface exposes."""

    # ....................... #

    @property
    def names(self) -> tuple[str, ...]:
        """Tool names in this palette."""

        return tuple(definition.name for definition in self.defs)

    # ....................... #

    def entry_for(self, name: str) -> OperationCatalogEntry | None:
        """The catalog entry bound to *name*, or ``None`` when it is not in this palette."""

        return self.bindings.get(name)
