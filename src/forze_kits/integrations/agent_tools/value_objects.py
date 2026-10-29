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
from types import MappingProxyType
from typing import Any, final

import attrs

from forze.application.contracts.querying import QueryFieldGuard
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
)
from forze.base.primitives import JsonDict

# ----------------------- #


def _read_only(given: Mapping[str, Any]) -> Mapping[str, Any]:
    """Copy a mapping into a read-only view of its own."""

    return MappingProxyType(dict(given))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ToolDef:
    """One operation, projected as a tool an agent may be offered."""

    name: str
    """Tool name: the operation key under the palette's naming policy (``orders.list`` is
    ``orders__list`` by default), so it is stable across restarts and legible to a model."""

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

    bindings: Mapping[str, OperationCatalogEntry] = attrs.field(
        factory=dict[str, OperationCatalogEntry],
        converter=_read_only,
    )
    """Tool name → the catalog entry it dispatches to. Keyed by the *name* the agent sends —
    the operation key under the naming policy — so dispatch never has to undo the policy;
    ``entry.op`` is the key it came from.

    Copied into a read-only view at construction, so the palette a reviewer approved is the
    palette that dispatches: the mapping cannot be written through afterwards, and it does
    not alias a dict the caller still holds. This is the value object keeping its own
    promise, not an authorization boundary — in-process code that could edit it could call
    ``run_operation`` directly."""

    filter_grammar: str | None = None
    """The filter grammar, stated once for the system prompt, when the palette was built
    with ``shared_filter_grammar=True`` and a tool in it takes a filter; ``None`` otherwise,
    when each filter-accepting def carries the grammar itself."""

    query_guards: Mapping[str, QueryFieldGuard] = attrs.field(
        factory=dict[str, QueryFieldGuard],
        converter=_read_only,
    )
    """Operation key → the guard enforcing the narrower query policy the palette was built
    with (``query_policies``). Dispatch applies it to every call of that operation, so a
    tool refuses a field its description does not name. Read-only, like :attr:`bindings`."""

    # ....................... #

    @property
    def names(self) -> tuple[str, ...]:
        """Tool names in this palette."""

        return tuple(definition.name for definition in self.defs)

    # ....................... #

    def entry_for(self, name: str) -> OperationCatalogEntry | None:
        """The catalog entry bound to *name*, or ``None`` when it is not in this palette.

        A palette operation's own key resolves too: tools were named by their key before the
        naming policy, and a run or history that journaled one still replays it.
        :func:`~forze_kits.integrations.agent_tools.operation_tools` refuses a tool name equal
        to another operation's key, so the two lookups never disagree.
        """

        entry = self.bindings.get(name)

        if entry is None:
            entry = next((e for e in self.bindings.values() if str(e.op) == name), None)

        return entry
