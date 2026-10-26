"""Projecting frozen operations into an agent's tool palette.

The palette is a **capability grant**, so the allowlist is required and there is no
"project everything" convenience to reach for: every tool is something the model can
invoke, and the dangerous default is simply not spellable. Curation happens at wiring,
where it can be reviewed.

Nothing here derives a schema of its own. The operation's descriptor already carries the
input DTO — it exists precisely so a driving adapter can build a tool catalog *without*
re-deriving schemas — so the tool an agent sees and the operation it invokes cannot drift
apart: they read the same type.
"""

import re
from collections.abc import Callable, Iterable, Mapping

from forze.application.contracts.querying import describe_query_discovery
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey

from .value_objects import OperationToolset, ToolDef

# ----------------------- #

_TOOL_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")
"""A tool name OpenAI-style function calling and Anthropic's Messages API both accept."""

# ....................... #


def provider_safe_tool_name(op: str) -> str:
    """The default naming policy: *op* with ``.`` spelled ``__`` and any other character a
    provider refuses spelled ``_``, so ``orders.list`` becomes ``orders__list``.

    It need not be reversible: dispatch resolves a name through the toolset's bindings,
    never by undoing the spelling.

    :param op: The operation key.
    :returns: The tool name.
    """

    return re.sub(r"[^A-Za-z0-9_-]", "_", op.replace(".", "__"))


# ....................... #


def _refuse_unsafe_names(claimed: Mapping[str, list[str]]) -> None:
    """Refuse a tool name a provider would reject, and one that two operations share.

    Found here, at wiring, rather than by the provider refusing the whole palette on the
    first request — or, for a shared name, by one operation silently shadowing another.
    """

    invalid = {name: ops for name, ops in claimed.items() if not _TOOL_NAME.fullmatch(name)}

    if invalid:
        raise exc.configuration(
            "Tool name(s) a provider would refuse (each must match ^[A-Za-z0-9_-]{1,64}$): "
            + ", ".join(f"{name!r} ({', '.join(ops)})" for name, ops in invalid.items()),
            code="agent_tools_invalid_tool_name",
            details={"names": invalid},
        )

    collisions = {name: ops for name, ops in claimed.items() if len(ops) > 1}

    if collisions:
        raise exc.configuration(
            "Operations share a tool name: "
            + "; ".join(f"{name!r} ({', '.join(ops)})" for name, ops in collisions.items()),
            code="agent_tools_tool_name_collision",
            details={"names": collisions},
        )


# ....................... #


def _input_schema(entry: OperationCatalogEntry) -> JsonDict:
    """JSON Schema of *entry*'s input DTO, or ``{}`` when the operation takes no input."""

    descriptor = entry.descriptor

    if descriptor is None or descriptor.input_type is None:
        return {}

    return descriptor.input_type.model_json_schema()


# ....................... #


def _description(entry: OperationCatalogEntry) -> str | None:
    """*entry*'s own description, plus its filter surface when it accepts one.

    A filter-accepting operation's input schema says a filter may be sent; it does not say
    which fields and operators the read model allows, because the DSL's expression types
    are field-agnostic by design. Without that, a model either guesses a field name or is
    told about it one refusal at a time. The descriptor already carries the discovery the
    generated HTTP and MCP surfaces advertise, so the palette advertises the same thing,
    through the same sentence they use.

    The governance facts MCP also states — required permissions, the authentication
    requirement, the deadline — are deliberately not here: a permission key is not
    something a model can act on, and a denial already reaches it as an error result
    carrying the code.
    """

    descriptor = entry.descriptor

    if descriptor is None:
        return None

    discovery = descriptor.query_discovery
    sentence = describe_query_discovery(discovery) if discovery is not None else None

    # Joined by what is actually there: a descriptor carrying no description, or a
    # discovery whose allow-sets are all empty, must not leave a stray space behind or
    # turn an absent description into one.
    return " ".join(part for part in (descriptor.description, sentence) if part) or None


# ....................... #


def operation_tools(
    registry: FrozenOperationRegistry,
    *,
    include: Iterable[StrKey],
    read_only: bool = True,
    name: Callable[[str], str] = provider_safe_tool_name,
) -> OperationToolset:
    """Project the allowlisted operations of *registry* into a tool palette.

    :param registry: The frozen registry whose operations back the tools.
    :param include: The operations to project, by key — **required**. A toolset is reviewed
        like a permission grant, so there is no way to ask for the whole registry.
    :param read_only: Project only ``QUERY``-plane operations (the default). A read-only
        toolset cannot mutate because the write operations are *absent from the palette*,
        not merely denied at dispatch — an agent's tools are its capabilities.
    :param name: The naming policy, operation key → tool name. The default,
        :func:`provider_safe_tool_name`, spells ``orders.list`` as ``orders__list``.
    :returns: The palette: :attr:`~.value_objects.OperationToolset.defs` for the agent's
        SDK, and the bindings dispatch resolves names against.
    :raises CoreException: When *include* is empty, names an operation the registry does
        not have, names a command operation in a read-only toolset, or names an operation
        that projects a sensitive read model; or, as a ``configuration`` error, when a
        tool name falls outside ``^[A-Za-z0-9_-]{1,64}$`` or two operations share one.
    """

    catalog = registry.catalog()
    selected = tuple(include)

    if not selected:
        raise exc.validation(
            "An agent toolset must name at least one operation",
            code="agent_tools_empty_allowlist",
        )

    defs: list[ToolDef] = []
    bindings: dict[str, OperationCatalogEntry] = {}
    claimed: dict[str, list[str]] = {}

    for op in selected:
        # A name may legitimately be listed twice when a palette is assembled from
        # concatenated slices (the query tools plus one command). The palette keeps the
        # first position and drops the repeat: two identical defs would be rejected by
        # the SDKs downstream, and refusing would make composition awkward for a mistake
        # that is never ambiguous.
        if any(str(op) in ops for ops in claimed.values()):
            continue

        entry = catalog.get(op)

        if entry is None:
            raise exc.validation(
                f"Unknown operation in the agent toolset: {op!r}",
                code="agent_tools_unknown_operation",
                details={"op": str(op)},
            )

        if read_only and not entry.is_read_only:
            raise exc.validation(
                f"{op!r} is a command operation and cannot be projected into a read-only "
                "toolset; build a command-capable toolset for an agent that must act",
                code="agent_tools_command_in_read_only",
                details={"op": str(op)},
            )

        # Refused rather than dropped, matching the generated HTTP and MCP surfaces: an
        # explicit allowlist naming a sensitive operation is an authoring mistake, and
        # silently omitting it would leave the app believing it granted a capability it
        # did not.
        if entry.descriptor is not None and entry.descriptor.sensitive:
            raise exc.validation(
                f"{op!r} projects a sensitive read model (its spec is marked "
                "sensitive=True; credential/secret material must not reach an agent) "
                "and cannot be projected as a tool",
                code="agent_tools_sensitive_operation",
                details={"op": str(op)},
            )

        tool_name = name(str(entry.op))
        claimed.setdefault(tool_name, []).append(str(entry.op))

        defs.append(
            ToolDef(
                name=tool_name,
                description=_description(entry),
                input_schema=_input_schema(entry),
            )
        )
        bindings[tool_name] = entry

    _refuse_unsafe_names(claimed)

    return OperationToolset(registry=registry, defs=tuple(defs), bindings=bindings)
