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

from forze.application.contracts.querying import (
    QueryFieldGuard,
    QueryFieldPolicy,
    describe_query_discovery,
    filter_grammar,
    narrow_query_discovery,
    takes_filter_expression,
    without_filter_grammar,
)
from forze.application.execution.ingress import query_policy_guards
from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey, StrKeyMapping

from .value_objects import OperationToolset, ToolDef

# ----------------------- #

_TOOL_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")
"""A tool name OpenAI-style function calling and Anthropic's Messages API both accept."""

_GRAMMAR_POINTER = "Its grammar is in the system prompt."
"""What a filter expression stripped of the grammar says in its place."""

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


def _checked_names(names: Mapping[str, object]) -> dict[str, str]:
    """*names* (operation key → what the naming policy returned), once each is a tool name
    a provider accepts and no two operations reach the same one.

    Found here, at wiring, rather than by the provider refusing the whole palette on the
    first request — or, for a shared name, by one operation silently shadowing another.
    """

    invalid = {
        op: name
        for op, name in names.items()
        if not isinstance(name, str) or not _TOOL_NAME.fullmatch(name)
    }

    if invalid:
        raise exc.configuration(
            "Tool name(s) a provider would refuse (each must match ^[A-Za-z0-9_-]{1,64}$): "
            + ", ".join(f"{name!r} ({op})" for op, name in invalid.items()),
            code="agent_tools_invalid_tool_name",
            details={"ops": list(invalid)},
        )

    checked = {op: name for op, name in names.items() if isinstance(name, str)}
    reached: dict[str, set[str]] = {}

    for op, name in checked.items():
        reached.setdefault(name, set()).add(op)

    # An operation's key still dispatches as a legacy name, so a tool name equal to another
    # operation's key reaches that operation too.
    for name, ops in reached.items():
        if name in checked:
            ops.add(name)

    collisions = {name: sorted(ops) for name, ops in reached.items() if len(ops) > 1}

    if collisions:
        raise exc.configuration(
            "Operations share a tool name: "
            + "; ".join(f"{name!r} ({', '.join(ops)})" for name, ops in collisions.items()),
            code="agent_tools_tool_name_collision",
            details={"names": collisions},
        )

    return checked


# ....................... #


def _grammar_note() -> str:
    """The filter grammar as a palette states it once, worded for a system prompt."""

    return (
        "Every filter expression a tool takes (each `filters` argument and `$having` clause) "
        f"follows this JSON schema:\n{filter_grammar()}"
    )


# ....................... #


def _input_schema(entry: OperationCatalogEntry) -> JsonDict:
    """JSON Schema of *entry*'s input DTO, or ``{}`` when the operation takes no input."""

    descriptor = entry.descriptor

    if descriptor is None or descriptor.input_type is None:
        return {}

    return descriptor.input_type.model_json_schema()


# ....................... #


def _description(
    entry: OperationCatalogEntry,
    guard: QueryFieldGuard | None = None,
) -> str | None:
    """*entry*'s own description, plus its filter surface when it accepts one — narrowed to
    *guard*'s policy when the palette narrows it.

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

    if discovery is not None and guard is not None:
        discovery = narrow_query_discovery(discovery, guard.policy)

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
    shared_filter_grammar: bool = False,
    query_policies: StrKeyMapping[QueryFieldPolicy] | None = None,
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
    :param shared_filter_grammar: State the filter grammar once instead of in every
        filter-accepting tool: each ``filters`` (and aggregate ``$having``) becomes a plain
        object in the tool's schema, as on MCP, and the grammar is
        :attr:`~.value_objects.OperationToolset.filter_grammar`, for the system prompt.
        Dispatch validates a call against the full grammar either way.
    :param query_policies: Operation key → a narrower policy for that tool: its description
        names only the fields the policy allows, and dispatch refuses a call that filters,
        sorts or aggregates on any other, so the palette accepts what it advertises. Each
        allow-set must be a subset of the operation's own; an axis left ``None`` keeps the
        operation's. The operation itself, served elsewhere, keeps its full allow-set.
    :returns: The palette: :attr:`~.value_objects.OperationToolset.defs` for the agent's
        SDK, and the bindings dispatch resolves names against.
    :raises CoreException: When *include* is empty, names an operation the registry does
        not have, names a command operation in a read-only toolset, or names an operation
        that projects a sensitive read model or returns a byte stream; or, as a
        ``configuration`` error, when a
        tool name is not a string matching ``^[A-Za-z0-9_-]{1,64}$``, or two operations
        share one — an operation's own key counting as one of its names; or, as a
        ``configuration`` error, when *query_policies* names an operation outside
        *include* or one that takes no filter, widens one of its allow-sets, or narrows an
        axis its input does not carry.
    """

    catalog = registry.catalog()
    selected = tuple(include)

    if not selected:
        raise exc.validation(
            "An agent toolset must name at least one operation",
            code="agent_tools_empty_allowlist",
        )

    entries: dict[str, OperationCatalogEntry] = {}

    for op in selected:
        # A name may legitimately be listed twice when a palette is assembled from
        # concatenated slices (the query tools plus one command). The palette keeps the
        # first position and drops the repeat: two identical defs would be rejected by
        # the SDKs downstream, and refusing would make composition awkward for a mistake
        # that is never ambiguous.
        if str(op) in entries:
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
                f"{op!r} is sensitive (its result carries credential or secret "
                "material, which must not reach an agent) and cannot be projected as a "
                "tool; leave it out of include",
                code="agent_tools_sensitive_operation",
                details={"op": str(op)},
            )

        # A tool result is JSON for the model to read; a chunk iterator would reach it as
        # an object repr and the bytes never would.
        if entry.descriptor is not None and entry.descriptor.streams:
            raise exc.validation(
                f"{op!r} returns a byte stream, which a tool result cannot carry, and "
                "cannot be projected as a tool",
                code="agent_tools_streaming_operation",
                details={"op": str(op)},
            )

        entries[str(op)] = entry

    # Widened to object: the policy is the app's code, and a non-str return must reach the
    # configuration error rather than fail on hashing.
    policy_names: dict[str, object] = {op: name(op) for op in entries}
    names = _checked_names(policy_names)
    guards = query_policy_guards(catalog, entries, query_policies, surface="agent tool")
    schemas = {op: _input_schema(entry) for op, entry in entries.items()}
    # Only a tool that takes a filter changes; the grammar is stated when one does.
    filtered = [
        op
        for op, schema in schemas.items()
        if shared_filter_grammar and takes_filter_expression(schema)
    ]

    for op in filtered:
        schemas[op] = without_filter_grammar(schemas[op], pointer=_GRAMMAR_POINTER)

    defs = tuple(
        ToolDef(
            name=names[op],
            description=_description(entry, guards.get(op)),
            input_schema=schemas[op],
        )
        for op, entry in entries.items()
    )
    bindings = {names[op]: entry for op, entry in entries.items()}

    return OperationToolset(
        registry=registry,
        defs=defs,
        bindings=bindings,
        filter_grammar=_grammar_note() if filtered else None,
        query_guards=guards,
    )
