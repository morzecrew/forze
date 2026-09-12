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

from collections.abc import Iterable

from forze.application.execution.operations import (
    FrozenOperationRegistry,
    OperationCatalogEntry,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, StrKey

from .value_objects import OperationToolset, ToolDef

# ----------------------- #


def _input_schema(entry: OperationCatalogEntry) -> JsonDict:
    """JSON Schema of *entry*'s input DTO, or ``{}`` when the operation takes no input."""

    descriptor = entry.descriptor

    if descriptor is None or descriptor.input_type is None:
        return {}

    return descriptor.input_type.model_json_schema()


# ....................... #


def operation_tools(
    registry: FrozenOperationRegistry,
    *,
    include: Iterable[StrKey],
    read_only: bool = True,
) -> OperationToolset:
    """Project the allowlisted operations of *registry* into a tool palette.

    :param registry: The frozen registry whose operations back the tools.
    :param include: The operations to project, by key — **required**. A toolset is reviewed
        like a permission grant, so there is no way to ask for the whole registry.
    :param read_only: Project only ``QUERY``-plane operations (the default). A read-only
        toolset cannot mutate because the write operations are *absent from the palette*,
        not merely denied at dispatch — an agent's tools are its capabilities.
    :returns: The palette: :attr:`~.value_objects.OperationToolset.defs` for the agent's
        SDK, and the bindings dispatch resolves names against.
    :raises CoreException: When *include* is empty, names an operation the registry does
        not have, names a command operation in a read-only toolset, or names an operation
        that projects a sensitive read model.
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

    for op in selected:
        # A name may legitimately be listed twice when a palette is assembled from
        # concatenated slices (the query tools plus one command). The palette keeps the
        # first position and drops the repeat: two identical defs would be rejected by
        # the SDKs downstream, and refusing would make composition awkward for a mistake
        # that is never ambiguous.
        if str(op) in bindings:
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

        name = str(entry.op)
        description = entry.descriptor.description if entry.descriptor is not None else None

        defs.append(ToolDef(name=name, description=description, input_schema=_input_schema(entry)))
        bindings[name] = entry

    return OperationToolset(registry=registry, defs=tuple(defs), bindings=bindings)
