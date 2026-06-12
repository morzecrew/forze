"""Catalog metadata for operations.

A :class:`OperationDescriptor` carries the *intrinsic*, interface-agnostic facts about
an operation that survive neither the handler factory (which erases ``Handler[Args, R]``
type arguments) nor the docstring: its input/output DTO types and a human description.

This metadata is what lets a driving adapter (MCP, an auto-generated HTTP router, …)
build a tool/route catalog without re-deriving schemas. Exposure decisions — *which*
descriptors a given surface projects — stay with the interface, not here. Likewise the
read/write classification lives on the plan (:class:`OperationKind`), not the descriptor,
because it is execution-semantic; :meth:`OperationCatalogEntry` joins the two.
"""

from typing import final

import attrs
from pydantic import BaseModel

from forze.base.primitives import JsonDict, StrKey

from .planning import OperationKind

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationDescriptor:
    """Interface-agnostic catalog metadata for a single operation.

    Carries the input/output DTO types and a human description so any driving adapter
    can derive a schema and a label without introspecting the handler factory.
    """

    input_type: type[BaseModel] | None = None
    """Input DTO type, used to derive the request schema. ``None`` for input-less ops."""

    output_type: type[BaseModel] | None = None
    """Output DTO type, used to derive the response schema. ``None`` for void ops."""

    description: str | None = None
    """Human/LLM-facing description of what the operation does."""

    title: str | None = None
    """Optional short, human-friendly title."""

    tags: tuple[str, ...] = ()
    """Optional free-form tags for grouping/filtering in a catalog."""

    sensitive: bool = False
    """The operation projects a read model that carries credential/secret material
    (``spec.sensitive``). An intrinsic fact, not an exposure decision: generated
    external surfaces (HTTP route generators, MCP tools/resources) must refuse to
    project operations marked sensitive."""

    # ....................... #

    def input_schema(self) -> JsonDict | None:
        """JSON schema for the input DTO, or ``None`` when the operation takes no input."""

        return None if self.input_type is None else self.input_type.model_json_schema()

    # ....................... #

    def output_schema(self) -> JsonDict | None:
        """JSON schema for the output DTO, or ``None`` when the operation returns nothing."""

        return (
            None if self.output_type is None else self.output_type.model_json_schema()
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OperationCatalogEntry:
    """A single catalog entry: descriptor metadata joined with the plan's read/write kind."""

    op: StrKey
    """Operation key."""

    kind: OperationKind
    """Read (``QUERY``) vs write (``COMMAND``) classification, taken from the plan."""

    descriptor: OperationDescriptor | None = None
    """Catalog metadata, if the operation declared one."""

    supports_idempotency_key: bool = False
    """The operation's plan carries an idempotency wrap: a duplicate invocation that
    binds the same idempotency key replays the stored result instead of re-executing.

    Derived at freeze via structural ``ProvidesIdempotency`` detection. "Supports",
    not "requires" — the wrap is a no-op when the caller binds no key, so surfaces
    should document the key as an *optional* parameter."""

    required_permissions: tuple[str, ...] = ()
    """Sorted union of permission keys declared by the plan's authz hooks
    (structural ``DeclaresAuthz`` detection at freeze); empty = no declared authz.

    Honesty caveat: declared-hook introspection, **not** a security statement. An
    operation may enforce authorization inside its handler (or via an undeclared
    hook) invisibly, so an empty tuple must not be read as "unauthenticated/open"."""

    # ....................... #

    @property
    def is_read_only(self) -> bool:
        """Whether the operation is read-only (a ``QUERY``)."""

        return self.kind is OperationKind.QUERY
