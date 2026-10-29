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

from datetime import timedelta
from typing import Any, final

import attrs
from pydantic import BaseModel, TypeAdapter

from forze.application.contracts.querying import QueryDiscovery
from forze.base.exceptions import exc
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

    output_type: Any = None
    """Output type, used to derive the response schema: a DTO, or any type pydantic validates —
    ``Model | None`` for an operation that may find nothing (served as ``null``), a list.
    ``None`` for void ops."""

    description: str | None = None
    """Human/LLM-facing description of what the operation does."""

    title: str | None = None
    """Optional short, human-friendly title."""

    tags: tuple[str, ...] = attrs.field(factory=tuple)
    """Optional free-form tags for grouping/filtering in a catalog."""

    sensitive: bool = False
    """The operation projects a read model that carries credential/secret material
    (``spec.sensitive``). An intrinsic fact, not an exposure decision: generated
    external surfaces (HTTP route generators, MCP tools/resources) must refuse to
    project operations marked sensitive."""

    streams: bool = False
    """The operation's result carries a byte stream (a single-use async chunk iterator), not a
    JSON value. An intrinsic fact, not an exposure decision: generated JSON surfaces (MCP
    tools and resource templates, agent tools, generated HTTP routes, ``forze ops``) cannot
    carry it, so they leave it out of a default projection and refuse it when named; serve
    it from a streaming route."""

    members: tuple[str, ...] = attrs.field(factory=tuple)
    """Names of the sources the operation reads rows from, each with its own read permission
    (a hub or federated search's members). An intrinsic fact: when the plan declares authz
    (``DeclaresAuthz``), hooks that authorize each member (``AuthorizesMembers``) must cover
    every one, or the registry refuses to freeze. Empty for a single-source operation."""

    members_of: str | None = None
    """Name of the hub or federated search whose :attr:`members` these are: the name its
    adapter reads the member grant under. A member authorization hook must name the same
    search, or the registry refuses to freeze. ``None`` for a single-source operation."""

    commutative: bool = False
    """The operation is declared **order-independent**: any interleaving of it against
    its peers must converge to the same final state. A *declaration*, verified — not
    consumed — by DST: the simulator asserts it (a declared-commutative operation whose
    reorderings diverge is a first-class finding), but no execution path reads this flag,
    so a wrong declaration costs nothing at runtime and is caught only under simulation.
    Default ``False`` (no claim made)."""

    query_discovery: QueryDiscovery | None = None
    """For a filter-accepting operation, the read model's filter/sort/aggregate surface
    (which fields, and which operators per field). Lets a driving adapter advertise the
    query contract — OpenAPI vendor extension, MCP tool text — so a caller need not
    discover it by trial and error. ``None`` for operations that take no filter."""

    # ....................... #

    def input_schema(self) -> JsonDict | None:
        """JSON schema for the input DTO, or ``None`` when the operation takes no input."""

        return None if self.input_type is None else self.input_type.model_json_schema()

    # ....................... #

    def output_schema(self) -> JsonDict | None:
        """JSON schema for the output DTO, or ``None`` when the operation returns nothing."""

        if self.output_type is None:
            return None

        # A model's schema is the same either way, so its operation fingerprint is unchanged.
        return TypeAdapter[Any](self.output_type).json_schema()


# ....................... #


def refuse_dropped_members(
    op: StrKey,
    current: OperationDescriptor | None,
    replacement: OperationDescriptor,
) -> None:
    """Refuse replacing *current* with a descriptor naming fewer :attr:`~OperationDescriptor.members`,
    or another :attr:`~OperationDescriptor.members_of`.

    The member authorization gate reads them, so an override dropping one would let the
    operation freeze without that member's check.

    :raises exc.configuration: *replacement* leaves out a member *current* names, or names
        another search.
    """

    if current is None:
        return

    if missing := sorted(set(current.members) - set(replacement.members)):
        raise exc.configuration(
            f"Descriptor override for operation {op!r} drops member(s) {missing}, which the "
            "member authorization check reads; keep them (attrs.evolve the descriptor).",
            code="descriptor_members_dropped",
        )

    if current.members_of is not None and replacement.members_of != current.members_of:
        raise exc.configuration(
            f"Descriptor override for operation {op!r} changes the search its members belong "
            f"to from {current.members_of!r}, which the member authorization check reads; "
            "keep it (attrs.evolve the descriptor).",
            code="descriptor_members_of_changed",
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

    required_permissions: tuple[str, ...] = attrs.field(factory=tuple)
    """Sorted union of permission keys declared by the plan's authz hooks
    (structural ``DeclaresAuthz`` detection at freeze); empty = no declared authz.

    Honesty caveat: declared-hook introspection, **not** a security statement. An
    operation may enforce authorization inside its handler (or via an undeclared
    hook) invisibly, so an empty tuple must not be read as "unauthenticated/open"."""

    requires_authn: bool = False
    """The plan declares it needs an authenticated principal (structural
    ``DeclaresAuthn`` detection at freeze — an authn guard, or any authz hook, since
    authorization presupposes a bound principal). Transports project this into their
    auth surface (FastAPI ``security`` / OpenAPI, MCP tool text).

    Same honesty caveat as :attr:`required_permissions`: declared-hook introspection,
    **not** a security statement — ``False`` does not prove the operation is open."""

    deadline: timedelta | None = None
    """Per-invocation time budget declared by the plan, or ``None`` for no cap.

    The effective merged budget (tightest across patches and the explicit plan).
    Enforced at operation entry: exceeding it fails with a non-retryable
    ``TIMEOUT`` (``code="deadline_exceeded"``; **504** over the FastAPI edge).
    A caller-bound deadline can only tighten it further, never extend it."""

    # ....................... #

    @property
    def is_read_only(self) -> bool:
        """Whether the operation is read-only (a ``QUERY``)."""

        return self.kind is OperationKind.QUERY
