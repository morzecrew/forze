"""The owned tables a :class:`PostgresDepsModule` binds (RFC 0080 §5.1, §5.7).

Every store table: the three durable tables, the outbox and its delivery log, the HLC
checkpoint, the inbox, idempotency claims, counters, principal epochs and rotating
credentials. Several routes may name one table (two outbox routes, say), each with its own
config, so a table is bound once, with the union of its routes' shapes: a column one route's
switch leaves out stays while another route writes it.

Given the spec registry, also every framework-owned document table: each read-write document
route whose spec the registry catalogues as
:attr:`~forze.application.contracts.inventory.SpecSource.FRAMEWORK`. Forze ships that spec's
models, so the application cannot change its table. Its shape is derived from the spec and the
route's config (:func:`~forze_postgres.kernel.catalog.owned.documents.document_shape`).

A relation given as a tenant-scoped resolver names no single table until a tenant is bound,
so it is skipped and reported, as document schema validation skips dynamic routes.
"""

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any, Protocol, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    SpecPlane,
    SpecRegistry,
    SpecSource,
)
from forze.application.contracts.resolution import RelationSpec, resolve_value
from forze.base.exceptions import exc

from ...adapters.durable.schema import (
    durable_run_shape,
    durable_schedule_shape,
    durable_step_shape,
)
from ...adapters.store_schema import (
    counter_shape,
    hlc_checkpoint_shape,
    idempotency_shape,
    inbox_shape,
    outbox_delivery_log_shape,
    outbox_shape,
    principal_epoch_shape,
    rotating_credentials_shape,
)
from ...kernel.catalog.owned import BoundOwnedRelation, OwnedRelationShape
from ...kernel.catalog.owned.documents import document_shape
from ...kernel.catalog.owned.render import default_partition_name
from .configs import PostgresDocumentConfig, PostgresOutboxConfig
from .document_binding import postgres_document_schema_spec_for_binding
from .module import PostgresDepsModule

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedRelations:
    """The owned relations a deps module binds, and the routes skipped as tenant-scoped."""

    bound: tuple[BoundOwnedRelation, ...]
    """Each bound store table with its shape, in deps-module field order, once per table."""

    skipped: tuple[str, ...]
    """Routes whose relation is a tenant-scoped resolver, so no single table is known."""

    documents: tuple[BoundOwnedRelation, ...] = ()
    """Each bound framework-owned document table, by route name. Empty unless the spec
    registry was given. The startup step leaves these to document schema validation (RFC 0080
    row 36), so a missing column is reported once."""

    # ....................... #

    @property
    def tables(self) -> tuple[BoundOwnedRelation, ...]:
        """The store tables, then the document tables: what ``forze schema`` prints."""

        return (*self.bound, *self.documents)


# ....................... #


class _RoutedConfig(Protocol):
    @property
    def relation(self) -> RelationSpec: ...


type _Entry = tuple[str, RelationSpec, OwnedRelationShape]
"""A route, the relation it names, and the shape its config declares."""


def _single[C: _RoutedConfig](
    route: str,
    config: C | None,
    shape: Callable[[C], OwnedRelationShape],
) -> list[_Entry]:
    return [] if config is None else [(route, config.relation, shape(config))]


def _routes[C: _RoutedConfig](
    field: str,
    configs: Mapping[str, C] | None,
    shape: Callable[[C], OwnedRelationShape],
) -> list[_Entry]:
    return [
        (f"{field}.{route}", config.relation, shape(config))
        for route, config in sorted((configs or {}).items())
    ]


def _delivery_logs(configs: Mapping[str, PostgresOutboxConfig] | None) -> list[_Entry]:
    return [
        (f"outboxes.{route}.delivery_log", config.delivery_log, outbox_delivery_log_shape(config))
        for route, config in sorted((configs or {}).items())
        if config.delivery_log is not None
    ]


def _bind(
    entries: Iterable[_Entry],
    *,
    documents: Iterable[_Entry] = (),
    skipped_documents: Iterable[str] = (),
) -> OwnedRelations:
    """One bound relation per table, its routes joined and their shapes united.

    *documents* are the document tables, bound after the store tables. No store table shares
    one, since the union of a document's shape with any other refuses.
    """

    documents = list(documents)
    # By table, not by route: tenants whose resolver names one table join their routes into
    # one label that no single route matches.
    document_tables = frozenset(
        tuple(relation) for _, relation, _ in documents if not callable(relation)
    )
    entries = [*entries, *documents]
    tables: dict[tuple[str, str], BoundOwnedRelation] = {}
    skipped: list[str] = []

    for route, relation, shape in entries:
        if callable(relation):
            skipped.append(route)
            continue

        schema, name = relation
        found = tables.get((schema, name))

        if found is None:
            tables[schema, name] = BoundOwnedRelation(
                route=route, schema=schema, name=name, shape=shape
            )
            continue

        tables[schema, name] = attrs.evolve(
            found, route=f"{found.route}, {route}", shape=found.shape.union(shape)
        )

    # Every name a partition could take, the bound tables' and each one already handed out.
    taken = set(tables)
    bound = [_name_default_partition(relation, taken) for relation in tables.values()]

    def is_document(relation: BoundOwnedRelation) -> bool:
        return (relation.schema, relation.name) in document_tables

    return OwnedRelations(
        bound=tuple(relation for relation in bound if not is_document(relation)),
        skipped=(*skipped, *skipped_documents),
        documents=tuple(relation for relation in bound if is_document(relation)),
    )


def _name_default_partition(
    relation: BoundOwnedRelation,
    taken: set[tuple[str, str]],
) -> BoundOwnedRelation:
    """*relation* with a ``DEFAULT`` partition name nothing in *taken* holds, which it then
    adds to *taken*.

    ``<name>_default`` is kept where it is free. Where a bound table or an earlier partition
    already holds it, such as an outbox ``events_default`` beside the log ``events``, the
    hashed form is taken.

    :raises CoreException: ``configuration``, code ``postgres_owned_partition_name_taken``,
        when both forms are taken.
    """

    if relation.shape.partitioned_by is None:
        return relation

    for hashed in (False, True):
        name = default_partition_name(relation.name, hashed=hashed)

        if (relation.schema, name) not in taken:
            taken.add((relation.schema, name))

            return relation if not hashed else attrs.evolve(relation, default_partition=name)

    raise exc.configuration(
        f"Every name for the DEFAULT partition of {relation.schema}.{relation.name} is taken "
        "by another table the deps module binds, or another table's partition; rename one.",
        code="postgres_owned_partition_name_taken",
    )


def _entries(deps: PostgresDepsModule) -> list[_Entry]:
    return [
        *_single("durable_run", deps.durable_run, durable_run_shape),
        *_single("durable_step", deps.durable_step, durable_step_shape),
        *_single("durable_schedule", deps.durable_schedule, durable_schedule_shape),
        *_routes("outboxes", deps.outboxes, outbox_shape),
        *_delivery_logs(deps.outboxes),
        *_single("hlc_checkpoint", deps.hlc_checkpoint, hlc_checkpoint_shape),
        *_routes("inboxes", deps.inboxes, inbox_shape),
        *_routes("idempotencies", deps.idempotencies, idempotency_shape),
        *_routes("counters", deps.counters, counter_shape),
        *_single("principal_epochs", deps.principal_epochs, principal_epoch_shape),
        *_single("rotating_credentials", deps.rotating_credentials, rotating_credentials_shape),
    ]


type _Document = tuple[str, PostgresDocumentConfig, DocumentSpec[Any, Any, Any, Any]]
"""A framework-owned document route, its config and its spec."""


def _framework_documents(
    deps: PostgresDepsModule,
    specs: SpecRegistry | FrozenSpecRegistry | None,
) -> list[_Document]:
    """The read-write routes whose spec *specs* catalogues as ``FRAMEWORK``, by route name.

    ``KIT`` is not enough: a kit registers the application's own spec as ``KIT``. A read-only
    route is the writer's table, not this module's.
    """

    if specs is None:
        return []

    registry = specs.freeze() if isinstance(specs, SpecRegistry) else specs
    found: list[_Document] = []

    for route, config in sorted((deps.rw_documents or {}).items()):
        entry = registry.find(SpecPlane.DOCUMENT, str(route))

        if (
            entry is not None
            and entry.source is SpecSource.FRAMEWORK
            and isinstance(entry.spec, DocumentSpec)
        ):
            found.append((str(route), config, entry.spec))  # pyright: ignore[reportUnknownArgumentType]

    return found


def _is_dynamic(config: PostgresDocumentConfig) -> bool:
    return any(callable(relation) for relation in (config.read, config.write, config.history))


def _document_entry(
    route: str,
    config: PostgresDocumentConfig,
    spec: DocumentSpec[Any, Any, Any, Any],
    *,
    label: str | None = None,
) -> _Entry:
    """The write relation of a static route, with the table its spec and config derive."""

    schema_spec = postgres_document_schema_spec_for_binding(route, spec=spec, config=config)

    return (label or route, config.write, document_shape(schema_spec))


async def _for_tenant(config: PostgresDocumentConfig, tenant: UUID) -> PostgresDocumentConfig:
    """*config* with each resolver replaced by the table it names for *tenant*."""

    async def resolved(relation: RelationSpec | None) -> RelationSpec | None:
        if relation is None or not callable(relation):
            return relation

        return await resolve_value(cast(RelationSpec, relation), tenant)

    return attrs.evolve(
        config,
        read=await resolved(config.read),
        write=await resolved(config.write),
        history=await resolved(config.history),
    )


def owned_relations(
    deps: PostgresDepsModule,
    *,
    specs: SpecRegistry | FrozenSpecRegistry | None = None,
) -> OwnedRelations:
    """Pair each owned table *deps* configures with the shape its configs declare.

    With *specs*, the application's spec registry, the framework-owned document tables are
    bound too (:attr:`OwnedRelations.documents`). A relation given as a tenant-scoped resolver
    is skipped; :func:`resolve_owned_relations` binds it for the tenants it is given.

    :raises CoreException: ``configuration``, code ``postgres_owned_relation_shared``, when
        routes that name one table declare different tables for it, such as an outbox and an
        inbox; code ``postgres_document_render_refused`` when a framework spec's table cannot
        be printed.
    """

    documents = _framework_documents(deps, specs)

    return _bind(
        _entries(deps),
        documents=[
            _document_entry(route, config, spec)
            for route, config, spec in documents
            if not _is_dynamic(config)
        ],
        skipped_documents=[route for route, config, _ in documents if _is_dynamic(config)],
    )


async def resolve_owned_relations(
    deps: PostgresDepsModule,
    *,
    tenants: Sequence[UUID],
    specs: SpecRegistry | FrozenSpecRegistry | None = None,
) -> OwnedRelations:
    """:func:`owned_relations`, with each tenant-scoped relation bound once per tenant.

    Each resolver is called for every tenant in *tenants*, and awaited when it returns an
    awaitable, as the adapters resolve it. The route of each table it names carries the
    tenant, ``outboxes.orders[<tenant>]``. Tenants whose resolver names one table bind it
    once. With no tenants, a tenant-scoped relation is skipped, as by
    :func:`owned_relations`.

    :raises CoreException: as :func:`owned_relations`, and whatever a resolver raises.
    """

    entries: list[_Entry] = []

    for route, relation, shape in _entries(deps):
        if not callable(relation) or not tenants:
            entries.append((route, relation, shape))
            continue

        for tenant in tenants:
            # The spec as configured, so the resolver's result types the value.
            configured = cast(RelationSpec, relation)
            resolved = await resolve_value(configured, tenant)
            entries.append((f"{route}[{tenant}]", resolved, shape))

    documents: list[_Entry] = []
    skipped: list[str] = []

    for route, config, spec in _framework_documents(deps, specs):
        if not _is_dynamic(config):
            documents.append(_document_entry(route, config, spec))
            continue

        if not tenants:
            skipped.append(route)
            continue

        for tenant in tenants:
            bound = await _for_tenant(config, tenant)
            documents.append(_document_entry(route, bound, spec, label=f"{route}[{tenant}]"))

    return _bind(entries, documents=documents, skipped_documents=skipped)
