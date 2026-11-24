"""The owned tables a :class:`PostgresDepsModule` binds (RFC 0080 §5.1).

Every store table: the three durable tables, the outbox and its delivery log, the HLC
checkpoint, the inbox, idempotency claims, counters, principal epochs and rotating
credentials. Several routes may name one table (two outbox routes, say), each with its own
config, so a table is bound once, with the union of its routes' shapes: a column one route's
switch leaves out stays while another route writes it.

A relation given as a tenant-scoped resolver names no single table until a tenant is bound,
so it is skipped and reported, as document schema validation skips dynamic routes.
"""

from collections.abc import Callable, Iterable, Mapping
from typing import Protocol, final

import attrs

from forze.application.contracts.resolution import RelationSpec

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
from .configs import PostgresOutboxConfig
from .module import PostgresDepsModule

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedRelations:
    """The owned relations a deps module binds, and the routes skipped as tenant-scoped."""

    bound: tuple[BoundOwnedRelation, ...]
    """Each bound table with its shape, in deps-module field order, once per table."""

    skipped: tuple[str, ...]
    """Routes whose relation is a tenant-scoped resolver, so no single table is known."""


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


def _bind(entries: Iterable[_Entry]) -> OwnedRelations:
    """One bound relation per table, its routes joined and their shapes united."""

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

    return OwnedRelations(bound=tuple(tables.values()), skipped=tuple(skipped))


def owned_relations(deps: PostgresDepsModule) -> OwnedRelations:
    """Pair each owned table *deps* configures with the shape its configs declare.

    :raises CoreException: ``configuration``, code ``postgres_owned_relation_shared``, when
        routes that name one table declare different tables for it, such as an outbox and an
        inbox.
    """

    return _bind(
        [
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
    )
