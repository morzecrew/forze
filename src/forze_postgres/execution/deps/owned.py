"""The owned tables a :class:`PostgresDepsModule` binds (RFC 0080 §5.1).

P1 covers the three durable tables. A relation given as a tenant-scoped resolver names no
single table until a tenant is bound, so it is skipped and reported, as document schema
validation skips dynamic routes.
"""

from collections.abc import Callable
from typing import Protocol, final

import attrs

from forze.application.contracts.resolution import RelationSpec

from ...adapters.durable.schema import (
    durable_run_shape,
    durable_schedule_shape,
    durable_step_shape,
)
from ...kernel.catalog.owned import BoundOwnedRelation, OwnedRelationShape
from .module import PostgresDepsModule

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedRelations:
    """The owned relations a deps module binds, and the routes skipped as tenant-scoped."""

    bound: tuple[BoundOwnedRelation, ...]
    """Each bound relation with its shape, in deps-module field order."""

    skipped: tuple[str, ...]
    """Routes whose relation is a tenant-scoped resolver, so no single table is known."""


# ....................... #


class _RoutedConfig(Protocol):
    @property
    def relation(self) -> RelationSpec: ...


def _bind[C: _RoutedConfig](
    route: str,
    config: C | None,
    shape: Callable[[C], OwnedRelationShape],
    relations: OwnedRelations,
) -> OwnedRelations:
    """*relations* with *route* added, bound or skipped, or unchanged when it is not wired."""

    if config is None:
        return relations

    relation = config.relation

    if callable(relation):
        return attrs.evolve(relations, skipped=(*relations.skipped, route))

    schema, name = relation
    bound = BoundOwnedRelation(route=route, schema=schema, name=name, shape=shape(config))

    return attrs.evolve(relations, bound=(*relations.bound, bound))


def owned_relations(deps: PostgresDepsModule) -> OwnedRelations:
    """Pair each owned relation *deps* configures with the shape its config declares."""

    relations = OwnedRelations(bound=(), skipped=())
    relations = _bind("durable_run", deps.durable_run, durable_run_shape, relations)
    relations = _bind("durable_step", deps.durable_step, durable_step_shape, relations)

    return _bind("durable_schedule", deps.durable_schedule, durable_schedule_shape, relations)
