"""The owned tables a :class:`PostgresDepsModule` binds (RFC 0080 §5.1).

P1 covers the three durable tables. A relation given as a tenant-scoped resolver names no
single table until a tenant is bound, so it is skipped and reported, as document schema
validation skips dynamic routes.
"""

from collections.abc import Callable
from typing import Any, final

import attrs

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


def owned_relations(deps: PostgresDepsModule) -> OwnedRelations:
    """Pair each owned relation *deps* configures with the shape its config declares."""

    routes: tuple[tuple[str, Any, Callable[[Any], OwnedRelationShape]], ...] = (
        ("durable_run", deps.durable_run, durable_run_shape),
        ("durable_step", deps.durable_step, durable_step_shape),
        ("durable_schedule", deps.durable_schedule, durable_schedule_shape),
    )
    bound: list[BoundOwnedRelation] = []
    skipped: list[str] = []

    for route, config, shape in routes:
        if config is None:
            continue

        if callable(config.relation):
            skipped.append(route)
            continue

        schema, name = config.relation
        bound.append(BoundOwnedRelation(route=route, schema=schema, name=name, shape=shape(config)))

    return OwnedRelations(bound=tuple(bound), skipped=tuple(skipped))
