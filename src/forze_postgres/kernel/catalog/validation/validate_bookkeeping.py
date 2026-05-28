"""Validate Postgres document bookkeeping strategy against relation triggers."""

from collections.abc import Sequence

import attrs

from forze.base.exceptions import exc

from forze_postgres.kernel._logger import logger
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.gateways.types import PostgresBookkeepingStrategy

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDocumentBookkeepingSpec:
    """Bookkeeping validation input for one writable document route."""

    name: str
    """Document route name (for error messages)."""

    bookkeeping_strategy: PostgresBookkeepingStrategy
    """Configured bookkeeping strategy from :class:`~forze_postgres.execution.deps.configs.PostgresDocumentConfig`."""

    write_relation: tuple[str, str]
    """Write table ``(schema, name)``."""

    history_enabled: bool = False
    """Whether the kernel spec enables history."""


# ....................... #


async def validate_postgres_document_bookkeeping(
    introspector: PostgresIntrospector,
    specs: Sequence[PostgresDocumentBookkeepingSpec],
) -> None:
    """Check write-relation UPDATE triggers align with ``bookkeeping_strategy``."""

    for spec in specs:
        schema, relation = spec.write_relation
        triggers = await introspector.get_relation_update_triggers(
            schema=schema,
            relation=relation,
        )
        label = f"{spec.name} write ({schema}.{relation})"

        if spec.bookkeeping_strategy == "database":
            if not triggers:
                raise exc.internal(
                    f"Postgres bookkeeping validation failed for {label!r}: "
                    f"bookkeeping_strategy='database' requires at least one user "
                    "UPDATE trigger on the write relation (for example "
                    f"{relation!r}_bump_rev).",
                    code="postgres_bookkeeping_validation_failed",
                    details={
                        "document": spec.name,
                        "schema": schema,
                        "relation": relation,
                        "strategy": spec.bookkeeping_strategy,
                    },
                )

            if spec.history_enabled:
                logger.warning(
                    "Postgres bookkeeping for document %r: history_enabled=True with "
                    "bookkeeping_strategy='database' — the application history gateway "
                    "is a no-op; history rows must be written by database triggers.",
                    spec.name,
                )

        elif triggers:
            logger.warning(
                "Postgres bookkeeping for document %r write relation %s.%s: "
                "bookkeeping_strategy='application' but UPDATE trigger(s) %s are "
                "present — risk of double revision bumps.",
                spec.name,
                schema,
                relation,
                sorted(triggers),
            )
