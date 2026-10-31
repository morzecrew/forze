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
    """Check write-relation triggers align with ``bookkeeping_strategy``.

    With ``'database'``, a write relation without a ``BEFORE UPDATE … FOR EACH ROW`` trigger
    is refused, and a history-enabled one without an INSERT trigger is warned about. With
    ``'application'``, such UPDATE triggers are warned about: they risk double revision bumps.

    :raises CoreException: ``internal``, code ``postgres_bookkeeping_validation_failed``,
        when ``'database'`` finds no ``BEFORE UPDATE … FOR EACH ROW`` trigger on the write
        relation.
    """

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
                    "BEFORE UPDATE ... FOR EACH ROW trigger on the write relation to bump rev "
                    f"(for example {relation!r}_bump_rev).",
                    code="postgres_bookkeeping_validation_failed",
                    details={
                        "document": spec.name,
                        "schema": schema,
                        "relation": relation,
                        "strategy": spec.bookkeeping_strategy,
                    },
                )

            # The UPDATE side is the revision bump trigger required above, which may also write
            # history, so only a relation with no INSERT trigger is surely missing history rows.
            if spec.history_enabled and not await introspector.insert_triggers(
                schema=schema,
                relation=relation,
            ):
                logger.warning(
                    "Postgres bookkeeping for document %r write relation %s.%s: "
                    "history_enabled=True with bookkeeping_strategy='database', but no trigger "
                    "fires on INSERT — created documents get no history row, since database "
                    "triggers must write history under this strategy.",
                    spec.name,
                    schema,
                    relation,
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
