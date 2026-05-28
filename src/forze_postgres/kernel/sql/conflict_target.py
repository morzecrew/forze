"""Resolve ``ON CONFLICT`` targets for Postgres document write gateways."""

from forze.base.exceptions import exc

from forze_postgres.kernel.catalog.introspect import PostgresIntrospector

# ----------------------- #


async def resolve_write_conflict_target(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    configured: tuple[str, ...] | None,
    document_label: str | None = None,
) -> tuple[str, ...]:
    """Resolve column names for ``INSERT … ON CONFLICT (…)`` on a write relation.

    When *configured* is ``None``, returns the relation primary-key columns from
    catalogs (plain column indexes only). When *configured* is set, verifies a
    matching PRIMARY KEY or UNIQUE constraint exists and returns *configured*.

    :param introspector: Postgres catalog introspector.
    :param schema: Table schema.
    :param relation: Table name.
    :param configured: Explicit conflict columns from adapter config, if any.
    :param document_label: Optional document route name for error messages.
    :returns: Ordered column names for ``ON CONFLICT``.
    :raises exc.internal: When auto-inference fails or explicit config is invalid.
    """

    label = document_label or f"{schema}.{relation}"
    pk = await introspector.get_primary_key_columns(schema=schema, relation=relation)

    if configured is not None:
        if not configured:
            raise exc.internal(
                f"Postgres conflict_target for {label!r} must not be empty.",
                code="postgres_conflict_target_invalid",
            )

        if not await introspector.constraint_exists_for_columns(
            schema=schema,
            relation=relation,
            columns=configured,
        ):
            raise exc.internal(
                f"Postgres conflict_target {list(configured)!r} for {label!r} does not "
                f"match a PRIMARY KEY or UNIQUE constraint on {schema}.{relation}.",
                code="postgres_conflict_target_invalid",
                details={
                    "document": document_label,
                    "schema": schema,
                    "relation": relation,
                    "conflict_target": list(configured),
                },
            )

        return configured

    if not pk:
        raise exc.internal(
            f"Cannot infer conflict_target for {label!r} ({schema}.{relation}): "
            "primary key is missing or uses an expression/partial index. "
            "Set conflict_target explicitly in PostgresDocumentConfig.",
            code="postgres_conflict_target_invalid",
            details={
                "document": document_label,
                "schema": schema,
                "relation": relation,
            },
        )

    return pk
