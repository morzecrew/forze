"""Compare Pydantic document shapes to Postgres relation columns (startup validation)."""

from typing import Sequence

import attrs
from pydantic import BaseModel

from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import exc
from forze.base.serialization import pydantic_field_names
from forze.domain.models import DocumentHistory
from forze_postgres.kernel._logger import logger
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.gateways.types import PostgresBookkeepingStrategy
from forze_postgres.kernel.sql.conflict_target import resolve_write_conflict_target

from .validate_schema_types import (
    validate_field_nullability,
    validate_field_type_compatibility,
)

# ----------------------- #


def _write_field_names_union(
    domain: type[BaseModel],
    create: type[BaseModel],
    update: type[BaseModel] | None,
) -> frozenset[str]:
    names = pydantic_field_names(domain, include_computed=False) | pydantic_field_names(
        create,
        include_computed=False,
    )

    if update is not None:
        names |= pydantic_field_names(update, include_computed=False)

    return frozenset(names)


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDocumentSchemaSpec:
    """Describe one document's read/write (and optional history) relations for schema checks."""

    name: str
    """Document route name (for error messages)."""

    read_model: type[BaseModel]
    """Read model deserialized from the read relation."""

    read_relation: tuple[str, str]
    """Read relation ``(schema, name)``."""

    read_omit_fields: frozenset[str] = frozenset()
    """Read model field names that are not stored on the read relation (computed, etc.)."""

    tenant_aware: bool = False
    """When ``True``, the write relation must expose :data:`~forze.application.contracts.tenancy.TENANT_ID_FIELD`."""

    write_domain_model: type[BaseModel] | None = None
    """Domain document model (persisted row shape)."""

    write_create_model: type[BaseModel] | None = None
    """Create command model."""

    write_update_model: type[BaseModel] | None = None
    """Update command model, if any."""

    write_relation: tuple[str, str] | None = None
    """Write table ``(schema, name)``."""

    write_omit_fields: frozenset[str] = frozenset()
    """Write-side field names not mapped to columns (rare escape hatch)."""

    history_enabled: bool = False
    """When ``True``, validate the history relation if :attr:`history_relation` is set."""

    history_relation: tuple[str, str] | None = None
    """History table ``(schema, name)``."""

    history_omit_fields: frozenset[str] = frozenset()
    """History row field names omitted from the physical table."""

    bookkeeping_strategy: PostgresBookkeepingStrategy | None = None
    """Bookkeeping strategy for the write relation; ``None`` when read-only."""

    conflict_target: tuple[str, ...] | None = None
    """Optional ``ON CONFLICT`` columns for ensure/upsert; ``None`` infers PRIMARY KEY."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.history_enabled and self.history_relation is None:
            raise exc.internal(
                f"Document {self.name!r}: history_enabled requires history_relation.",
            )

        if self.write_relation is not None:
            if self.write_domain_model is None or self.write_create_model is None:
                raise exc.internal(
                    f"Document {self.name!r}: write_relation requires "
                    "write_domain_model and write_create_model.",
                )


# ....................... #


async def _require_columns(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    required: frozenset[str],
    label: str,
) -> None:
    types = await introspector.get_column_types(schema=schema, relation=relation)
    have = frozenset(types.keys())
    missing = required - have

    if missing:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r} ({schema}.{relation}): "
            f"missing columns {sorted(missing)}.",
            code="postgres_schema_validation_failed",
            details={
                "document": label,
                "schema": schema,
                "relation": relation,
                "missing": sorted(missing),
            },
        )


async def _validate_relation_models(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    models: Sequence[tuple[type[BaseModel], frozenset[str], str]],
) -> None:
    column_types = await introspector.get_column_types(schema=schema, relation=relation)

    for model, omit_fields, label in models:
        validate_field_type_compatibility(
            model=model,
            column_types=column_types,
            omit_fields=omit_fields,
            label=label,
        )
        validate_field_nullability(
            model=model,
            column_types=column_types,
            omit_fields=omit_fields,
            label=label,
        )


async def _validate_tenant_column(
    introspector: PostgresIntrospector,
    *,
    schema: str,
    relation: str,
    label: str,
) -> None:
    types = await introspector.get_column_types(schema=schema, relation=relation)
    pg_t = types.get(TENANT_ID_FIELD)

    if pg_t is None:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"tenant-aware document requires column {TENANT_ID_FIELD!r}.",
            code="postgres_schema_validation_failed",
            details={"label": label, "schema": schema, "relation": relation},
        )

    if pg_t.base != "uuid" or pg_t.is_array:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"{TENANT_ID_FIELD!r} must be type uuid.",
            code="postgres_schema_validation_failed",
            details={
                "label": label,
                "actual_base": pg_t.base,
                "is_array": pg_t.is_array,
            },
        )

    if not pg_t.not_null:
        raise exc.internal(
            f"Postgres schema validation failed for {label!r}: "
            f"{TENANT_ID_FIELD!r} must be NOT NULL.",
            code="postgres_schema_validation_failed",
            details={"label": label},
        )


def _warn_read_not_subset_of_write(
    *,
    spec: PostgresDocumentSchemaSpec,
    write_column_types: dict[str, PostgresType],
) -> None:
    if spec.write_relation is None:
        return

    if spec.read_relation == spec.write_relation:
        return

    read_fields = (
        pydantic_field_names(spec.read_model, include_computed=False)
        - spec.read_omit_fields
    )
    write_cols = frozenset(write_column_types.keys())
    extra = read_fields - write_cols

    if extra:
        logger.warning(
            "Postgres schema validation for document %r: read fields %s are not on "
            "write relation %s.%s (expected for views; silence with read_omit_fields).",
            spec.name,
            sorted(extra),
            spec.write_relation[0],
            spec.write_relation[1],
        )


def _warn_unused_tenant_column(
    *,
    spec: PostgresDocumentSchemaSpec,
    write_column_types: dict[str, PostgresType],
) -> None:
    if spec.tenant_aware or spec.write_relation is None:
        return

    if TENANT_ID_FIELD not in write_column_types:
        return

    schema, relation = spec.write_relation
    logger.warning(
        "Postgres schema validation for document %r: write relation %s.%s has "
        "column %r but tenant_aware=False — row-level isolation is disabled; "
        "confirm wiring or enable tenant_aware.",
        spec.name,
        schema,
        relation,
        TENANT_ID_FIELD,
    )


async def validate_postgres_document_schemas(
    introspector: PostgresIntrospector,
    specs: Sequence[PostgresDocumentSchemaSpec],
) -> None:
    """Assert each spec's relations expose the columns implied by the Pydantic models."""

    for spec in specs:
        read_need = frozenset(
            pydantic_field_names(spec.read_model, include_computed=False)
            - spec.read_omit_fields,
        )
        await _require_columns(
            introspector,
            schema=spec.read_relation[0],
            relation=spec.read_relation[1],
            required=read_need,
            label=f"{spec.name} read",
        )

        await _validate_relation_models(
            introspector,
            schema=spec.read_relation[0],
            relation=spec.read_relation[1],
            models=[
                (
                    spec.read_model,
                    spec.read_omit_fields,
                    f"{spec.name} read",
                ),
            ],
        )

        if spec.write_relation is not None:
            if spec.write_domain_model is None or spec.write_create_model is None:
                raise exc.internal(
                    f"Document {spec.name!r}: write_relation requires "
                    "write_domain_model and write_create_model.",
                )

            write_need = (
                _write_field_names_union(
                    spec.write_domain_model,
                    spec.write_create_model,
                    spec.write_update_model,
                )
                - spec.write_omit_fields
            )

            if spec.tenant_aware:
                write_need = write_need | {TENANT_ID_FIELD}

            await _require_columns(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                required=write_need,
                label=f"{spec.name} write",
            )

            write_models: list[tuple[type[BaseModel], frozenset[str], str]] = [
                (
                    spec.write_domain_model,
                    spec.write_omit_fields,
                    f"{spec.name} write domain",
                ),
                (
                    spec.write_create_model,
                    spec.write_omit_fields,
                    f"{spec.name} write create",
                ),
            ]

            if spec.write_update_model is not None:
                write_models.append(
                    (
                        spec.write_update_model,
                        spec.write_omit_fields,
                        f"{spec.name} write update",
                    ),
                )

            await _validate_relation_models(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                models=write_models,
            )

            if spec.tenant_aware:
                await _validate_tenant_column(
                    introspector,
                    schema=spec.write_relation[0],
                    relation=spec.write_relation[1],
                    label=f"{spec.name} write",
                )

            write_types = await introspector.get_column_types(
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
            )
            _warn_read_not_subset_of_write(spec=spec, write_column_types=write_types)
            _warn_unused_tenant_column(spec=spec, write_column_types=write_types)

            await resolve_write_conflict_target(
                introspector,
                schema=spec.write_relation[0],
                relation=spec.write_relation[1],
                configured=spec.conflict_target,
                document_label=spec.name,
            )

        if spec.history_relation is not None:
            hist_need = (
                frozenset(pydantic_field_names(DocumentHistory, include_computed=False))
                - spec.history_omit_fields
            )

            if spec.tenant_aware:
                hist_need = hist_need | {TENANT_ID_FIELD}

            await _require_columns(
                introspector,
                schema=spec.history_relation[0],
                relation=spec.history_relation[1],
                required=hist_need,
                label=f"{spec.name} history",
            )

            await _validate_relation_models(
                introspector,
                schema=spec.history_relation[0],
                relation=spec.history_relation[1],
                models=[
                    (
                        DocumentHistory,
                        spec.history_omit_fields,
                        f"{spec.name} history",
                    ),
                ],
            )

            if spec.tenant_aware:
                await _validate_tenant_column(
                    introspector,
                    schema=spec.history_relation[0],
                    relation=spec.history_relation[1],
                    label=f"{spec.name} history",
                )
