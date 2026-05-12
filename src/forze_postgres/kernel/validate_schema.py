"""Compare Pydantic document shapes to Postgres relation columns (startup validation)."""

from collections.abc import Sequence

import attrs
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import TENANT_ID_FIELD
from forze.domain.models import DocumentHistory

from .introspect import PostgresIntrospector

# ----------------------- #


def _write_field_names_union(
    domain: type[BaseModel],
    create: type[BaseModel],
    update: type[BaseModel] | None,
) -> frozenset[str]:
    names = pydantic_field_names(domain) | pydantic_field_names(create)

    if update is not None:
        names |= pydantic_field_names(update)

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
    """When ``True``, the write relation must expose :data:`~forze.domain.constants.TENANT_ID_FIELD`."""

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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.history_enabled and self.history_relation is None:
            raise CoreError(
                f"Document {self.name!r}: history_enabled requires history_relation.",
            )

        if self.write_relation is not None:
            if self.write_domain_model is None or self.write_create_model is None:
                raise CoreError(
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
        raise CoreError(
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

        if spec.write_relation is not None:
            if spec.write_domain_model is None or spec.write_create_model is None:
                raise CoreError(
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
