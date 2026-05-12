"""Lifecycle wiring for optional Postgres document schema validation."""

from collections.abc import Sequence
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import ExecutionContext, LifecycleHook, LifecycleStep
from forze.base.errors import CoreError

from ..kernel.validate_schema import (
    PostgresDocumentSchemaSpec,
    validate_postgres_document_schemas,
)
from .deps.configs import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig
from .deps.keys import PostgresIntrospectorDepKey

# ----------------------- #


def postgres_document_schema_spec_for_binding(
    name: str,
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    config: PostgresReadOnlyDocumentConfig | PostgresDocumentConfig,
) -> PostgresDocumentSchemaSpec:
    """Build a :class:`~forze_postgres.kernel.validate_schema.PostgresDocumentSchemaSpec` from kernel wiring."""

    read_rel = config["read"]
    tenant_aware = config.get("tenant_aware", False)

    if spec.write is None:
        return PostgresDocumentSchemaSpec(
            name=name,
            read_model=spec.read,
            read_relation=read_rel,
            tenant_aware=tenant_aware,
        )

    if "write" not in config:
        raise CoreError(
            f"Document {name!r} has write spec but config is not a Postgres document config "
            "(missing 'write' relation).",
        )

    w = spec.write
    hist: tuple[str, str] | None = None

    if spec.history_enabled:
        if "history" in config:
            hist = config["history"]  # type: ignore[typeddict-item]

        if hist is None:
            raise CoreError(
                f"Document {name!r}: history_enabled but PostgresDocumentConfig "
                "has no 'history' relation.",
            )

    return PostgresDocumentSchemaSpec(
        name=name,
        read_model=spec.read,
        read_relation=read_rel,
        tenant_aware=tenant_aware,
        write_domain_model=w["domain"],
        write_create_model=w["create_cmd"],
        write_update_model=w.get("update_cmd"),
        write_relation=config.get("write"),  # type: ignore[arg-type]
        history_enabled=spec.history_enabled,
        history_relation=hist,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDocumentSchemaValidationHook(LifecycleHook):
    """Startup hook that validates document relations against Pydantic models."""

    specs: Sequence[PostgresDocumentSchemaSpec]

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        introspector = ctx.dep(PostgresIntrospectorDepKey)

        try:
            await validate_postgres_document_schemas(introspector, self.specs)

        except CoreError as e:
            if getattr(e, "code", None) == "introspection_partition_required":
                logger.trace(
                    "Postgres document schema validation skipped "
                    "(introspector partition unavailable)",
                )
                return

            raise


def postgres_document_schema_validation_lifecycle_step(
    name: str = "postgres_document_schema_validate",
    *,
    specs: Sequence[PostgresDocumentSchemaSpec],
) -> LifecycleStep:
    """Build a lifecycle step that validates document read/write/history columns.

    Run after the Postgres client lifecycle step (and typically after catalog
    warmup). With ``introspector_cache_partition_key`` set and no tenant during
    startup, validation is skipped (trace log only).

    :param name: Unique step name.
    :param specs: One spec per document to validate (see
        :func:`postgres_document_schema_spec_for_binding`).
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        name=name,
        startup=PostgresDocumentSchemaValidationHook(specs=tuple(specs)),
    )
