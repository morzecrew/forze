"""Lifecycle wiring for optional Postgres document schema validation."""

from typing import Any, Sequence, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc

from ...kernel.catalog.validation.validate_bookkeeping import (
    PostgresDocumentBookkeepingSpec,
    validate_postgres_document_bookkeeping,
)
from ...kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
    validate_postgres_document_schemas,
)
from ...kernel.relation import require_static_relation
from ..deps.configs import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig
from ..deps.keys import PostgresIntrospectorDepKey
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #


def postgres_document_schema_spec_for_binding(
    name: str,
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    config: PostgresReadOnlyDocumentConfig | PostgresDocumentConfig,
) -> PostgresDocumentSchemaSpec:
    """Build a :class:`~forze_postgres.kernel.catalog.validation.validate_schema.PostgresDocumentSchemaSpec` from kernel wiring.

    Dynamic :class:`~forze_postgres.kernel.relation.RelationSpec` resolvers are rejected
    (see :func:`~forze_postgres.kernel.relation.require_static_relation`) because startup
    validation introspects fixed relation names.
    """

    read_rel = require_static_relation(
        config.read,
        document_name=name,
        field="read",
    )
    tenant_aware = config.tenant_aware

    if spec.write is None:
        return PostgresDocumentSchemaSpec(
            name=name,
            read_model=spec.read,
            read_relation=read_rel,
            read_omit_fields=spec.lenient_read_fields,
            tenant_aware=tenant_aware,
            materialized=spec.materialized,
        )

    if not isinstance(config, PostgresDocumentConfig):
        raise exc.internal(
            f"Document {name!r} has write spec but config is not a PostgresDocumentConfig.",
        )

    w = spec.write
    hist: tuple[str, str] | None = None

    if spec.history_enabled:
        if config.history is None:
            hist = None
        else:
            hist = require_static_relation(
                config.history,
                document_name=name,
                field="history",
            )

        if hist is None:
            raise exc.internal(
                f"Document {name!r}: history_enabled but PostgresDocumentConfig "
                "has no 'history' relation.",
            )

    return PostgresDocumentSchemaSpec(
        name=name,
        read_model=spec.read,
        read_relation=read_rel,
        read_omit_fields=spec.lenient_read_fields,
        tenant_aware=tenant_aware,
        write_domain_model=w["domain"],
        write_create_model=w["create_cmd"],
        write_update_model=w.get("update_cmd"),
        write_relation=require_static_relation(
            config.write,
            document_name=name,
            field="write",
        ),
        history_enabled=spec.history_enabled,
        history_relation=hist,
        bookkeeping_strategy=config.bookkeeping_strategy,
        conflict_target=config.conflict_target,
        materialized=spec.materialized,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresDocumentSchemaValidationHook(LifecycleHook):
    """Startup hook that validates document relations against Pydantic models."""

    specs: Sequence[PostgresDocumentSchemaSpec]

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        introspector = ctx.deps.provide(PostgresIntrospectorDepKey)

        try:
            await validate_postgres_document_schemas(introspector, self.specs)

            bookkeeping_specs = [
                PostgresDocumentBookkeepingSpec(
                    name=s.name,
                    bookkeeping_strategy=s.bookkeeping_strategy,  # type: ignore[arg-type]
                    write_relation=s.write_relation,  # type: ignore[arg-type]
                    history_enabled=s.history_enabled,
                )
                for s in self.specs
                if s.write_relation is not None and s.bookkeeping_strategy is not None
            ]

            if bookkeeping_specs:
                await validate_postgres_document_bookkeeping(
                    introspector,
                    bookkeeping_specs,
                )

        except exc as e:
            if getattr(e, "code", None) == "introspection_partition_required":
                logger.trace(
                    "Postgres document schema validation skipped "
                    "(introspector partition unavailable)",
                )
                return

            raise


# ....................... #


def postgres_document_schema_validation_lifecycle_step(
    name: str = "postgres_document_schema_validate",
    *,
    specs: Sequence[PostgresDocumentSchemaSpec],
) -> LifecycleStep:
    """Build a lifecycle step that validates document read/write/history columns.

    Requires :data:`~forze_postgres.execution.lifecycle.capabilities.POSTGRES_CLIENT_CAPABILITY`.
    With ``introspector_cache_partition_key`` set and no tenant during startup,
    validation is skipped (trace log only).

    :param name: Unique step name.
    :param specs: One spec per document to validate (see
        :func:`postgres_document_schema_spec_for_binding`).
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        id=name,
        startup=PostgresDocumentSchemaValidationHook(specs=tuple(specs)),
        requires=(POSTGRES_CLIENT_CAPABILITY,),
    )
