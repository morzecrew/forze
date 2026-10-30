"""Lifecycle wiring for optional Postgres document schema validation."""

from collections.abc import Iterable, Sequence
from typing import Any, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecPlane, SpecSource
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
from ..deps.module import PostgresDepsModule
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #


def postgres_document_schema_spec_for_binding(
    name: str,
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    config: PostgresReadOnlyDocumentConfig | PostgresDocumentConfig,
    warn_undeclared: bool = True,
) -> PostgresDocumentSchemaSpec:
    """Build a :class:`~forze_postgres.kernel.catalog.validation.validate_schema.PostgresDocumentSchemaSpec` from kernel wiring.

    Dynamic :class:`~forze_postgres.kernel.relation.RelationSpec` resolvers are rejected
    (see :func:`~forze_postgres.kernel.relation.require_static_relation`) because startup
    validation introspects fixed relation names.

    A read-only binding validates the spec's read side alone, even when the spec has a write
    side another service writes through.

    :param warn_undeclared: Whether to report unique indexes no guarantee declares. Pass
        ``False`` for a spec the framework owns, such as a ``forze_identity`` one.
    :raises CoreException: ``configuration``, code ``postgres_document_spec_read_only``, when a
        read-write binding pairs with a spec that has no write model.
    """

    read_rel = require_static_relation(
        config.read,
        document_name=name,
        field="read",
    )
    tenant_aware = config.tenant_aware

    if isinstance(config, PostgresDocumentConfig) and spec.write is None:
        raise exc.configuration(
            f"Postgres document route {name!r} is bound read-write, but its spec has no write "
            "model, so nothing can write through it; bind it in ro_documents.",
            code="postgres_document_spec_read_only",
            details={"route": name},
        )

    # A read-only binding reads the spec's read side alone, whatever else the spec declares.
    if not isinstance(config, PostgresDocumentConfig) or spec.write is None:
        return PostgresDocumentSchemaSpec(
            name=name,
            read_model=spec.read,
            read_relation=read_rel,
            read_omit_fields=spec.resolved_lenient_read_fields,
            tenant_aware=tenant_aware,
            materialized=spec.materialized,
            guarantees=spec.guarantees,
            warn_undeclared=warn_undeclared,
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
        read_omit_fields=spec.resolved_lenient_read_fields,
        derived_read_fields=frozenset(spec.derived_read_fields),
        write_omit_fields=spec.write_omit_fields,
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
        guarantees=spec.guarantees,
        warn_undeclared=warn_undeclared,
    )


# ....................... #


def postgres_document_schema_specs(
    registry: FrozenSpecRegistry,
    deps: PostgresDepsModule,
    *,
    exclude: Iterable[str] = (),
) -> tuple[PostgresDocumentSchemaSpec, ...]:
    """Build a schema spec for every document *deps* binds, from the specs *registry* holds.

    Each route in :attr:`~PostgresDepsModule.ro_documents` and
    :attr:`~PostgresDepsModule.rw_documents` is paired with the catalogued
    :class:`DocumentSpec` of the same name through
    :func:`postgres_document_schema_spec_for_binding`, so a document added to the wiring is
    validated without anyone remembering to list it.

    A route using a dynamic :class:`~forze_postgres.kernel.relation.RelationSpec` is skipped
    with a warning naming it: its relation is known only per request, and one tenant-routed
    document must not stop every other one being checked. A bound route with no catalogued
    spec is refused instead — skipping it would leave it unvalidated with nothing said, which
    is the gap this exists to close. A spec catalogued as :attr:`SpecSource.FRAMEWORK` is not
    reported for unique indexes it does not declare: the application cannot change it.

    The reverse — a catalogued document no backend binds — is not checked here: inventory
    reconciliation (:func:`~forze.application.contracts.inventory.reconcile_specs`) refuses it
    when the runtime is built with a spec registry.

    :param registry: The application's spec inventory.
    :param deps: The module that binds the documents.
    :param exclude: Routes the application knowingly leaves unvalidated.
    :returns: One spec per validated document, ordered by route name.
    :raises CoreException: ``configuration``, code ``postgres_schema_spec_uncatalogued``,
        naming every bound route the registry has no document spec for; code
        ``postgres_document_route_duplicated`` when a route is both read-only and read-write.
    """

    # The deps module refuses this too, but only when it registers; here one config would win.
    if duplicated := sorted(
        str(name) for name in set(deps.ro_documents or {}) & set(deps.rw_documents or {})
    ):
        raise exc.configuration(
            f"Postgres document route(s) {duplicated} are bound both read-only and read-write.",
            code="postgres_document_route_duplicated",
            details={"routes": duplicated},
        )

    skipped = frozenset(exclude)
    bindings: dict[str, PostgresReadOnlyDocumentConfig | PostgresDocumentConfig] = {
        str(name): config
        for mapping in (deps.ro_documents, deps.rw_documents)
        for name, config in (mapping or {}).items()
        if str(name) not in skipped
    }
    specs: list[PostgresDocumentSchemaSpec] = []
    uncatalogued: list[str] = []

    for name in sorted(bindings):
        entry = registry.find(SpecPlane.DOCUMENT, name)

        if entry is None or not isinstance(entry.spec, DocumentSpec):
            uncatalogued.append(name)
            continue

        try:
            specs.append(
                postgres_document_schema_spec_for_binding(
                    name,
                    spec=entry.spec,  # pyright: ignore[reportUnknownArgumentType]
                    config=bindings[name],
                    warn_undeclared=entry.source is not SpecSource.FRAMEWORK,
                )
            )

        except exc as e:
            if getattr(e, "code", None) != "dynamic_relation_startup_validation":
                raise

            logger.warning(
                "Postgres document schema validation skips route %r: its %s relation is a "
                "dynamic RelationSpec, known only per request.",
                name,
                (e.details or {}).get("field"),
            )

    if uncatalogued:
        raise exc.configuration(
            f"Postgres binds document route(s) {uncatalogued} that the spec registry does not "
            "catalogue, so their schema cannot be validated. Register their DocumentSpec, or "
            "pass the routes in exclude= to leave them unvalidated.",
            code="postgres_schema_spec_uncatalogued",
            details={"routes": uncatalogued},
        )

    return tuple(specs)


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
    :param specs: One spec per document to validate — every bound document from
        :func:`postgres_document_schema_specs`, or hand-built with
        :func:`postgres_document_schema_spec_for_binding`.
    :returns: Lifecycle step with startup hook only.
    """

    return LifecycleStep(
        id=name,
        startup=PostgresDocumentSchemaValidationHook(specs=tuple(specs)),
        requires=(POSTGRES_CLIENT_CAPABILITY,),
    )
