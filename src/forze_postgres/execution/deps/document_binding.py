"""The schema spec of one document binding: what startup validation checks, and what
``forze schema`` renders a framework-owned table from (RFC 0080 §5.7)."""

from collections.abc import Mapping
from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.base.exceptions import exc

from ...kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec
from ...kernel.relation import require_static_relation
from .configs import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig

# ----------------------- #


def postgres_document_schema_spec_for_binding(
    name: str,
    *,
    spec: DocumentSpec[Any, Any, Any, Any],
    config: PostgresReadOnlyDocumentConfig | PostgresDocumentConfig,
    warn_undeclared: bool = True,
    bound_relations: Mapping[str, tuple[str, str]] | None = None,
) -> PostgresDocumentSchemaSpec:
    """Build a :class:`~forze_postgres.kernel.catalog.validation.validate_schema.PostgresDocumentSchemaSpec` from kernel wiring.

    Dynamic :class:`~forze_postgres.kernel.relation.RelationSpec` resolvers are rejected
    (see :func:`~forze_postgres.kernel.relation.require_static_relation`) because startup
    validation introspects fixed relation names.

    A read-only binding validates the spec's read side alone, even when the spec has a write
    side another service writes through.

    :param warn_undeclared: Whether to report unique indexes no guarantee declares. Pass
        ``False`` for a spec the framework owns, such as a ``forze_identity`` one.
    :param bound_relations: The relation of each document the same module binds, by route name,
        where a declared ``Reference`` must point its foreign key. A spec declaring one is
        refused at validation when the spec it names is missing here.
    :raises CoreException: ``configuration``, code ``postgres_document_spec_read_only``, when a
        read-write binding pairs with a spec that has no write model.
    """

    sealed = spec.encryption.sealed if spec.encryption is not None else frozenset[str]()
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

    # A read-only binding reads the spec's read side alone, whatever else the spec declares;
    # what the write domain model implies binds the writer's table, not this relation.
    if not isinstance(config, PostgresDocumentConfig) or spec.write is None:
        return PostgresDocumentSchemaSpec(
            name=name,
            read_model=spec.read,
            read_relation=read_rel,
            read_omit_fields=spec.resolved_lenient_read_fields,
            tenant_aware=tenant_aware,
            materialized=spec.materialized,
            guarantees=spec.declared_guarantees,
            warn_undeclared=warn_undeclared,
            bound_relations=bound_relations or {},
            sealed_fields=sealed,
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
        bound_relations=bound_relations or {},
        sealed_fields=sealed,
        required_indexes=spec.required_indexes,
    )
