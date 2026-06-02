"""Gateway factory helpers for building Postgres read, write, search, and history gateways."""

from typing import Any, Literal, Mapping

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentWriteTypes,
    document_codecs_for_write_types,
)
from forze.application.execution import ExecutionContext
from forze.base.serialization import ModelCodec
from forze_postgres.kernel.relation import RelationSpec

from ...kernel.gateways import (
    PostgresBookkeepingStrategy,
    PostgresHistoryGateway,
    PostgresReadGateway,
    PostgresWriteGateway,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #

DocWriteTypes = DocumentWriteTypes[Any, Any, Any]

# ....................... #


def read_gw(
    ctx: ExecutionContext,
    *,
    read_type: type[Any],
    read_relation: RelationSpec,
    tenant_aware: bool,
    nested_field_hints: Mapping[str, type[Any]] | None = None,
    codec: ModelCodec[Any, Any] | None = None,
    read_validation: Literal["strict", "trusted"] = "strict",
) -> PostgresReadGateway[Any]:
    """Build a read gateway for a relation and model.

    :param ctx: Execution context for resolving client and types provider.
    :param read_type: Read type.
    :param read_relation: Read table name or resolver.
    :param tenant_aware: Whether the document is tenant-aware.
    :returns: Postgres read gateway.
    """

    client = ctx.deps.provide(PostgresClientDepKey)
    introspector = ctx.deps.provide(PostgresIntrospectorDepKey)

    resolved_codec = codec if codec is not None else default_model_codec(read_type)

    return PostgresReadGateway(
        relation=read_relation,
        client=client,
        model_type=read_type,
        codec=resolved_codec,
        introspector=introspector,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        nested_field_hints=nested_field_hints,
        read_validation=read_validation,
    )


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    *,
    domain_type: type[Any],
    domain_codec: ModelCodec[Any, Any],
    history_relation: RelationSpec,
    write_relation: RelationSpec,
    bookkeeping_strategy: PostgresBookkeepingStrategy,
    tenant_aware: bool,
    history_codec: ModelCodec[Any, Any],
) -> PostgresHistoryGateway[Any]:
    """Build a history gateway for document audit trails.

    :param ctx: Execution context.
    :param domain_type: Domain type.
    :param history_relation: History table name or resolver.
    :param write_relation: Write table name or resolver.
    :param bookkeeping_strategy: Bookkeeping strategy.
    :param tenant_aware: Whether the document is tenant-aware.
    :returns: Postgres history gateway.
    """

    client = ctx.deps.provide(PostgresClientDepKey)
    introspector = ctx.deps.provide(PostgresIntrospectorDepKey)

    return PostgresHistoryGateway(
        relation=history_relation,
        target_relation=write_relation,
        strategy=bookkeeping_strategy,
        client=client,
        model_type=domain_type,
        codec=domain_codec,
        history_codec=history_codec,
        introspector=introspector,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    *,
    write_types: DocWriteTypes,
    write_relation: RelationSpec,
    codecs: DocumentCodecs[Any, Any, Any, Any] | None = None,
    history_relation: RelationSpec | None = None,
    history_enabled: bool = False,
    bookkeeping_strategy: PostgresBookkeepingStrategy,
    tenant_aware: bool,
    nested_field_hints: Mapping[str, type[Any]] | None = None,
    conflict_target: tuple[str, ...] | None = None,
) -> PostgresWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history.

    :param ctx: Execution context.
    :param write_types: Write types (domain, create_cmd, update_cmd).
    :param codecs: Document codec bundle from the spec. When omitted, codecs are
        derived via :func:`~forze.application.contracts.document.document_codecs_for_write_types`
        (intended for tests and direct helper use; production adapters pass
        ``spec.resolved_codecs``).
    :param write_relation: Write table (schema, name) or resolver.
    :param history_relation: Optional history table (schema, name) or resolver.
    :param history_enabled: Whether to enable history.
    :param bookkeeping_strategy: Bookkeeping strategy.
    :param tenant_aware: Whether the document is tenant-aware.
    :param conflict_target: Optional ``ON CONFLICT`` columns; ``None`` infers PRIMARY KEY.
    :returns: Postgres write gateway.
    """

    client = ctx.deps.provide(PostgresClientDepKey)
    introspector = ctx.deps.provide(PostgresIntrospectorDepKey)

    resolved_codecs = (
        codecs
        if codecs is not None
        else document_codecs_for_write_types(
            write_types,
            read=write_types["domain"],
            history_enabled=history_enabled,
        )
    )

    domain_codec = resolved_codecs.domain
    if domain_codec is None:
        msg = "Document write codecs require a domain codec"
        raise ValueError(msg)

    create_codec = resolved_codecs.create
    if create_codec is None:
        msg = "Document write codecs require a create codec"
        raise ValueError(msg)

    read = read_gw(
        ctx,
        read_type=write_types["domain"],
        read_relation=write_relation,
        tenant_aware=tenant_aware,
        nested_field_hints=nested_field_hints,
        codec=domain_codec,
    )
    hist = None

    if history_relation is not None and history_enabled:
        history_codec = resolved_codecs.history
        if history_codec is None:
            msg = "History is enabled but no history codec is configured on the spec"
            raise ValueError(msg)

        hist = _doc_history_gw(
            ctx,
            domain_type=write_types["domain"],
            domain_codec=domain_codec,
            history_relation=history_relation,
            write_relation=write_relation,
            bookkeeping_strategy=bookkeeping_strategy,
            tenant_aware=tenant_aware,
            history_codec=history_codec,
        )

    return PostgresWriteGateway(
        relation=write_relation,
        client=client,
        introspector=introspector,
        model_type=write_types["domain"],
        codec=domain_codec,
        read_gw=read,
        create_cmd_type=write_types["create_cmd"],
        update_cmd_type=write_types.get("update_cmd"),
        create_codec=create_codec,
        update_codec=resolved_codecs.update,
        history_gw=hist,
        strategy=bookkeeping_strategy,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        conflict_target=conflict_target,
    )
