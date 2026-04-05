"""Gateway factory helpers for building Postgres read, write, search, and history gateways."""

from typing import Any

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import ExecutionContext

from ...kernel.gateways import (
    PostgresBookkeepingStrategy,
    PostgresHistoryGateway,
    PostgresQualifiedName,
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
    read_relation: tuple[str, str],
    tenant_aware: bool,
) -> PostgresReadGateway[Any]:
    """Build a read gateway for a relation and model.

    :param ctx: Execution context for resolving client and types provider.
    :param read_type: Read type.
    :param read_relation: Read table name.
    :param tenant_aware: Whether the document is tenant-aware.
    :returns: Postgres read gateway.
    """

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresReadGateway(
        qname=PostgresQualifiedName(
            schema=read_relation[0],
            name=read_relation[1],
        ),
        client=client,
        model_type=read_type,
        introspector=introspector,
        tenant_provider=ctx.get_tenant_id,
        tenant_aware=tenant_aware,
    )


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    *,
    domain_type: type[Any],
    history_relation: tuple[str, str],
    write_relation: tuple[str, str],
    bookkeeping_strategy: PostgresBookkeepingStrategy,
    tenant_aware: bool,
) -> PostgresHistoryGateway[Any]:
    """Build a history gateway for document audit trails.

    :param ctx: Execution context.
    :param domain_type: Domain type.
    :param history_relation: History table name.
    :param write_relation: Write table name.
    :param bookkeeping_strategy: Bookkeeping strategy.
    :param tenant_aware: Whether the document is tenant-aware.
    :returns: Postgres history gateway.
    """

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresHistoryGateway(
        qname=PostgresQualifiedName(
            schema=history_relation[0],
            name=history_relation[1],
        ),
        target_qname=PostgresQualifiedName(
            schema=write_relation[0],
            name=write_relation[1],
        ),
        strategy=bookkeeping_strategy,
        client=client,
        model_type=domain_type,
        introspector=introspector,
        tenant_provider=ctx.get_tenant_id,
        tenant_aware=tenant_aware,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    *,
    write_types: DocWriteTypes,
    write_relation: tuple[str, str],
    history_relation: tuple[str, str] | None = None,
    history_enabled: bool = False,
    bookkeeping_strategy: PostgresBookkeepingStrategy,
    tenant_aware: bool,
) -> PostgresWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history.

    :param ctx: Execution context.
    :param write_types: Write types (domain, create_cmd, update_cmd).
    :param write_relation: Write table (schema, name).
    :param history_relation: Optional history table (schema, name).
    :param history_enabled: Whether to enable history.
    :param bookkeeping_strategy: Bookkeeping strategy.
    :param tenant_aware: Whether the document is tenant-aware.
    :returns: Postgres write gateway.
    """

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    read = read_gw(
        ctx,
        read_type=write_types["domain"],
        read_relation=write_relation,
        tenant_aware=tenant_aware,
    )
    hist = None

    if history_relation is not None and history_enabled:
        hist = _doc_history_gw(
            ctx,
            domain_type=write_types["domain"],
            history_relation=history_relation,
            write_relation=write_relation,
            bookkeeping_strategy=bookkeeping_strategy,
            tenant_aware=tenant_aware,
        )

    return PostgresWriteGateway(
        qname=PostgresQualifiedName(
            schema=write_relation[0],
            name=write_relation[1],
        ),
        client=client,
        introspector=introspector,
        read_gw=read,
        model_type=write_types["domain"],
        create_cmd_type=write_types["create_cmd"],
        update_cmd_type=write_types["update_cmd"],
        history_gw=hist,
        strategy=bookkeeping_strategy,
        tenant_provider=ctx.get_tenant_id,
        tenant_aware=tenant_aware,
    )
