"""Gateway factory helpers for building Postgres read, write, search, and history gateways."""

from typing import Any, Optional

from forze.application.contracts.document import DocumentModelSpec
from forze.application.execution import ExecutionContext

from ...kernel.gateways import (
    PostgresHistoryGateway,
    PostgresHistoryWriteStrategy,
    PostgresQualifiedName,
    PostgresReadGateway,
    PostgresRevBumpStrategy,
    PostgresWriteGateway,
)
from .keys import PostgresClientDepKey, PostgresIntrospectorDepKey

# ----------------------- #


def read_gw(ctx: ExecutionContext, relation: str, model: type[Any]):
    """Build a read gateway for a relation and model.

    :param ctx: Execution context for resolving client and types provider.
    :param relation: Table or view name.
    :param model: Pydantic model for row validation.
    :returns: Postgres read gateway.
    """
    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresReadGateway(
        qname=PostgresQualifiedName.from_string(relation),
        client=client,
        model=model,
        introspector=introspector,
    )


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    relation: str,
    write_relation: str,
    model: type[Any],
    history_write_strategy: PostgresHistoryWriteStrategy = "database",
):
    """Build a history gateway for document audit trails."""

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresHistoryGateway(
        qname=PostgresQualifiedName.from_string(relation),
        target_qname=PostgresQualifiedName.from_string(write_relation),
        strategy=history_write_strategy,
        client=client,
        model=model,
        introspector=introspector,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    relation: str,
    models: DocumentModelSpec[Any, Any, Any, Any],
    history_relation: Optional[str] = None,
    *,
    rev_bump_strategy: PostgresRevBumpStrategy = "database",
    history_write_strategy: PostgresHistoryWriteStrategy = "database",
):
    """Build a write gateway for document CRUD with optional history.

    :param ctx: Execution context.
    :param relation: Write table name.
    :param models: Document model spec (domain, create_cmd, update_cmd).
    :param history_relation: Optional history table name.
    :param rev_bump_strategy: Revision bump strategy.
    :param history_write_strategy: History write strategy.
    :returns: Postgres write gateway.
    """
    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    read = read_gw(ctx, relation, models["domain"])
    hist = None

    if history_relation:
        hist = _doc_history_gw(
            ctx,
            history_relation,
            relation,
            models["domain"],
            history_write_strategy,
        )

    return PostgresWriteGateway(
        qname=PostgresQualifiedName.from_string(relation),
        client=client,
        introspector=introspector,
        read=read,
        model=models["domain"],
        create_dto=models["create_cmd"],
        update_dto=models["update_cmd"],
        history=hist,
        rev_bump_strategy=rev_bump_strategy,
    )
