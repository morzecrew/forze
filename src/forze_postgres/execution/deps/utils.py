"""Gateway factory helpers for building Postgres read, write, search, and history gateways."""

from typing import Any, Optional

from forze.application.contracts.document import (
    DocumentHistorySpec,
    DocumentReadSpec,
    DocumentWriteSpec,
)
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


def read_gw(
    ctx: ExecutionContext, spec: DocumentReadSpec[Any]
) -> PostgresReadGateway[Any]:
    """Build a read gateway for a relation and model.

    :param ctx: Execution context for resolving client and types provider.
    :param spec: Document read specification.
    :returns: Postgres read gateway.
    """

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresReadGateway(
        qname=PostgresQualifiedName.from_string(spec["source"]),
        client=client,
        model=spec["model"],
        introspector=introspector,
    )


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    spec: DocumentHistorySpec,
    write_spec: DocumentWriteSpec[Any, Any, Any],
    history_write_strategy: PostgresHistoryWriteStrategy = "database",
) -> PostgresHistoryGateway[Any]:
    """Build a history gateway for document audit trails."""

    client = ctx.dep(PostgresClientDepKey)
    introspector = ctx.dep(PostgresIntrospectorDepKey)

    return PostgresHistoryGateway(
        qname=PostgresQualifiedName.from_string(spec["source"]),
        target_qname=PostgresQualifiedName.from_string(write_spec["source"]),
        strategy=history_write_strategy,
        client=client,
        model=write_spec["models"]["domain"],
        introspector=introspector,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    spec: DocumentWriteSpec[Any, Any, Any],
    history_spec: Optional[DocumentHistorySpec] = None,
    *,
    rev_bump_strategy: PostgresRevBumpStrategy = "database",
    history_write_strategy: PostgresHistoryWriteStrategy = "database",
) -> PostgresWriteGateway[Any, Any, Any]:
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

    read = read_gw(ctx, {"source": spec["source"], "model": spec["models"]["domain"]})
    hist = None

    if history_spec:
        hist = _doc_history_gw(
            ctx,
            history_spec,
            spec,
            history_write_strategy,
        )

    return PostgresWriteGateway(
        qname=PostgresQualifiedName.from_string(spec["source"]),
        client=client,
        introspector=introspector,
        read=read,
        model=spec["models"]["domain"],
        create_dto=spec["models"]["create_cmd"],
        update_dto=spec["models"]["update_cmd"],
        history=hist,
        rev_bump_strategy=rev_bump_strategy,
    )
