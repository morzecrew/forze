from typing import Any, Optional

from forze.application.kernel.context import ExecutionContext
from forze.application.kernel.specs import DocumentModelSpec, DocumentSearchSpec

from ..kernel.gateways import (
    PostgresHistoryGateway,
    PostgresReadGateway,
    PostgresSearchGateway,
    PostgresSearchIndexSpec,
    PostgresTableSpec,
    PostgresWriteGateway,
)
from .keys import PostgresClientDepKey, PostgresTypesProviderDepKey

# ----------------------- #


def read_gw(ctx: ExecutionContext, relation: str, model: type[Any]):
    client = ctx.dep(PostgresClientDepKey)
    types_provider = ctx.dep(PostgresTypesProviderDepKey)

    return PostgresReadGateway(
        spec=PostgresTableSpec.from_relation(relation),
        client=client,
        model=model,
        types_provider=types_provider,
    )


# ....................... #


def _doc_history_gw(ctx: ExecutionContext, relation: str, model: type[Any]):
    client = ctx.dep(PostgresClientDepKey)
    types_provider = ctx.dep(PostgresTypesProviderDepKey)

    return PostgresHistoryGateway(
        spec=PostgresTableSpec.from_relation(relation),
        client=client,
        model=model,
        types_provider=types_provider,
    )


# ....................... #


def doc_search_gw(
    ctx: ExecutionContext,
    relation: str,
    model: type[Any],
    search: DocumentSearchSpec,
):
    client = ctx.dep(PostgresClientDepKey)
    types_provider = ctx.dep(PostgresTypesProviderDepKey)

    return PostgresSearchGateway(
        spec=PostgresTableSpec.from_relation(relation),
        client=client,
        model=model,
        types_provider=types_provider,
        indexes=PostgresSearchIndexSpec.from_dict(search),
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    relation: str,
    models: DocumentModelSpec[Any, Any, Any, Any],
    history_relation: Optional[str] = None,
):
    client = ctx.dep(PostgresClientDepKey)
    types_provider = ctx.dep(PostgresTypesProviderDepKey)

    read = read_gw(ctx, relation, models["domain"])
    hist = None

    if history_relation:
        hist = _doc_history_gw(ctx, history_relation, models["domain"])

    return PostgresWriteGateway(
        spec=PostgresTableSpec.from_relation(relation),
        client=client,
        types_provider=types_provider,
        read=read,
        model=models["domain"],
        create_dto=models["create_cmd"],
        update_dto=models["update_cmd"],
        history=hist,
    )
