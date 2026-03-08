"""Gateway factory helpers for building Mongo read, write, and history gateways."""

from typing import Any, Optional

from forze.application.contracts.document import DocumentModelSpec
from forze.application.execution import ExecutionContext

from ...kernel.gateways import (
    MongoHistoryGateway,
    MongoHistoryWriteStrategy,
    MongoReadGateway,
    MongoRevBumpStrategy,
    MongoWriteGateway,
)
from .keys import MongoClientDepKey

# ----------------------- #


def read_gw(
    ctx: ExecutionContext,
    source: str,
    model: type[Any],
):
    """Build a read gateway for a source and model."""
    client = ctx.dep(MongoClientDepKey)

    return MongoReadGateway(source=source, client=client, model=model)


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    source: str,
    write_source: str,
    model: type[Any],
    history_write_strategy: MongoHistoryWriteStrategy = "application",
):
    """Build a history gateway for document audit trails."""
    client = ctx.dep(MongoClientDepKey)

    return MongoHistoryGateway(
        source=source,
        target_source=write_source,
        strategy=history_write_strategy,
        client=client,
        model=model,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    source: str,
    models: DocumentModelSpec[Any, Any, Any, Any],
    history_source: Optional[str] = None,
    *,
    rev_bump_strategy: MongoRevBumpStrategy = "application",
    history_write_strategy: MongoHistoryWriteStrategy = "application",
):
    """Build a write gateway for document CRUD with optional history."""
    client = ctx.dep(MongoClientDepKey)
    read = read_gw(ctx, source, models["domain"])
    hist = None

    if history_source:
        hist = _doc_history_gw(
            ctx,
            history_source,
            source,
            models["domain"],
            history_write_strategy,
        )

    return MongoWriteGateway(
        source=source,
        client=client,
        model=models["domain"],
        read=read,
        create_dto=models["create_cmd"],
        update_dto=models["update_cmd"],
        history=hist,
        rev_bump_strategy=rev_bump_strategy,
    )
