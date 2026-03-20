"""Gateway factory helpers for building Mongo read, write, and history gateways."""

from typing import Any

from forze.application.contracts.document import (
    DocumentHistorySpec,
    DocumentReadSpec,
    DocumentWriteSpec,
)
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
    spec: DocumentReadSpec[Any],
) -> MongoReadGateway[Any]:
    """Build a read gateway for a source and model."""
    client = ctx.dep(MongoClientDepKey)

    return MongoReadGateway(source=spec["source"], client=client, model=spec["model"])


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    spec: DocumentHistorySpec,
    write_spec: DocumentWriteSpec[Any, Any, Any],
    history_write_strategy: MongoHistoryWriteStrategy = "application",
) -> MongoHistoryGateway[Any]:
    """Build a history gateway for document audit trails."""
    client = ctx.dep(MongoClientDepKey)

    return MongoHistoryGateway(
        source=spec["source"],
        target_source=write_spec["source"],
        strategy=history_write_strategy,
        client=client,
        model=write_spec["models"]["domain"],
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    spec: DocumentWriteSpec[Any, Any, Any],
    history_spec: DocumentHistorySpec | None = None,
    *,
    rev_bump_strategy: MongoRevBumpStrategy = "application",
    history_write_strategy: MongoHistoryWriteStrategy = "application",
) -> MongoWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history."""
    client = ctx.dep(MongoClientDepKey)
    read = read_gw(ctx, {"source": spec["source"], "model": spec["models"]["domain"]})
    hist = None

    if history_spec:
        hist = _doc_history_gw(
            ctx,
            history_spec,
            spec,
            history_write_strategy,
        )

    return MongoWriteGateway(
        source=spec["source"],
        client=client,
        model=spec["models"]["domain"],
        read=read,
        create_dto=spec["models"]["create_cmd"],
        update_dto=spec["models"]["update_cmd"],
        history=hist,
        rev_bump_strategy=rev_bump_strategy,
    )
