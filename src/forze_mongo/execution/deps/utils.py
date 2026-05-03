"""Gateway factory helpers for building Mongo read, write, and history gateways."""

from typing import Any

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import ExecutionContext

from ...kernel.gateways import MongoHistoryGateway, MongoReadGateway, MongoWriteGateway
from .keys import MongoClientDepKey

# ----------------------- #

DocWriteTypes = DocumentWriteTypes[Any, Any, Any]

# ....................... #


def read_gw(
    ctx: ExecutionContext,
    *,
    read_type: type[Any],
    read_relation: tuple[str, str],
    tenant_aware: bool,
) -> MongoReadGateway[Any]:
    """Build a read gateway for a source and model."""
    client = ctx.dep(MongoClientDepKey)

    return MongoReadGateway(
        database=read_relation[0],
        collection=read_relation[1],
        client=client,
        model_type=read_type,
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
    tenant_aware: bool,
) -> MongoHistoryGateway[Any]:
    """Build a history gateway for document audit trails."""

    client = ctx.dep(MongoClientDepKey)

    return MongoHistoryGateway(
        database=history_relation[0],
        collection=history_relation[1],
        target_database=write_relation[0],
        target_collection=write_relation[1],
        client=client,
        model_type=domain_type,
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
    tenant_aware: bool,
) -> MongoWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history."""
    client = ctx.dep(MongoClientDepKey)

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
            tenant_aware=tenant_aware,
        )

    return MongoWriteGateway(
        database=write_relation[0],
        collection=write_relation[1],
        client=client,
        model_type=write_types["domain"],
        create_cmd_type=write_types["create_cmd"],
        update_cmd_type=write_types.get("update_cmd"),
        read_gw=read,
        history_gw=hist,
        tenant_provider=ctx.get_tenant_id,
        tenant_aware=tenant_aware,
    )
