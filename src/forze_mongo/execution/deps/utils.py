"""Gateway factory helpers for building Mongo read, write, and history gateways."""

from typing import Any, Literal

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.contracts.resolution import RelationSpec
from forze.application.execution import ExecutionContext
from forze.base.serialization import PydanticRecordMappingCodec, RecordMappingCodec

from ...kernel.gateways import MongoHistoryGateway, MongoReadGateway, MongoWriteGateway
from .keys import MongoClientDepKey

# ----------------------- #

DocWriteTypes = DocumentWriteTypes[Any, Any, Any]

# ....................... #


def read_gw(
    ctx: ExecutionContext,
    *,
    read_type: type[Any],
    read_relation: RelationSpec,
    tenant_aware: bool,
    row_codec: RecordMappingCodec[Any, Any] | None = None,
    read_validation: Literal["strict", "trusted"] = "strict",
) -> MongoReadGateway[Any]:
    """Build a read gateway for a source and model."""
    client = ctx.deps.provide(MongoClientDepKey)

    codec = (
        row_codec
        if row_codec is not None
        else PydanticRecordMappingCodec(read_type)
    )

    return MongoReadGateway(
        relation=read_relation,
        client=client,
        model_type=read_type,
        row_codec=codec,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        read_validation=read_validation,
    )


# ....................... #


def _doc_history_gw(
    ctx: ExecutionContext,
    *,
    domain_type: type[Any],
    history_relation: RelationSpec,
    write_relation: RelationSpec,
    tenant_aware: bool,
) -> MongoHistoryGateway[Any]:
    """Build a history gateway for document audit trails."""

    client = ctx.deps.provide(MongoClientDepKey)

    return MongoHistoryGateway(
        relation=history_relation,
        target_relation=write_relation,
        client=client,
        model_type=domain_type,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )


# ....................... #


def doc_write_gw(
    ctx: ExecutionContext,
    *,
    write_types: DocWriteTypes,
    write_relation: RelationSpec,
    history_relation: RelationSpec | None = None,
    history_enabled: bool = False,
    tenant_aware: bool,
) -> MongoWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history."""
    client = ctx.deps.provide(MongoClientDepKey)

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
        relation=write_relation,
        client=client,
        model_type=write_types["domain"],
        create_cmd_type=write_types["create_cmd"],
        update_cmd_type=write_types.get("update_cmd"),
        read_gw=read,
        history_gw=hist,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )
