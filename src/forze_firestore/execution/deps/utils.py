"""Gateway factory helpers for Firestore read, write, and history gateways."""

from typing import Any

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.contracts.resolution import RelationSpec
from forze.application.execution import ExecutionContext

from ...kernel.gateways import (
    FirestoreHistoryGateway,
    FirestoreReadGateway,
    FirestoreWriteGateway,
)
from .keys import FirestoreClientDepKey

DocWriteTypes = DocumentWriteTypes[Any, Any, Any]


def read_gw(
    ctx: ExecutionContext,
    *,
    read_type: type[Any],
    read_relation: RelationSpec,
    tenant_aware: bool,
) -> FirestoreReadGateway[Any]:
    client = ctx.deps.provide(FirestoreClientDepKey)

    return FirestoreReadGateway(
        relation=read_relation,
        client=client,
        model_type=read_type,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )


def _doc_history_gw(
    ctx: ExecutionContext,
    *,
    domain_type: type[Any],
    history_relation: RelationSpec,
    write_relation: RelationSpec,
    tenant_aware: bool,
) -> FirestoreHistoryGateway[Any]:
    client = ctx.deps.provide(FirestoreClientDepKey)

    return FirestoreHistoryGateway(
        relation=history_relation,
        target_relation=write_relation,
        client=client,
        model_type=domain_type,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )


def doc_write_gw(
    ctx: ExecutionContext,
    *,
    write_types: DocWriteTypes,
    write_relation: RelationSpec,
    history_relation: RelationSpec | None = None,
    history_enabled: bool = False,
    tenant_aware: bool,
) -> FirestoreWriteGateway[Any, Any, Any]:
    client = ctx.deps.provide(FirestoreClientDepKey)

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

    return FirestoreWriteGateway(
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
