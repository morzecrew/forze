"""Gateway factory helpers for Firestore read, write, and history gateways."""

from typing import Any, Literal

from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentWriteTypes,
    document_codecs_for_write_types,
)
from forze.application.contracts.resolution import RelationSpec
from forze.application.execution import ExecutionContext, resolve_resilience_executor
from forze.base.serialization import ModelCodec, default_model_codec

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
    codec: ModelCodec[Any, Any] | None = None,
    read_validation: Literal["strict", "trusted"] = "strict",
    lenient_read_fields: frozenset[str] = frozenset(),
    sealed_fields: frozenset[str] = frozenset(),
) -> FirestoreReadGateway[Any]:
    client = ctx.deps.provide(FirestoreClientDepKey)

    resolved_codec = codec if codec is not None else default_model_codec(read_type)

    return FirestoreReadGateway(
        relation=read_relation,
        client=client,
        model_type=read_type,
        codec=resolved_codec,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        read_validation=read_validation,
        lenient_read_fields=lenient_read_fields,
        sealed_fields=sealed_fields,
    )


def _doc_history_gw(
    ctx: ExecutionContext,
    *,
    domain_type: type[Any],
    domain_codec: ModelCodec[Any, Any],
    history_relation: RelationSpec,
    write_relation: RelationSpec,
    tenant_aware: bool,
    history_codec: ModelCodec[Any, Any],
) -> FirestoreHistoryGateway[Any]:
    client = ctx.deps.provide(FirestoreClientDepKey)

    return FirestoreHistoryGateway(
        relation=history_relation,
        target_relation=write_relation,
        client=client,
        model_type=domain_type,
        codec=domain_codec,
        history_codec=history_codec,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
    )


def doc_write_gw(
    ctx: ExecutionContext,
    *,
    write_types: DocWriteTypes,
    write_relation: RelationSpec,
    codecs: DocumentCodecs[Any, Any, Any, Any] | None = None,
    history_relation: RelationSpec | None = None,
    history_enabled: bool = False,
    tenant_aware: bool,
    write_omit_fields: frozenset[str] = frozenset(),
) -> FirestoreWriteGateway[Any, Any, Any]:
    """Build a write gateway for document CRUD with optional history.

    When ``codecs`` is omitted, codecs are derived via
    :func:`~forze.application.contracts.document.document_codecs_for_write_types`
    (tests/direct helpers; production passes ``spec.resolved_codecs``).
    """

    client = ctx.deps.provide(FirestoreClientDepKey)

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
        codec=domain_codec,
        lenient_read_fields=write_omit_fields,
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
            tenant_aware=tenant_aware,
            history_codec=history_codec,
        )

    return FirestoreWriteGateway(
        relation=write_relation,
        client=client,
        model_type=write_types["domain"],
        codec=domain_codec,
        lenient_read_fields=write_omit_fields,
        write_omit_fields=write_omit_fields,
        create_cmd_type=write_types["create_cmd"],
        update_cmd_type=write_types.get("update_cmd"),
        read_gw=read,
        create_codec=create_codec,
        update_codec=resolved_codecs.update,
        history_gw=hist,
        tenant_provider=ctx.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        resilience=resolve_resilience_executor(ctx),
    )
