from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from datetime import timedelta
from typing import Any, Callable

from fastapi import APIRouter

from forze.application.composition.document import DocumentDTOs
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import ExecutionContext, UsecaseRegistry

from .._logger import logger
from ..http import attach_http_endpoint
from .endpoints import (
    build_document_create_endpoint_spec,
    build_document_delete_endpoint_spec,
    build_document_get_by_number_id_endpoint_spec,
    build_document_get_endpoint_spec,
    build_document_kill_endpoint_spec,
    build_document_list_endpoint_spec,
    build_document_raw_list_endpoint_spec,
    build_document_restore_endpoint_spec,
    build_document_update_endpoint_spec,
)
from .specs import DocumentEndpointsSpec

# ----------------------- #
#! A bit damn function, but building a framework around is even more stupid.


def attach_document_endpoints(
    router: APIRouter,
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: DocumentEndpointsSpec | None = None,
    exclude_none: bool = True,
) -> APIRouter:
    endpoints = endpoints or {}
    config = endpoints.get("config", {})

    get_endpoint = endpoints.get("get_", {})
    get_by_number_id_endpoint = endpoints.get("get_by_number_id", {})
    list_endpoint = endpoints.get("list_", {})
    raw_list_endpoint = endpoints.get("raw_list", {})
    create_endpoint = endpoints.get("create", {})
    update_endpoint = endpoints.get("update", {})
    kill_endpoint = endpoints.get("kill", {})
    delete_endpoint = endpoints.get("delete", {})
    restore_endpoint = endpoints.get("restore", {})

    if not get_endpoint.get("disable", False):
        get_endpoint_spec = build_document_get_endpoint_spec(
            dtos=dtos,
            path_override=get_endpoint.get("path_override", None),
            metadata=get_endpoint.get("metadata", None),
            etag=config.get("enable_etag", False),
            etag_auto_304=config.get("etag_auto_304", False),
        )
        attach_http_endpoint(
            router=router,
            spec=get_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if not get_by_number_id_endpoint.get("disable", False):
        if not document.supports_number_id():
            logger.warning(
                "Number ID is not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            get_by_number_id_endpoint_spec = (
                build_document_get_by_number_id_endpoint_spec(
                    dtos=dtos,
                    path_override=get_by_number_id_endpoint.get("path_override", None),
                    metadata=get_by_number_id_endpoint.get("metadata", None),
                    etag=config.get("enable_etag", False),
                    etag_auto_304=config.get("etag_auto_304", False),
                )
            )
            attach_http_endpoint(
                router=router,
                spec=get_by_number_id_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if not list_endpoint.get("disable", False):
        list_endpoint_spec = build_document_list_endpoint_spec(
            dtos=dtos,
            path_override=list_endpoint.get("path_override", None),
            metadata=list_endpoint.get("metadata", None),
        )
        attach_http_endpoint(
            router=router,
            spec=list_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if not raw_list_endpoint.get("disable", False):
        raw_list_endpoint_spec = build_document_raw_list_endpoint_spec(
            dtos=dtos,
            path_override=raw_list_endpoint.get("path_override", None),
            metadata=raw_list_endpoint.get("metadata", None),
        )
        attach_http_endpoint(
            router=router,
            spec=raw_list_endpoint_spec,
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if not create_endpoint.get("disable", False):
        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                str(document.name),
            )

        if dtos.create is None:
            logger.warning(
                "Create DTO is not provided for document '%s', skipping",
                str(document.name),
            )

        else:
            idempotency_ttl = config.get("idempotency_ttl", timedelta(seconds=30))
            enable_idempotency = config.get("enable_idempotency", True)

            idempotency = (
                IdempotencySpec(name=str(document.name), ttl=idempotency_ttl)
                if enable_idempotency
                else None
            )

            create_endpoint_spec = build_document_create_endpoint_spec(
                dtos=dtos,
                path_override=create_endpoint.get("path_override", None),
                metadata=create_endpoint.get("metadata", None),
                idempotency=idempotency,
            )
            attach_http_endpoint(
                router=router,
                spec=create_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if not update_endpoint.get("disable", False):
        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                str(document.name),
            )

        elif not dtos.update:
            logger.warning(
                "Update DTO is not provided for document '%s', skipping",
                str(document.name),
            )

        elif not document.supports_update():
            logger.warning(
                "Update is not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            update_endpoint_spec = build_document_update_endpoint_spec(
                dtos=dtos,
                path_override=update_endpoint.get("path_override", None),
                metadata=update_endpoint.get("metadata", None),
            )
            attach_http_endpoint(
                router=router,
                spec=update_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if not kill_endpoint.get("disable", False):
        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            kill_endpoint_spec = build_document_kill_endpoint_spec(
                path_override=kill_endpoint.get("path_override", None),
                metadata=kill_endpoint.get("metadata", None),
            )
            attach_http_endpoint(
                router=router,
                spec=kill_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if not delete_endpoint.get("disable", False):
        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                document.name,
            )

        elif not document.supports_soft_delete():
            logger.warning(
                "Soft delete is not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            delete_endpoint_spec = build_document_delete_endpoint_spec(
                dtos=dtos,
                path_override=delete_endpoint.get("path_override", None),
                metadata=delete_endpoint.get("metadata", None),
            )
            attach_http_endpoint(
                router=router,
                spec=delete_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if not restore_endpoint.get("disable", False):
        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                str(document.name),
            )

        elif not document.supports_soft_delete():
            logger.warning(
                "Soft delete is not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            restore_endpoint_spec = build_document_restore_endpoint_spec(
                dtos=dtos,
                path_override=restore_endpoint.get("path_override", None),
                metadata=restore_endpoint.get("metadata", None),
            )
            attach_http_endpoint(
                router=router,
                spec=restore_endpoint_spec,
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    return router
