from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from datetime import timedelta
from typing import Any, Callable, Sequence

from fastapi import APIRouter

from forze.application.composition.document import DocumentDTOs
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import ExecutionContext, UsecaseRegistry

from .._logger import logger
from ..http import (
    AuthnRequirement,
    HttpEndpointSpec,
    SimpleHttpEndpointSpec,
    attach_http_endpoint,
)
from ..http.policy import (
    AnyFeature,
    apply_authn_requirement,
    with_default_http_features,
)
from .endpoints import (
    build_document_aggregated_list_endpoint_spec,
    build_document_create_endpoint_spec,
    build_document_delete_endpoint_spec,
    build_document_get_by_number_id_endpoint_spec,
    build_document_get_endpoint_spec,
    build_document_kill_endpoint_spec,
    build_document_list_cursor_endpoint_spec,
    build_document_list_endpoint_spec,
    build_document_raw_list_cursor_endpoint_spec,
    build_document_raw_list_endpoint_spec,
    build_document_restore_endpoint_spec,
    build_document_update_endpoint_spec,
)
from .specs import DocumentEndpointsSpec

# ----------------------- #
#! A bit damn function, but building a framework around is even more stupid.

HttpEndSpec = HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any, Any]


def attach_document_endpoints(
    router: APIRouter,
    *,
    document: DocumentSpec[Any, Any, Any, Any],
    dtos: DocumentDTOs[Any, Any, Any],
    registry: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    endpoints: DocumentEndpointsSpec | None = None,
    exclude_none: bool = True,
    default_http_features: Sequence[AnyFeature] | None = None,
) -> APIRouter:
    endpoints = endpoints or {}
    config = endpoints.get("config", {})
    base_authn: AuthnRequirement | None = endpoints.get("authn")

    def _resolve_authn(
        simple: SimpleHttpEndpointSpec | None,
    ) -> AuthnRequirement | None:
        if simple is not None:
            per_endpoint = simple.get("authn")
            if per_endpoint is not None:
                return per_endpoint
        return base_authn

    def _apply_defaults(
        spec: HttpEndSpec,
        simple: SimpleHttpEndpointSpec | None = None,
    ) -> HttpEndSpec:
        with_defaults = with_default_http_features(spec, default_http_features)
        return apply_authn_requirement(with_defaults, _resolve_authn(simple))

    get_endpoint = endpoints.get("get_", False)
    get_by_number_id_endpoint = endpoints.get("get_by_number_id", False)
    list_endpoint = endpoints.get("list_", False)
    raw_list_endpoint = endpoints.get("raw_list", False)
    list_cursor_endpoint = endpoints.get("list_cursor", False)
    raw_list_cursor_endpoint = endpoints.get("raw_list_cursor", False)
    aggregated_list_endpoint = endpoints.get("aggregated_list", False)
    create_endpoint = endpoints.get("create", False)
    update_endpoint = endpoints.get("update", False)
    kill_endpoint = endpoints.get("kill", False)
    delete_endpoint = endpoints.get("delete", False)
    restore_endpoint = endpoints.get("restore", False)

    if get_endpoint is not False:
        _get = get_endpoint if get_endpoint is not True else SimpleHttpEndpointSpec()

        get_endpoint_spec = build_document_get_endpoint_spec(
            dtos=dtos,
            path_override=_get.get("path_override"),
            metadata=_get.get("metadata"),
            etag=config.get("enable_etag", False),
            etag_auto_304=config.get("etag_auto_304", False),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(get_endpoint_spec, _get),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if get_by_number_id_endpoint is not False:
        _get_by_number_id = (
            get_by_number_id_endpoint
            if get_by_number_id_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        if not document.supports_number_id():
            logger.warning(
                "Number ID is not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            get_by_number_id_endpoint_spec = (
                build_document_get_by_number_id_endpoint_spec(
                    dtos=dtos,
                    path_override=_get_by_number_id.get("path_override"),
                    metadata=_get_by_number_id.get("metadata"),
                    etag=config.get("enable_etag", False),
                    etag_auto_304=config.get("etag_auto_304", False),
                )
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(get_by_number_id_endpoint_spec, _get_by_number_id),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if list_endpoint is not False:
        _list = list_endpoint if list_endpoint is not True else SimpleHttpEndpointSpec()

        list_endpoint_spec = build_document_list_endpoint_spec(
            dtos=dtos,
            path_override=_list.get("path_override"),
            metadata=_list.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(list_endpoint_spec, _list),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if raw_list_endpoint is not False:
        _raw_list = (
            raw_list_endpoint
            if raw_list_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_list_endpoint_spec = build_document_raw_list_endpoint_spec(
            dtos=dtos,
            path_override=_raw_list.get("path_override"),
            metadata=_raw_list.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(raw_list_endpoint_spec, _raw_list),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if aggregated_list_endpoint is not False:
        _aggregated_list = (
            aggregated_list_endpoint
            if aggregated_list_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        aggregated_list_endpoint_spec = build_document_aggregated_list_endpoint_spec(
            dtos=dtos,
            path_override=_aggregated_list.get("path_override"),
            metadata=_aggregated_list.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(aggregated_list_endpoint_spec, _aggregated_list),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if list_cursor_endpoint is not False:
        _list_c = (
            list_cursor_endpoint
            if list_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        list_cursor_endpoint_spec = build_document_list_cursor_endpoint_spec(
            dtos=dtos,
            path_override=_list_c.get("path_override"),
            metadata=_list_c.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(list_cursor_endpoint_spec, _list_c),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if raw_list_cursor_endpoint is not False:
        _raw_list_c = (
            raw_list_cursor_endpoint
            if raw_list_cursor_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        raw_list_cursor_endpoint_spec = build_document_raw_list_cursor_endpoint_spec(
            dtos=dtos,
            path_override=_raw_list_c.get("path_override"),
            metadata=_raw_list_c.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(raw_list_cursor_endpoint_spec, _raw_list_c),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if create_endpoint is not False:
        _create = (
            create_endpoint if create_endpoint is not True else SimpleHttpEndpointSpec()
        )

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
            enable_idempotency = config.get("enable_idempotency", False)

            idempotency = (
                IdempotencySpec(name=str(document.name), ttl=idempotency_ttl)
                if enable_idempotency
                else None
            )

            create_endpoint_spec = build_document_create_endpoint_spec(
                dtos=dtos,
                path_override=_create.get("path_override"),
                metadata=_create.get("metadata"),
                idempotency=idempotency,
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(create_endpoint_spec, _create),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if update_endpoint is not False:
        _update = (
            update_endpoint if update_endpoint is not True else SimpleHttpEndpointSpec()
        )

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
                path_override=_update.get("path_override"),
                metadata=_update.get("metadata"),
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(update_endpoint_spec, _update),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if kill_endpoint is not False:
        _kill = kill_endpoint if kill_endpoint is not True else SimpleHttpEndpointSpec()

        if document.write is None:
            logger.warning(
                "Write operations are not supported for document '%s', skipping",
                str(document.name),
            )

        else:
            kill_endpoint_spec = build_document_kill_endpoint_spec(
                path_override=_kill.get("path_override"),
                metadata=_kill.get("metadata"),
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(kill_endpoint_spec, _kill),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if delete_endpoint is not False:
        _delete = (
            delete_endpoint if delete_endpoint is not True else SimpleHttpEndpointSpec()
        )

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
                path_override=_delete.get("path_override"),
                metadata=_delete.get("metadata"),
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(delete_endpoint_spec, _delete),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    if restore_endpoint is not False:
        _restore = (
            restore_endpoint
            if restore_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

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
                path_override=_restore.get("path_override"),
                metadata=_restore.get("metadata"),
            )
            attach_http_endpoint(
                router=router,
                spec=_apply_defaults(restore_endpoint_spec, _restore),
                registry=registry,
                ctx_dep=ctx_dep,
                exclude_none=exclude_none,
            )

    return router
