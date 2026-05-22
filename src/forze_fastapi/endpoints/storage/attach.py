from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from datetime import timedelta
from typing import Any, Callable, Sequence

from fastapi import APIRouter

from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze.application.execution.registry import FrozenOperationRegistry
from forze.base.errors import CoreError
from forze.base.primitives import StrKeyNamespace

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
    build_storage_delete_endpoint_spec,
    build_storage_download_endpoint_spec,
    build_storage_list_endpoint_spec,
    build_storage_upload_endpoint_spec,
)
from .specs import StorageEndpointsSpec

# ----------------------- #

HttpEndSpec = HttpEndpointSpec[Any, Any, Any, Any, Any, Any, Any, Any]


def attach_storage_endpoints(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ctx_dep: Callable[[], ExecutionContext],
    storage: StorageSpec | None = None,
    namespace: StrKeyNamespace | None = None,
    endpoints: StorageEndpointsSpec | None = None,
    exclude_none: bool = True,
    default_http_features: Sequence[AnyFeature] | None = None,
) -> APIRouter:
    """Attach storage routes (list, upload, download, delete).

    :param storage: Optional :class:`~forze.application.contracts.storage.StorageSpec`
        used for idempotency naming and logging.
    :param namespace: Operation namespace for endpoint operation keys. When omitted,
        derived from ``storage.default_namespace`` when ``storage`` is provided.
    """

    endpoints = endpoints or {}
    config = endpoints.get("config", {})

    base_authn: AuthnRequirement | None = endpoints.get("authn")
    if namespace is None:
        if storage is None:
            msg = "attach_storage_endpoints requires storage=... or namespace=..."
            raise CoreError(msg)
        resolved_namespace = storage.default_namespace
    else:
        resolved_namespace = namespace
    logical_name = (
        str(storage.name) if storage is not None else resolved_namespace.prefix
    )

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

    list_endpoint = endpoints.get("list_", False)
    upload_endpoint = endpoints.get("upload", False)
    download_endpoint = endpoints.get("download", False)
    delete_endpoint = endpoints.get("delete", False)

    if list_endpoint is not False:
        _list = list_endpoint if list_endpoint is not True else SimpleHttpEndpointSpec()

        list_spec = build_storage_list_endpoint_spec(
            namespace=resolved_namespace,
            path_override=_list.get("path_override"),
            metadata=_list.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(list_spec, _list),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if upload_endpoint is not False:
        _upload = (
            upload_endpoint if upload_endpoint is not True else SimpleHttpEndpointSpec()
        )

        upload_spec = build_storage_upload_endpoint_spec(
            namespace=resolved_namespace,
            path_override=_upload.get("path_override"),
            metadata=_upload.get("metadata"),
            idempotency=(
                IdempotencySpec(
                    name=logical_name,
                    ttl=timedelta(seconds=config.get("idempotency_ttl_seconds", 86400)),  # type: ignore[arg-type]
                )
                if config.get("enable_idempotency", False)
                else None
            ),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(upload_spec, _upload),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if download_endpoint is not False:
        _download = (
            download_endpoint
            if download_endpoint is not True
            else SimpleHttpEndpointSpec()
        )

        download_spec = build_storage_download_endpoint_spec(
            namespace=resolved_namespace,
            path_override=_download.get("path_override"),
            metadata=_download.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(download_spec, _download),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    if delete_endpoint is not False:
        _delete = (
            delete_endpoint if delete_endpoint is not True else SimpleHttpEndpointSpec()
        )

        delete_spec = build_storage_delete_endpoint_spec(
            namespace=resolved_namespace,
            path_override=_delete.get("path_override"),
            metadata=_delete.get("metadata"),
        )
        attach_http_endpoint(
            router=router,
            spec=_apply_defaults(delete_spec, _delete),
            registry=registry,
            ctx_dep=ctx_dep,
            exclude_none=exclude_none,
        )

    return router
