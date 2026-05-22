from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any, cast

from forze.application.composition.storage import StorageKernelOp
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.handlers.storage import ListedObjects, ListObjectsRequestDTO
from forze.application.handlers.storage.dto import (
    StoredObjectDTO,
    UploadObjectRequestDTO,
)
from forze.base.primitives import StrKeyNamespace

from .._utils import path_coerce
from ..http import (
    BodyAsIsMapper,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestSpec,
    HttpSpec,
    IdempotencyFeature,
    build_http_endpoint_spec,
)
from .mappers import (
    DownloadedObjectResponseMapper,
    StorageKeyFromPathMapper,
    StorageUploadFormMapper,
)
from .models import StorageObjectKeyPath, StorageUploadFormBody

# ----------------------- #

type ListEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    ListObjectsRequestDTO,
    ListObjectsRequestDTO,
    ListedObjects,
    ListedObjects,
]


def build_storage_list_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> ListEndpointSpec:
    path = path_override or "/list"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, ListObjectsRequestDTO] = {
        "body_type": ListObjectsRequestDTO,
    }

    return build_http_endpoint_spec(
        namespace.key(StorageKernelOp.LIST),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=ListedObjects,
        request_mapper=BodyAsIsMapper(ListObjectsRequestDTO),
    )


# ....................... #

type UploadEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    StorageUploadFormBody,
    UploadObjectRequestDTO,
    StoredObjectDTO,
    StoredObjectDTO,
]


def build_storage_upload_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    idempotency: IdempotencySpec | None = None,
) -> UploadEndpointSpec:
    path = path_override or "/upload"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, StorageUploadFormBody] = {
        "body_type": StorageUploadFormBody,
        "body_mode": "form",
    }

    features: (
        list[
            IdempotencyFeature[
                Any,
                Any,
                Any,
                Any,
                StorageUploadFormBody,
                UploadObjectRequestDTO,
                StoredObjectDTO,
                StoredObjectDTO,
            ]
        ]
        | None
    )
    if idempotency is not None:
        features = [IdempotencyFeature(spec=idempotency)]
    else:
        features = None

    return build_http_endpoint_spec(
        namespace.key(StorageKernelOp.UPLOAD),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=StoredObjectDTO,
        request_mapper=StorageUploadFormMapper(),
        features=features,
    )


# ....................... #

type DownloadEndpointSpec = HttpEndpointSpec[
    Any,
    StorageObjectKeyPath,
    Any,
    Any,
    Any,
    Any,
    Any,
    Any,
]


def build_storage_download_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> DownloadEndpointSpec:
    path = path_override or "/download/{key:path}"
    path = path_coerce(path)

    merged_metadata: HttpMetadataSpec | None
    if "responses" not in dict(metadata or {}):
        merged_metadata = cast(
            HttpMetadataSpec,
            {
                **dict(metadata or {}),
                "responses": {
                    200: {
                        "description": "Object bytes",
                        "content": {
                            "application/octet-stream": {
                                "schema": {"type": "string", "format": "binary"},
                            },
                        },
                    },
                },
            },
        )
    else:
        merged_metadata = metadata

    http_spec: HttpSpec = {"method": "GET", "path": path}
    request_spec: HttpRequestSpec[Any, StorageObjectKeyPath, Any, Any, Any] = {
        "path_type": StorageObjectKeyPath,
    }

    return build_http_endpoint_spec(
        namespace.key(StorageKernelOp.DOWNLOAD),
        http=http_spec,
        request=request_spec,
        metadata=merged_metadata,
        response=type(None),
        response_mapper=DownloadedObjectResponseMapper(),
        request_mapper=StorageKeyFromPathMapper(),
    )


# ....................... #

type DeleteEndpointSpec = HttpEndpointSpec[
    Any,
    StorageObjectKeyPath,
    Any,
    Any,
    Any,
    Any,
    Any,
    Any,
]


def build_storage_delete_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> DeleteEndpointSpec:
    path = path_override or "/delete/{key:path}"
    path = path_coerce(path)

    http_spec: HttpSpec = {
        "method": "DELETE",
        "path": path,
        "status_code": 204,
    }
    request_spec: HttpRequestSpec[Any, StorageObjectKeyPath, Any, Any, Any] = {
        "path_type": StorageObjectKeyPath,
    }

    return build_http_endpoint_spec(
        namespace.key(StorageKernelOp.DELETE),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        request_mapper=StorageKeyFromPathMapper(),
    )
