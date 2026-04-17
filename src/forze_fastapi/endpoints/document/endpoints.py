from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from forze.application.composition.document import DocumentDTOs, DocumentUsecasesFacade
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.dto import (
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentNumberIdDTO,
    DocumentUpdateDTO,
    ListRequestDTO,
    Paginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from .._utils import path_coerce
from ..http import (
    BodyAsIsMapper,
    ETagFeature,
    HttpEndpointSpec,
    HttpMetadataSpec,
    IdempotencyFeature,
    QueryAsIsBodyAssignMapper,
    QueryAsIsMapper,
    build_http_endpoint_spec,
)
from .features import document_etag

# ----------------------- #

Facade = DocumentUsecasesFacade[Any, Any, Any]
Idempotency = IdempotencyFeature[Any, Any, Any, Any, Any, Any, Any, Any]
ETag = ETagFeature[Any, Any, Any, Any, Any, Any, Any, Any]

# ....................... #

type GetDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type GetEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    DocumentIdDTO,
    Any,
    Any,
    Any,
    Any,
    Any,
    R,
    Facade,
]


def build_document_get_endpoint_spec[R: ReadDocument](
    dtos: GetDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    etag: bool = True,
    etag_auto_304: bool = True,
) -> GetEndpointSpec[R]:
    path = path_override or "/get"
    path = path_coerce(path)

    features = (
        [
            ETag(
                provider=document_etag,
                auto_304=etag_auto_304,
            )
        ]
        if etag
        else None
    )

    return build_http_endpoint_spec(
        Facade,
        Facade.get,  # type: ignore[misc]
        http={"method": "GET", "path": path},
        request={"query_type": DocumentIdDTO},
        metadata=metadata,
        response=dtos.read,
        mapper=QueryAsIsMapper(DocumentIdDTO),
        features=features,
    )


# ....................... #

type GetByNumberIdDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type GetByNumberIdEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    DocumentNumberIdDTO,
    Any,
    Any,
    Any,
    Any,
    Any,
    R,
    Facade,
]


def build_document_get_by_number_id_endpoint_spec[R: ReadDocument](
    dtos: GetByNumberIdDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    etag: bool = True,
    etag_auto_304: bool = True,
) -> GetByNumberIdEndpointSpec[R]:
    path = path_override or "/get-by-num"
    path = path_coerce(path)

    features = (
        [
            ETag(
                provider=document_etag,
                auto_304=etag_auto_304,
            )
        ]
        if etag
        else None
    )

    return build_http_endpoint_spec(
        Facade,
        Facade.get_by_number_id,  # type: ignore[misc]
        http={"method": "GET", "path": path},
        request={"query_type": DocumentNumberIdDTO},
        metadata=metadata,
        response=dtos.read,
        mapper=QueryAsIsMapper(DocumentNumberIdDTO),
        features=features,
    )


# ....................... #

type ListDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type ListEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    ListRequestDTO,
    ListRequestDTO,
    Paginated[R],
    Facade,
]


def build_document_list_endpoint_spec[R: ReadDocument](
    dtos: ListDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> ListEndpointSpec[R]:
    path = path_override or "/list"
    path = path_coerce(path)

    return build_http_endpoint_spec(
        Facade,
        Facade.list,  # type: ignore[misc]
        http={"method": "POST", "path": path},
        request={"body_type": ListRequestDTO},
        metadata=metadata,
        response=Paginated[dtos.read],  # type: ignore[name-defined]
        mapper=BodyAsIsMapper(ListRequestDTO),
    )


# ....................... #

type RawListDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type RawListEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawListRequestDTO,
    RawListRequestDTO,
    RawPaginated,
    Facade,
]


def build_document_raw_list_endpoint_spec[R: ReadDocument](
    dtos: RawListDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawListEndpointSpec[R]:
    path = path_override or "/raw-list"
    path = path_coerce(path)

    return build_http_endpoint_spec(
        Facade,
        Facade.raw_list,  # type: ignore[misc]
        http={"method": "POST", "path": path},
        request={"body_type": RawListRequestDTO},
        metadata=metadata,
        response=RawPaginated,
        mapper=BodyAsIsMapper(RawListRequestDTO),
    )


# ....................... #

type CreateDTOs[R: ReadDocument, C: BaseDTO] = DocumentDTOs[R, C, Any]
type CreateEndpointSpec[R: ReadDocument, C: BaseDTO] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    C,
    C,
    R,
    Facade,
]


def build_document_create_endpoint_spec[R: ReadDocument, C: BaseDTO](
    dtos: CreateDTOs[R, C],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    idempotency: IdempotencySpec | None = None,
) -> CreateEndpointSpec[R, C]:
    path = path_override or "/create"
    path = path_coerce(path)

    if dtos.create is None:
        raise CoreError("Create DTO is not provided")

    features = [Idempotency(spec=idempotency)] if idempotency is not None else None

    return build_http_endpoint_spec(
        Facade,
        Facade.create,  # type: ignore[misc]
        http={"method": "POST", "path": path},
        request={"body_type": dtos.create},
        metadata=metadata,
        response=dtos.read,
        mapper=BodyAsIsMapper(dtos.create),
        features=features,
    )


# ....................... #

type UpdateDTOs[R: ReadDocument, U: BaseDTO] = DocumentDTOs[R, Any, U]
type UpdateEndpointSpec[R: ReadDocument, U: BaseDTO] = HttpEndpointSpec[
    DocumentIdRevDTO,
    Any,
    Any,
    Any,
    U,
    DocumentUpdateDTO[U],
    R,
    Facade,
]


def build_document_update_endpoint_spec[R: ReadDocument, U: BaseDTO](
    dtos: UpdateDTOs[R, U],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> UpdateEndpointSpec[R, U]:
    path = path_override or "/update"
    path = path_coerce(path)

    if dtos.update is None:
        raise CoreError("Update DTO is not provided")

    return build_http_endpoint_spec(
        Facade,
        Facade.update,  # type: ignore[misc]
        http={"method": "PATCH", "path": path},
        request={
            "query_type": DocumentIdRevDTO,
            "body_type": dtos.update,
        },
        metadata=metadata,
        response=dtos.read,
        mapper=QueryAsIsBodyAssignMapper(
            DocumentUpdateDTO[dtos.update],  # type: ignore[name-defined]
            body_key="dto",
        ),
    )


# ....................... #

KillEndpointSpec = HttpEndpointSpec[
    DocumentIdDTO,
    Any,
    Any,
    Any,
    Any,
    Any,
    Any,
    Facade,
]


def build_document_kill_endpoint_spec(
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> KillEndpointSpec:
    path = path_override or "/kill"
    path = path_coerce(path)

    return build_http_endpoint_spec(
        Facade,
        Facade.kill,  # type: ignore[misc]
        http={"method": "DELETE", "path": path, "status_code": 204},
        request={"query_type": DocumentIdDTO},
        metadata=metadata,
        mapper=QueryAsIsMapper(DocumentIdDTO),
        response=None,
    )


# ....................... #

type SoftDeleteDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type SoftDeleteEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    DocumentIdRevDTO,
    Any,
    Any,
    Any,
    Any,
    Any,
    R,
    Facade,
]


def build_document_delete_endpoint_spec[R: ReadDocument](
    dtos: SoftDeleteDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> SoftDeleteEndpointSpec[R]:
    path = path_override or "/delete"
    path = path_coerce(path)

    return build_http_endpoint_spec(
        Facade,
        Facade.delete,  # type: ignore[misc]
        http={"method": "PATCH", "path": path},
        request={"query_type": DocumentIdRevDTO},
        response=dtos.read,
        metadata=metadata,
        mapper=QueryAsIsMapper(DocumentIdRevDTO),
    )


def build_document_restore_endpoint_spec[R: ReadDocument](
    dtos: SoftDeleteDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> SoftDeleteEndpointSpec[R]:
    path = path_override or "/restore"
    path = path_coerce(path)

    return build_http_endpoint_spec(
        Facade,
        Facade.restore,  # type: ignore[misc]
        http={"method": "PATCH", "path": path},
        request={"query_type": DocumentIdRevDTO},
        response=dtos.read,
        metadata=metadata,
        mapper=QueryAsIsMapper(DocumentIdRevDTO),
    )
