from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from forze.application.composition.document import DocumentDTOs, DocumentUsecasesFacade
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    CursorPaginated,
    DocumentIdDTO,
    DocumentIdRevDTO,
    DocumentNumberIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
    ListRequestDTO,
    Paginated,
    RawCursorListRequestDTO,
    RawCursorPaginated,
    RawListRequestDTO,
    RawPaginated,
)
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, ReadDocument

from .._utils import path_coerce
from ..http import (
    BodyAsIsMapper,
    DocumentUpdateResDataMapper,
    ETagFeature,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestSpec,
    HttpSpec,
    IdempotencyFeature,
    QueryAsIsBodyAssignMapper,
    QueryAsIsMapper,
    build_http_endpoint_spec,
)
from .features import document_etag

# ----------------------- #

Facade = DocumentUsecasesFacade[Any, Any, Any]

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
    R,
    Facade,
]


def build_document_get_endpoint_spec[R: ReadDocument](
    dtos: GetDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    etag: bool = False,
    etag_auto_304: bool = False,
) -> GetEndpointSpec[R]:
    path = path_override or "/get"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "GET", "path": path}
    request_spec: HttpRequestSpec[DocumentIdDTO, Any, Any, Any, Any] = {
        "query_type": DocumentIdDTO,
    }

    features: (
        list[
            ETagFeature[DocumentIdDTO, Any, Any, Any, Any, DocumentIdDTO, R, R, Facade]
        ]
        | None
    )
    if etag:
        features = [
            ETagFeature(
                provider=document_etag,
                auto_304=etag_auto_304,
            )
        ]
    else:
        features = None

    return build_http_endpoint_spec(
        Facade,
        Facade.get,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
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
    R,
    Facade,
]


def build_document_get_by_number_id_endpoint_spec[R: ReadDocument](
    dtos: GetByNumberIdDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
    etag: bool = False,
    etag_auto_304: bool = False,
) -> GetByNumberIdEndpointSpec[R]:
    path = path_override or "/get-by-num"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "GET", "path": path}
    request_spec: HttpRequestSpec[DocumentNumberIdDTO, Any, Any, Any, Any] = {
        "query_type": DocumentNumberIdDTO,
    }

    features: (
        list[
            ETagFeature[
                DocumentNumberIdDTO,
                Any,
                Any,
                Any,
                Any,
                DocumentNumberIdDTO,
                R,
                R,
                Facade,
            ]
        ]
        | None
    )
    if etag:
        features = [
            ETagFeature(
                provider=document_etag,
                auto_304=etag_auto_304,
            )
        ]
    else:
        features = None

    return build_http_endpoint_spec(
        Facade,
        Facade.get_by_number_id,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
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

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, ListRequestDTO] = {
        "body_type": ListRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.list,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
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

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawListRequestDTO] = {
        "body_type": RawListRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.raw_list,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawPaginated,
        mapper=BodyAsIsMapper(RawListRequestDTO),
    )


# ....................... #

type ListCursorDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type ListCursorEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    CursorListRequestDTO,
    CursorListRequestDTO,
    CursorPaginated[R],
    CursorPaginated[R],
    Facade,
]


def build_document_list_cursor_endpoint_spec[R: ReadDocument](
    dtos: ListCursorDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> ListCursorEndpointSpec[R]:
    path = path_override or "/list-cursor"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, CursorListRequestDTO] = {
        "body_type": CursorListRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.list_cursor,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=CursorPaginated[dtos.read],  # type: ignore[name-defined]
        mapper=BodyAsIsMapper(CursorListRequestDTO),
    )


# ....................... #

type RawListCursorDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type RawListCursorEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawCursorListRequestDTO,
    RawCursorListRequestDTO,
    RawCursorPaginated,
    RawCursorPaginated,
    Facade,
]


def build_document_raw_list_cursor_endpoint_spec[R: ReadDocument](
    dtos: RawListCursorDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawListCursorEndpointSpec[R]:
    path = path_override or "/raw-list-cursor"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawCursorListRequestDTO] = {
        "body_type": RawCursorListRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.raw_list_cursor,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawCursorPaginated,
        mapper=BodyAsIsMapper(RawCursorListRequestDTO),
    )


# ....................... #

type AggregatedListDTOs[R: ReadDocument] = DocumentDTOs[R, Any, Any]
type AggregatedListEndpointSpec[R: ReadDocument] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    AggregatedListRequestDTO,
    AggregatedListRequestDTO,
    RawPaginated,
    RawPaginated,
    Facade,
]


def build_document_aggregated_list_endpoint_spec[R: ReadDocument](
    dtos: AggregatedListDTOs[R],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> AggregatedListEndpointSpec[R]:
    path = path_override or "/aggregated-list"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, AggregatedListRequestDTO] = {
        "body_type": AggregatedListRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.agg_list,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawPaginated,
        mapper=BodyAsIsMapper(AggregatedListRequestDTO),
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

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, C] = {"body_type": dtos.create}

    features: list[IdempotencyFeature[Any, Any, Any, Any, C, C, R, R, Facade]] | None
    if idempotency is not None:
        features = [IdempotencyFeature(spec=idempotency)]
    else:
        features = None

    return build_http_endpoint_spec(
        Facade,
        Facade.create,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
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
    DocumentUpdateRes[R],
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

    http_spec: HttpSpec = {"method": "PATCH", "path": path}
    request_spec: HttpRequestSpec[DocumentIdRevDTO, Any, Any, Any, U] = {
        "query_type": DocumentIdRevDTO,
        "body_type": dtos.update,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.update,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=dtos.read,
        mapper=QueryAsIsBodyAssignMapper(
            DocumentUpdateDTO[dtos.update],  # type: ignore[name-defined]
            body_key="dto",
        ),
        response_mapper=DocumentUpdateResDataMapper(),
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

    http_spec: HttpSpec = {
        "method": "DELETE",
        "path": path,
        "status_code": 204,
    }
    request_spec: HttpRequestSpec[DocumentIdDTO, Any, Any, Any, Any] = {
        "query_type": DocumentIdDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.kill,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        mapper=QueryAsIsMapper(DocumentIdDTO),
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

    http_spec: HttpSpec = {"method": "PATCH", "path": path}
    request_spec: HttpRequestSpec[DocumentIdRevDTO, Any, Any, Any, Any] = {
        "query_type": DocumentIdRevDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.delete,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
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

    http_spec: HttpSpec = {"method": "PATCH", "path": path}
    request_spec: HttpRequestSpec[DocumentIdRevDTO, Any, Any, Any, Any] = {
        "query_type": DocumentIdRevDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.restore,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        response=dtos.read,
        metadata=metadata,
        mapper=QueryAsIsMapper(DocumentIdRevDTO),
    )
