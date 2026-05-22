from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from pydantic import BaseModel

from forze.application.composition.search import SearchDTOs, SearchKernelOp
from forze.application.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
)
from forze.application.handlers.search import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchPaginated,
    ProjectedSearchRequestDTO,
    SearchRequestDTO,
)

RawSearchRequestDTO = ProjectedSearchRequestDTO
RawCursorSearchRequestDTO = ProjectedCursorSearchRequestDTO
RawPaginated = ProjectedSearchPaginated
RawCursorPaginated = ProjectedCursorPaginated
from forze.base.primitives import StrKeyNamespace

from .._utils import path_coerce
from ..http import (
    BodyAsIsMapper,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestSpec,
    HttpSpec,
    build_http_endpoint_spec,
)

# ----------------------- #

type TypedSearchDTOs[M: BaseModel] = SearchDTOs[M]
type TypedSearchEndpointSpec[M: BaseModel] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    SearchRequestDTO,
    SearchRequestDTO,
    Paginated[M],
    Paginated[M],
]


def build_typed_search_endpoint_spec[M: BaseModel](
    dtos: TypedSearchDTOs[M],
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> TypedSearchEndpointSpec[M]:
    path = path_override or "/search"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, SearchRequestDTO] = {
        "body_type": SearchRequestDTO,
    }

    return build_http_endpoint_spec(
        namespace.key(SearchKernelOp.TYPED),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=Paginated[dtos.read],  # type: ignore[name-defined]
        request_mapper=BodyAsIsMapper(SearchRequestDTO),
    )


# ....................... #

RawSearchEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawSearchRequestDTO,
    RawSearchRequestDTO,
    RawPaginated,
    RawPaginated,
]


def build_raw_search_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawSearchEndpointSpec:
    path = path_override or "/raw-search"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawSearchRequestDTO] = {
        "body_type": RawSearchRequestDTO,
    }

    return build_http_endpoint_spec(
        namespace.key(SearchKernelOp.RAW),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawPaginated,
        request_mapper=BodyAsIsMapper(RawSearchRequestDTO),
    )


# ....................... #

type TypedSearchCursorDTOs[M: BaseModel] = SearchDTOs[M]
type TypedSearchCursorEndpointSpec[M: BaseModel] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    CursorSearchRequestDTO,
    CursorSearchRequestDTO,
    CursorPaginated[M],
    CursorPaginated[M],
]


def build_typed_search_cursor_endpoint_spec[M: BaseModel](
    dtos: TypedSearchCursorDTOs[M],
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> TypedSearchCursorEndpointSpec[M]:
    path = path_override or "/search-cursor"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, CursorSearchRequestDTO] = {
        "body_type": CursorSearchRequestDTO,
    }

    return build_http_endpoint_spec(
        namespace.key(SearchKernelOp.TYPED_CURSOR),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=CursorPaginated[dtos.read],  # type: ignore[name-defined]
        request_mapper=BodyAsIsMapper(CursorSearchRequestDTO),
    )


# ....................... #

RawSearchCursorEndpointSpec = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawCursorSearchRequestDTO,
    RawCursorSearchRequestDTO,
    RawCursorPaginated,
    RawCursorPaginated,
]


def build_raw_search_cursor_endpoint_spec(
    *,
    namespace: StrKeyNamespace,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawSearchCursorEndpointSpec:
    path = path_override or "/raw-search-cursor"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawCursorSearchRequestDTO] = {
        "body_type": RawCursorSearchRequestDTO,
    }

    return build_http_endpoint_spec(
        namespace.key(SearchKernelOp.RAW_CURSOR),
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawCursorPaginated,
        request_mapper=BodyAsIsMapper(RawCursorSearchRequestDTO),
    )
