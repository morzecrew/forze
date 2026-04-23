from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from pydantic import BaseModel

from forze.application.composition.search import SearchDTOs, SearchUsecasesFacade
from forze.application.dto import (
    CursorPaginated,
    CursorSearchRequestDTO,
    Paginated,
    RawCursorPaginated,
    RawCursorSearchRequestDTO,
    RawPaginated,
    RawSearchRequestDTO,
    SearchRequestDTO,
)

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

Facade = SearchUsecasesFacade[Any]

# ....................... #

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
    Facade,
]


def build_typed_search_endpoint_spec[M: BaseModel](
    dtos: TypedSearchDTOs[M],
    *,
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
        Facade,
        Facade.search,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=Paginated[dtos.read],  # type: ignore[name-defined]
        mapper=BodyAsIsMapper(SearchRequestDTO),
    )


# ....................... #

type RawSearchDTOs[M: BaseModel] = SearchDTOs[M]
type RawSearchEndpointSpec[M: BaseModel] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawSearchRequestDTO,
    RawSearchRequestDTO,
    RawPaginated,
    RawPaginated,
    Facade,
]


def build_raw_search_endpoint_spec[M: BaseModel](
    dtos: RawSearchDTOs[M],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawSearchEndpointSpec[M]:
    path = path_override or "/raw-search"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawSearchRequestDTO] = {
        "body_type": RawSearchRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.raw_search,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawPaginated,
        mapper=BodyAsIsMapper(RawSearchRequestDTO),
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
    Facade,
]


def build_typed_search_cursor_endpoint_spec[M: BaseModel](
    dtos: TypedSearchCursorDTOs[M],
    *,
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
        Facade,
        Facade.search_cursor,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=CursorPaginated[dtos.read],  # type: ignore[name-defined]
        mapper=BodyAsIsMapper(CursorSearchRequestDTO),
    )


# ....................... #

type RawSearchCursorDTOs[M: BaseModel] = SearchDTOs[M]
type RawSearchCursorEndpointSpec[M: BaseModel] = HttpEndpointSpec[
    Any,
    Any,
    Any,
    Any,
    RawCursorSearchRequestDTO,
    RawCursorSearchRequestDTO,
    RawCursorPaginated,
    RawCursorPaginated,
    Facade,
]


def build_raw_search_cursor_endpoint_spec[M: BaseModel](
    dtos: RawSearchCursorDTOs[M],
    *,
    path_override: str | None = None,
    metadata: HttpMetadataSpec | None = None,
) -> RawSearchCursorEndpointSpec[M]:
    path = path_override or "/raw-search-cursor"
    path = path_coerce(path)

    http_spec: HttpSpec = {"method": "POST", "path": path}
    request_spec: HttpRequestSpec[Any, Any, Any, Any, RawCursorSearchRequestDTO] = {
        "body_type": RawCursorSearchRequestDTO,
    }

    return build_http_endpoint_spec(
        Facade,
        Facade.raw_search_cursor,  # type: ignore[misc]
        http=http_spec,
        request=request_spec,
        metadata=metadata,
        response=RawCursorPaginated,
        mapper=BodyAsIsMapper(RawCursorSearchRequestDTO),
    )
