from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Any

from pydantic import BaseModel

from forze.application.composition.search import SearchDTOs, SearchUsecasesFacade
from forze.application.dto import (
    Paginated,
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
