from typing import Sequence, TypeVar, overload

from forze.application.contracts.mapping import Mapper
from forze.base.errors import CoreError
from forze.base.primitives import StrKey
from forze_fastapi.endpoints.http.mapping import EmptyMapper

from ..contracts import (
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestDTO,
    HttpRequestSpec,
    HttpSpec,
)
from ..contracts.typevars import B, C, H, In, P, Q, R, Raw
from ..features import ETagFeature, IdempotencyFeature

# ----------------------- #

_RR = TypeVar("_RR")
_RM = TypeVar("_RM")

# ....................... #


def validate_http_features(
    http: HttpSpec,
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]] | None
    ) = None,
) -> None:
    if features is None:
        return

    idempotency_feature = next(
        (f for f in features if isinstance(f, IdempotencyFeature)), None
    )
    etag_feature = next((f for f in features if isinstance(f, ETagFeature)), None)

    if idempotency_feature is not None and http["method"] != "POST":
        raise CoreError("Idempotent endpoints must be POST methods")

    if etag_feature is not None and http["method"] != "GET":
        raise CoreError("ETag endpoints must be GET methods")

    return None


# ....................... #

# Mypy: explicit ``response: type[...]`` first; no-body last before the generic
# fallback, so a non-default ``response=`` is not checked against the no-body
# branch first. Call sites use annotated ``HttpSpec`` / ``HttpRequestSpec[...]``
# locals so dict literals are checked against those TypedDicts.


@overload
def build_http_endpoint_spec(
    operation: StrKey,
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[_RR],
    response_mapper: Mapper[Raw, _RR] | None = None,
    request_mapper: Mapper[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, _RR]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, _RR]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    operation: StrKey,
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[None] = type(None),
    response_mapper: Mapper[Raw, _RM],
    request_mapper: Mapper[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, _RM]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, _RM]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    operation: StrKey,
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[None] = type(None),
    response_mapper: None = None,
    request_mapper: Mapper[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, None]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, None]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    operation: StrKey,
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[R | None] = type(None),
    response_mapper: Mapper[Raw, R] | None = None,
    request_mapper: Mapper[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, R]: ...


# ....................... #


def build_http_endpoint_spec(
    operation: StrKey,
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[R | None] = type(None),
    response_mapper: Mapper[Raw, R] | None = None,
    request_mapper: Mapper[HttpRequestDTO[Q, P, H, C, B], In] = EmptyMapper(),  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, R]:

    validate_http_features(http, features)

    return HttpEndpointSpec(
        http=http,
        operation=operation,
        metadata=metadata,
        features=features,
        request=request,
        response=response,
        request_mapper=request_mapper,
        response_mapper=response_mapper,
    )


# ....................... #


def compose_endpoint_features(
    handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R],
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R]] | None
    ) = None,
) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R]:
    wrapped = handler

    if features is not None:
        for feature in features:
            wrapped = feature.wrap(wrapped)

    return wrapped
