from typing import Sequence, TypeVar, overload

from forze.application.contracts.mapping import MapperPort
from forze.application.execution import facade_call, facade_op
from forze.base.errors import CoreError
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
from ..contracts.typevars import B, C, F, H, In, P, Q, R, Raw
from ..features import ETagFeature, IdempotencyFeature

# ----------------------- #

_RR = TypeVar("_RR")
_RM = TypeVar("_RM")

# ....................... #


def validate_http_features(
    http: HttpSpec,
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F]] | None
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
    facade_type: type[F],
    call: facade_op[In, Raw],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[_RR],
    response_mapper: MapperPort[Raw, _RR] | None = None,
    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, _RR, F]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, _RR, F]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    facade_type: type[F],
    call: facade_op[In, Raw],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[None] = type(None),
    response_mapper: MapperPort[Raw, _RM],
    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, _RM, F]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, _RM, F]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    facade_type: type[F],
    call: facade_op[In, Raw],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[None] = type(None),
    response_mapper: None = None,
    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, None, F]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, None, F]: ...


# ....................... #


@overload
def build_http_endpoint_spec(
    facade_type: type[F],
    call: facade_op[In, Raw],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[R | None] = type(None),
    response_mapper: MapperPort[Raw, R] | None = None,
    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In] = ...,  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, R, F]: ...


# ....................... #


def build_http_endpoint_spec(
    facade_type: type[F],
    call: facade_op[In, Raw],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    metadata: HttpMetadataSpec | None = None,
    response: type[R | None] = type(None),
    response_mapper: MapperPort[Raw, R] | None = None,
    mapper: MapperPort[HttpRequestDTO[Q, P, H, C, B], In] = EmptyMapper(),  # type: ignore[assignment]
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F]] | None
    ) = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, Raw, R, F]:

    # fail fast if features are invalid
    validate_http_features(http, features)

    return HttpEndpointSpec(
        http=http,
        metadata=metadata,
        features=features,
        request=request,
        response=response,
        mapper=mapper,
        response_mapper=response_mapper,
        facade_type=facade_type,
        call=facade_call(call),
    )


# ....................... #


def compose_endpoint_features(
    handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F],
    features: (
        Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, Raw, R, F]] | None
    ) = None,
) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, Raw, R, F]:
    wrapped = handler

    if features is not None:
        for feature in features:
            wrapped = feature.wrap(wrapped)

    return wrapped
