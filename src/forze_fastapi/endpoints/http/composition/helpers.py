from typing import Sequence

from forze.application.contracts.mapper import LocalMapperPort
from forze.application.execution import facade_call, facade_op

from ..contracts import (
    HttpEndpointFeaturePort,
    HttpEndpointHandlerPort,
    HttpEndpointSpec,
    HttpMetadataSpec,
    HttpRequestDTO,
    HttpRequestSpec,
    HttpSpec,
)
from ..contracts.typevars import B, C, F, H, In, P, Q, R

# ----------------------- #


def build_http_endpoint_spec(
    facade_type: type[F],
    call: facade_op[In, R],
    *,
    http: HttpSpec,
    request: HttpRequestSpec[Q, P, H, C, B] | None = None,
    mapper: LocalMapperPort[HttpRequestDTO[Q, P, H, C, B], In],
    metadata: HttpMetadataSpec | None = None,
    response: type[R] | None = None,
    features: Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, R, F]] | None = None,
) -> HttpEndpointSpec[Q, P, H, C, B, In, R, F]:
    return HttpEndpointSpec(
        http=http,
        metadata=metadata,
        features=features,
        request=request,
        response=response,
        mapper=mapper,
        facade_type=facade_type,
        call=facade_call(call),
    )


# ....................... #


def compose_endpoint_features(
    handler: HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F],
    features: Sequence[HttpEndpointFeaturePort[Q, P, H, C, B, In, R, F]] | None = None,
) -> HttpEndpointHandlerPort[Q, P, H, C, B, In, R, F]:
    wrapped = handler

    if features is not None:
        for feature in features:
            wrapped = feature.wrap(wrapped)

    return wrapped
