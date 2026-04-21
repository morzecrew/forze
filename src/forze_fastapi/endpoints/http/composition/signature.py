from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from typing import Callable

from fastapi import Header, Request

from forze.application.execution import ExecutionContext

from ..contracts import (
    HTTP_CTX_KEY,
    HTTP_FACADE_KEY,
    HTTP_REQUEST_KEY,
    HttpBodyMode,
    HttpEndpointSpec,
)
from ..contracts.typevars import B, C, F, H, In, P, Q, R, Raw
from ..features import (
    IDEMPOTENCY_KEY_HEADER,
    IF_NONE_MATCH_HEADER_KEY,
    ETagFeature,
    IdempotencyFeature,
)
from .utils import (
    build_body_parameters,
    build_cookie_parameter,
    build_dependency_parameter,
    build_header_parameter,
    build_path_parameter,
    build_query_parameter,
    validate_http_param_name_conflicts,
)

# ----------------------- #


def build_http_endpoint_signature(
    *,
    spec: HttpEndpointSpec[Q, P, H, C, B, In, Raw, R, F],
    facade_dep: Callable[[ExecutionContext], F],
    ctx_dep: Callable[[], ExecutionContext],
) -> inspect.Signature:
    path_model = None
    query_model = None
    body_model = None
    body_mode: HttpBodyMode = "json"
    header_model = None
    cookie_model = None

    if spec.request is not None:
        path_model = spec.request.get("path_type")
        query_model = spec.request.get("query_type")
        body_model = spec.request.get("body_type")
        body_mode = spec.request.get("body_mode", "json")
        header_model = spec.request.get("header_type")
        cookie_model = spec.request.get("cookie_type")

    validate_http_param_name_conflicts(
        path_model=path_model,
        query_model=query_model,
        body_model=body_model,
        body_mode=body_mode,
        header_model=header_model,
        cookie_model=cookie_model,
    )

    params: list[inspect.Parameter] = []

    if query_model is not None:
        for field_name, field in query_model.model_fields.items():
            params.append(build_query_parameter(field_name, field))

    if path_model is not None:
        for field_name, field in path_model.model_fields.items():
            params.append(build_path_parameter(field_name, field))

    if header_model is not None:
        for field_name, field in header_model.model_fields.items():
            params.append(build_header_parameter(field_name, field))

    if cookie_model is not None:
        for field_name, field in cookie_model.model_fields.items():
            params.append(build_cookie_parameter(field_name, field))

    if body_model is not None:
        params.extend(build_body_parameters(body_model, body_mode))

    params.append(
        inspect.Parameter(
            name=HTTP_REQUEST_KEY,
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=Request,
            default=inspect.Parameter.empty,
        )
    )

    params.append(
        build_dependency_parameter(
            name=HTTP_CTX_KEY,
            annotation=ExecutionContext,
            dependency=ctx_dep,
        )
    )

    params.append(
        build_dependency_parameter(
            name=HTTP_FACADE_KEY,
            annotation=spec.facade_type,
            dependency=facade_dep,
        )
    )

    if spec.features:
        idempotency_presented = (
            next((f for f in spec.features if isinstance(f, IdempotencyFeature)), None)
            is not None
        )
        etag_presented = (
            next((f for f in spec.features if isinstance(f, ETagFeature)), None)
            is not None
        )

        if idempotency_presented:
            params.append(
                inspect.Parameter(
                    name="__idempotency_key",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=str,
                    default=Header(..., alias=IDEMPOTENCY_KEY_HEADER),
                )
            )

        if etag_presented:
            params.append(
                inspect.Parameter(
                    name="__if_none_match",
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=str | None,
                    default=Header(default=None, alias=IF_NONE_MATCH_HEADER_KEY),
                )
            )

    return inspect.Signature(
        parameters=params,
        return_annotation=spec.response or type(None),
    )
