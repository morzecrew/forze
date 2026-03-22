from forze.base.primitives import JsonDict

from ..contracts import HTTP_BODY_KEY, HttpRequestDTO, HttpRequestSpec
from ..contracts.typevars import B, C, H, P, Q
from .utils import model_from_kwargs

# ----------------------- #


def build_request_dto(
    *,
    kwargs: JsonDict,
    spec: HttpRequestSpec[Q, P, H, C, B] | None,
) -> HttpRequestDTO[Q, P, H, C, B]:
    path_model = None
    query_model = None
    body_model = None
    body_mode = "json"
    header_model = None
    cookie_model = None

    if spec is not None:
        path_model = spec.get("path_type")
        query_model = spec.get("query_type")
        body_model = spec.get("body_type")
        body_mode = spec.get("body_mode", "json")
        header_model = spec.get("header_type")
        cookie_model = spec.get("cookie_type")

    if body_mode == "json":
        body: B | None = kwargs.get(HTTP_BODY_KEY)

    else:
        body = model_from_kwargs(model_type=body_model, kwargs=kwargs)

    query = model_from_kwargs(model_type=query_model, kwargs=kwargs)
    path = model_from_kwargs(model_type=path_model, kwargs=kwargs)
    header = model_from_kwargs(model_type=header_model, kwargs=kwargs)
    cookie = model_from_kwargs(model_type=cookie_model, kwargs=kwargs)

    return HttpRequestDTO(
        query=query,
        path=path,
        header=header,
        cookie=cookie,
        body=body,
    )
