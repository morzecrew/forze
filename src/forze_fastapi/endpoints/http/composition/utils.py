from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import inspect
from collections import defaultdict
from typing import Any, Iterable

from fastapi.params import Body, Cookie, Depends, Form, Header, Path, Query
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from forze.base.errors import CoreError

from ..contracts import (
    HTTP_BODY_KEY,
    HTTP_CTX_KEY,
    HTTP_FACADE_KEY,
    HTTP_REQUEST_KEY,
    HttpBodyMode,
)

# ----------------------- #


#! move to base utils or so
def iter_model_field_names(model: type[BaseModel] | None) -> Iterable[str]:
    if model is None:
        return ()
    return model.model_fields.keys()


# ....................... #


#! move to base utils or so
def default_from_field(
    field: FieldInfo,
    *,
    required_override: bool | None = None,
) -> Any:
    if required_override is True:
        return ...
    if required_override is False:
        if field.default is not PydanticUndefined:
            return field.default
        return None

    if field.default is not PydanticUndefined:
        return field.default
    return ...


# ....................... #


def common_kwargs(field: Any) -> dict[str, Any]:
    extra: dict[str, Any] = getattr(field, "json_schema_extra", None) or {}

    return {
        "alias": getattr(field, "alias", None),
        "title": getattr(field, "title", None),
        "description": getattr(field, "description", None),
        "gt": getattr(field, "gt", None),
        "ge": getattr(field, "ge", None),
        "lt": getattr(field, "lt", None),
        "le": getattr(field, "le", None),
        "min_length": getattr(field, "min_length", None),
        "max_length": getattr(field, "max_length", None),
        "pattern": getattr(field, "pattern", None),
        "deprecated": extra.get("deprecated"),
        "examples": getattr(field, "examples", None),
    }


# ....................... #


#! move to base utils or so
def model_from_kwargs[M: BaseModel](
    *,
    model_type: type[M] | None,
    kwargs: dict[str, Any],
) -> M | None:
    if model_type is None:
        return None

    payload: dict[str, Any] = {}

    for field_name, _ in model_type.model_fields.items():
        if field_name in kwargs:
            payload[field_name] = kwargs[field_name]

    return model_type.model_validate(payload)


# ....................... #


def validate_http_param_name_conflicts(
    *,
    path_model: type[BaseModel] | None,
    query_model: type[BaseModel] | None,
    header_model: type[BaseModel] | None = None,
    cookie_model: type[BaseModel] | None = None,
    body_model: type[BaseModel] | None = None,
    body_mode: HttpBodyMode,
) -> None:
    owners: dict[str, list[str]] = defaultdict(list)

    for name in iter_model_field_names(path_model):
        owners[name].append("path")

    for name in iter_model_field_names(query_model):
        owners[name].append("query")

    for name in iter_model_field_names(header_model):
        owners[name].append("header")

    for name in iter_model_field_names(cookie_model):
        owners[name].append("cookie")

    if body_model is not None and body_mode == "form":
        for name in iter_model_field_names(body_model):
            owners[name].append("body")

    conflicts = {name: tuple(kinds) for name, kinds in owners.items() if len(kinds) > 1}

    if conflicts:
        parts = ", ".join(
            f"{name}: {', '.join(kinds)}" for name, kinds in sorted(conflicts.items())
        )
        raise CoreError(f"HTTP parameter name conflicts detected: {parts}")

    reserved_names = {HTTP_REQUEST_KEY, HTTP_CTX_KEY, HTTP_FACADE_KEY}

    reserved_conflicts = {
        name: tuple(kinds) for name, kinds in owners.items() if name in reserved_names
    }

    if reserved_conflicts:
        parts = ", ".join(
            f"{name}: {', '.join(kinds)}"
            for name, kinds in sorted(reserved_conflicts.items())
        )
        raise CoreError(f"HTTP parameter name conflicts with reserved names: {parts}")


# ....................... #


def query_from_field(field: Any) -> Any:
    return Query(default=default_from_field(field), **common_kwargs(field))


def header_from_field(field: Any) -> Any:
    return Header(default=default_from_field(field), **common_kwargs(field))


def cookie_from_field(field: Any) -> Any:
    return Cookie(default=default_from_field(field), **common_kwargs(field))


def path_from_field(field: Any) -> Any:
    # path params always required
    return Path(default=..., **common_kwargs(field))


def form_from_field(field: Any) -> Any:
    return Form(default=default_from_field(field), **common_kwargs(field))


def body_from_field(field: Any) -> Any:
    return Body(default=default_from_field(field), **common_kwargs(field))


# ....................... #


def build_query_parameter(field_name: str, field: Any) -> inspect.Parameter:
    return inspect.Parameter(
        name=field_name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=field.annotation,
        default=query_from_field(field),
    )


def build_path_parameter(field_name: str, field: Any) -> inspect.Parameter:
    return inspect.Parameter(
        name=field_name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=field.annotation,
        default=path_from_field(field),
    )


def build_header_parameter(field_name: str, field: Any) -> inspect.Parameter:
    return inspect.Parameter(
        name=field_name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=field.annotation,
        default=header_from_field(field),
    )


def build_cookie_parameter(field_name: str, field: Any) -> inspect.Parameter:
    return inspect.Parameter(
        name=field_name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=field.annotation,
        default=cookie_from_field(field),
    )


def build_form_parameter(field_name: str, field: Any) -> inspect.Parameter:
    return inspect.Parameter(
        name=field_name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=field.annotation,
        default=form_from_field(field),
    )


def build_body_parameters(
    body_model: type[BaseModel],
    body_mode: HttpBodyMode,
) -> list[inspect.Parameter]:
    params: list[inspect.Parameter] = []

    match body_mode:
        case "json":
            params.append(
                inspect.Parameter(
                    name=HTTP_BODY_KEY,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                    annotation=body_model,
                    default=Body(...),
                )
            )

        case "form":
            for field_name, field in body_model.model_fields.items():
                params.append(build_form_parameter(field_name, field))

    return params


def build_dependency_parameter(
    name: str,
    annotation: Any,
    dependency: Any,
) -> inspect.Parameter:
    return inspect.Parameter(
        name=name,
        kind=inspect.Parameter.KEYWORD_ONLY,
        annotation=annotation,
        default=Depends(dependency),
    )
