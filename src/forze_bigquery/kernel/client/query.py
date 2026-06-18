"""Build BigQuery REST query requests and parameter bindings."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

from forze.application.integrations.analytics.sql import (
    build_count_sql as build_count_sql,  # thin re-export of the shared builder
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #


_SCALAR_BQ_TYPE: dict[type, str] = {
    bool: "BOOL",
    int: "INT64",
    float: "FLOAT64",
    Decimal: "NUMERIC",
    datetime: "TIMESTAMP",
    date: "DATE",
    UUID: "STRING",
    str: "STRING",
    bytes: "BYTES",
}


def params_to_query_parameters(params: BaseModel | JsonDict) -> list[JsonDict]:
    """Convert a params model (or already-lowered dict) to BigQuery ``queryParameters``.

    When *params* is a Pydantic model, each field's declared annotation guides
    type inference -- so an empty list still emits a typed ``ARRAY`` and a
    ``None`` carries the field's real type. A raw dict has no annotations and
    falls back to value-based inference.
    """

    if isinstance(params, BaseModel):
        data = params.model_dump()
        annotations = {
            name: field.annotation
            for name, field in type(params).model_fields.items()
        }
    else:
        data = dict(params)
        annotations = {}

    out: list[JsonDict] = []

    for name, value in data.items():
        param_type, param_value = _infer_parameter(value, annotations.get(name))
        out.append(
            {
                "name": name,
                "parameterType": param_type,
                "parameterValue": param_value,
            }
        )

    return out


# ....................... #


def _unwrap_optional(annotation: Any) -> Any:
    """Strip a single ``Optional[...]`` / ``X | None`` wrapper, else return as-is."""

    args = get_args(annotation)
    if args and type(None) in args:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]

    return annotation


def _list_elem_annotation(annotation: Any) -> Any | None:
    """Element annotation of a ``list[...]`` / ``tuple[...]`` field, else ``None``."""

    if annotation is None:
        return None

    annotation = _unwrap_optional(annotation)
    if get_origin(annotation) in (list, tuple):
        args = [a for a in get_args(annotation) if a is not type(None)]
        return args[0] if args else None

    return None


def _bq_type_for_annotation(annotation: Any) -> JsonDict | None:
    """Map a Python annotation to a BigQuery ``parameterType``, or ``None`` if unknown."""

    if annotation is None:
        return None

    annotation = _unwrap_optional(annotation)
    origin = get_origin(annotation)

    if origin in (list, tuple):
        elem = _list_elem_annotation(annotation)
        elem_type = _bq_type_for_annotation(elem) if elem is not None else None
        return None if elem_type is None else {"type": "ARRAY", "arrayType": elem_type}

    bq = _SCALAR_BQ_TYPE.get(annotation)
    return None if bq is None else {"type": bq}


# ....................... #


def _infer_parameter(value: Any, annotation: Any = None) -> tuple[JsonDict, JsonDict]:
    if value is None:
        return (_bq_type_for_annotation(annotation) or {"type": "STRING"}), {"value": None}

    if isinstance(value, bool):
        return {"type": "BOOL"}, {"value": value}

    if isinstance(value, int):
        return {"type": "INT64"}, {"value": str(value)}

    if isinstance(value, float):
        return {"type": "FLOAT64"}, {"value": value}

    if isinstance(value, Decimal):
        return {"type": "NUMERIC"}, {"value": str(value)}

    if isinstance(value, datetime):
        return {"type": "TIMESTAMP"}, {"value": value.isoformat()}

    if isinstance(value, date):
        return {"type": "DATE"}, {"value": value.isoformat()}

    if isinstance(value, UUID):
        return {"type": "STRING"}, {"value": str(value)}

    if isinstance(value, str):
        return {"type": "STRING"}, {"value": value}

    if isinstance(value, (list, tuple)):
        elem_annotation = _list_elem_annotation(annotation)
        elem_type = (
            _bq_type_for_annotation(elem_annotation)
            if elem_annotation is not None
            else None
        )

        if not value:
            # Empty arrays carry no value to infer from; BigQuery still requires
            # ``arrayType``, so a typed list[...] field is needed here.
            if elem_type is None:
                raise exc.precondition(
                    "Cannot infer BigQuery ARRAY element type for an empty list "
                    "without a typed list parameter field."
                )

            return {"type": "ARRAY", "arrayType": elem_type}, {"arrayValues": []}

        if elem_type is None:
            # No annotation: infer from the first non-null element (a leading
            # ``None`` must not force the whole array to STRING).
            sample = next((v for v in value if v is not None), value[0])
            elem_type, _ = _infer_parameter(sample, elem_annotation)

        return (
            {"type": "ARRAY", "arrayType": elem_type},
            {
                "arrayValues": [
                    _infer_parameter(item, elem_annotation)[1] for item in value
                ],
            },
        )

    raise exc.precondition(
        f"Unsupported BigQuery query parameter type: {type(value).__name__}"
    )


# ....................... #


def build_sync_query_request(
    sql: str,
    *,
    query_parameters: list[JsonDict] | None = None,
    dry_run: bool = False,
    use_legacy_sql: bool = False,
    maximum_bytes_billed: int | None = None,
    max_results: int | None = None,
    start_index: int | None = None,
    page_token: str | None = None,
    timeout_ms: int | None = None,
    default_dataset: str | None = None,
) -> JsonDict:
    """Build a body for ``POST .../projects/{project}/queries``."""

    body: JsonDict = {
        "query": sql,
        "useLegacySql": use_legacy_sql,
        "dryRun": dry_run,
    }

    if default_dataset is not None:
        body["defaultDataset"] = {"datasetId": default_dataset}

    if query_parameters:
        body["parameterMode"] = "NAMED"
        body["queryParameters"] = query_parameters

    if maximum_bytes_billed is not None:
        body["maximumBytesBilled"] = str(maximum_bytes_billed)

    if max_results is not None:
        body["maxResults"] = max_results

    if start_index is not None:
        body["startIndex"] = str(start_index)

    if page_token is not None:
        body["pageToken"] = page_token

    if timeout_ms is not None:
        body["timeoutMs"] = timeout_ms

    return body
