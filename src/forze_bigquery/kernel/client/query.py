"""Build BigQuery REST query requests and parameter bindings."""

from __future__ import annotations

import base64
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
    # Annotation-driven default, used where there is no value to inspect (a typed
    # ``None``, an empty typed array). A concrete Decimal picks NUMERIC vs
    # BIGNUMERIC from its own exponent — see ``_decimal_parameter_type``.
    Decimal: "NUMERIC",
    datetime: "TIMESTAMP",
    date: "DATE",
    UUID: "STRING",
    str: "STRING",
    bytes: "BYTES",
}

# ....................... #

# BigQuery NUMERIC holds 9 fractional and 29 integer digits; BIGNUMERIC 38 and 38.
# NUMERIC is preferred where it is exact (narrower, and implicitly coercible
# wherever the wider type is accepted); a Decimal needing more scale or magnitude
# goes out as BIGNUMERIC, and one exceeding even that is refused — BigQuery would
# otherwise round the parameter server-side, silently changing a money value.
_NUMERIC_MAX_SCALE = 9
_NUMERIC_MAX_INTEGER_DIGITS = 29
_BIGNUMERIC_MAX_SCALE = 38
_BIGNUMERIC_MAX_INTEGER_DIGITS = 38


def _decimal_parameter_type(value: Decimal) -> str:
    """``NUMERIC`` or ``BIGNUMERIC`` for *value*, by its exponent/significant digits.

    The choice is about the numeric value, not its spelling — trailing zero digits
    are stripped first (``Decimal("1.000000000000")`` is scale 0, not 12), by hand
    rather than via ``Decimal.normalize()``, which rounds to the *context* precision
    (28 significant digits by default) and would misclassify wider exact values.
    Non-finite values and values no BigQuery decimal type can hold exactly raise
    ``precondition``.
    """

    if not value.is_finite():
        raise exc.precondition(f"Non-finite numeric not allowed: {value!r}")

    if value.is_zero():
        return "NUMERIC"

    _sign, digits, exponent = value.as_tuple()
    exponent = int(exponent)

    while len(digits) > 1 and digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1

    scale = max(0, -exponent)
    integer_digits = max(0, len(digits) + exponent)

    if scale <= _NUMERIC_MAX_SCALE and integer_digits <= _NUMERIC_MAX_INTEGER_DIGITS:
        return "NUMERIC"

    if scale <= _BIGNUMERIC_MAX_SCALE and integer_digits <= _BIGNUMERIC_MAX_INTEGER_DIGITS:
        return "BIGNUMERIC"

    raise exc.precondition(
        f"Decimal exceeds BigQuery BIGNUMERIC exactness ({integer_digits} integer "
        f"digits, scale {scale}; max {_BIGNUMERIC_MAX_INTEGER_DIGITS}/"
        f"{_BIGNUMERIC_MAX_SCALE}): {value!r}"
    )


# ....................... #


def params_to_query_parameters(params: BaseModel | JsonDict) -> list[JsonDict]:
    """Convert a params model (or already-lowered dict) to BigQuery ``queryParameters``.

    When *params* is a Pydantic model, each field's declared annotation guides
    type inference -- so an empty list still emits a typed ``ARRAY`` and a
    ``None`` carries the field's real type. A raw dict has no annotations and
    falls back to value-based inference.
    """

    if isinstance(params, BaseModel):
        data = params.model_dump()
        annotations = {name: field.annotation for name, field in type(params).model_fields.items()}
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


# ....................... #


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
        return {"type": _decimal_parameter_type(value)}, {"value": str(value)}

    if isinstance(value, datetime):
        return {"type": "TIMESTAMP"}, {"value": value.isoformat()}

    if isinstance(value, date):
        return {"type": "DATE"}, {"value": value.isoformat()}

    if isinstance(value, UUID):
        return {"type": "STRING"}, {"value": str(value)}

    if isinstance(value, str):
        return {"type": "STRING"}, {"value": value}

    if isinstance(value, (bytes, bytearray)):
        # BigQuery wire format encodes BYTES as base64.
        return {"type": "BYTES"}, {"value": base64.b64encode(bytes(value)).decode("ascii")}

    if isinstance(value, (list, tuple)):
        return _infer_array_parameter(value, annotation)

    raise exc.precondition(f"Unsupported BigQuery query parameter type: {type(value).__name__}")


# ....................... #


def _infer_array_parameter(value: Any, annotation: Any) -> tuple[JsonDict, JsonDict]:
    """Infer the ``ARRAY`` parameter type/value for a list or tuple *value*."""
    elem_annotation = _list_elem_annotation(annotation)
    elem_type = _bq_type_for_annotation(elem_annotation) if elem_annotation is not None else None

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

    # The annotation default and the first-sample inference both see one element;
    # the array's type must hold every element exactly, so any member needing
    # BIGNUMERIC widens the whole array.
    if elem_type.get("type") in ("NUMERIC", "BIGNUMERIC") and any(
        isinstance(item, Decimal) and _decimal_parameter_type(item) == "BIGNUMERIC"
        for item in value
    ):
        elem_type = {"type": "BIGNUMERIC"}

    return (
        {"type": "ARRAY", "arrayType": elem_type},
        {"arrayValues": [_infer_parameter(item, elem_annotation)[1] for item in value]},
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
