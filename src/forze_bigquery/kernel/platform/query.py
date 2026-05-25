"""Build BigQuery REST query requests and parameter bindings."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

# ----------------------- #


def params_to_query_parameters(params: BaseModel) -> list[JsonDict]:
    """Convert a Pydantic params model to BigQuery ``queryParameters`` entries."""

    out: list[JsonDict] = []

    for name, value in params.model_dump().items():
        param_type, param_value = _infer_parameter(value)
        out.append(
            {
                "name": name,
                "parameterType": param_type,
                "parameterValue": param_value,
            }
        )

    return out


# ....................... #


def _infer_parameter(value: Any) -> tuple[JsonDict, JsonDict]:
    if value is None:
        return {"type": "STRING"}, {"value": None}

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
        if not value:
            return {"type": "ARRAY"}, {"arrayValues": []}
        elem_type, _ = _infer_parameter(value[0])
        return {
            "type": "ARRAY",
            "arrayType": elem_type,
        }, {
            "arrayValues": [
                _infer_parameter(item)[1] for item in value  # type: ignore[arg-type]
            ],
        }

    raise CoreError(
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
) -> JsonDict:
    """Build a body for ``POST .../projects/{project}/queries``."""

    body: JsonDict = {
        "query": sql,
        "useLegacySql": use_legacy_sql,
        "dryRun": dry_run,
    }

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


# ....................... #


def build_count_sql(inner_sql: str) -> str:
    """Wrap *inner_sql* in ``SELECT COUNT(*)`` for total row counts."""

    stripped = inner_sql.strip().rstrip(";")
    return (
        f"SELECT COUNT(*) AS forze_cnt FROM ({stripped}) AS forze_analytics_subq"  # nosec B608
    )
