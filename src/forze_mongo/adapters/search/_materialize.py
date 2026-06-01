"""Shared hit materialization for Mongo search adapters."""

from __future__ import annotations

from typing import Any, Sequence, TypeVar

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.base.serialization import (
    PydanticRecordMappingCodec,
    RecordMappingCodec,
    materialize_mapping_rows,
)

# ----------------------- #

M = TypeVar("M", bound=BaseModel)


def materialize_search_page(
    *,
    page_rows: list[JsonDict],
    pool: list[M] | None,
    u: int,
    page_limit: int,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    model_type: type[M],
    row_codec: RecordMappingCodec[Any, Any] | None,
) -> list[Any] | list[JsonDict]:
    """Build the API page payload after optional snapshot storage."""

    codec = row_codec or PydanticRecordMappingCodec(model_type)

    return materialize_mapping_rows(
        row_codec=codec,
        model_type=model_type,
        page_rows=page_rows,
        pool=pool,
        u=u,
        page_limit=page_limit,
        return_type=return_type,
        return_fields=return_fields,
    )
