"""Shared hit materialization for Mongo search adapters."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_validate_many

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
) -> list[Any] | list[JsonDict]:
    """Build the API page payload after optional snapshot storage."""

    if return_fields is not None:
        return [{k: r.get(k, None) for k in return_fields} for r in page_rows]

    if return_type is not None:
        if pool is not None and return_type == model_type:
            return pool[u : u + page_limit]

        return pydantic_validate_many(return_type, page_rows)

    if pool is not None:
        return pool[u : u + page_limit]

    return pydantic_validate_many(model_type, page_rows)
