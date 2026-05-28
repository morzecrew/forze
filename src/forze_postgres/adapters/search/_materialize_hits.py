"""Shared hit materialization for Postgres search adapters."""

from typing import Any, Sequence, TypeVar

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
    """Build the API page payload after optional snapshot storage.

    When ``pool`` is present (full result set already validated as ``model_type``),
    reuse ``pool[u : u + page_limit]`` instead of re-parsing ``page_rows`` for the
    default hit model.

    :param page_rows: Row dicts for the current page window (same order as SQL).
    :param pool: Full validated hit list when snapshot path ran, else :obj:`None`.
    :param u: Page offset used when slicing ``pool`` (must match row window).
    :param page_limit: Maximum slice length on ``pool`` for the page window.
    :param return_type: Optional projection model; when ``None``, use ``model_type``.
    :param return_fields: When set, build plain dict projections from ``page_rows``.
    :param model_type: Default read model for this adapter.
    :returns: Either a list of Pydantic models or plain dict projections.
    """

    if return_fields is not None:
        return [{k: r.get(k, None) for k in return_fields} for r in page_rows]

    if return_type is not None:
        if pool is not None and return_type == model_type:
            return pool[u : u + page_limit]

        return pydantic_validate_many(return_type, page_rows)

    if pool is not None:
        return pool[u : u + page_limit]

    return pydantic_validate_many(model_type, page_rows)
