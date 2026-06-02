"""Shared hit materialization for Postgres search adapters."""

from typing import Any, Sequence, TypeVar

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.base.serialization import (
    ModelCodec,
    default_model_codec,
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
    codec: ModelCodec[Any, Any] | None,
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
    :param codec: Search spec row codec for decode/materialization.
    :returns: Either a list of Pydantic models or plain dict projections.
    """

    resolved = codec or default_model_codec(model_type)

    return materialize_mapping_rows(
        codec=resolved,
        model_type=model_type,
        page_rows=page_rows,
        pool=pool,
        u=u,
        page_limit=page_limit,
        return_type=return_type,
        return_fields=return_fields,
    )
