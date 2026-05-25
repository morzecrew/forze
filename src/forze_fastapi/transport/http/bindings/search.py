"""HTTP bindings for search operations."""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import Depends
from fastapi.params import Body
from pydantic import BaseModel

from forze.application.composition.search import SearchOperationEntry
from forze.application.composition.search.facades import SearchFacade
from forze.application.composition.search.value_objects import SearchDTOs
from forze.application.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
)
from forze.application.handlers.search import ProjectedSearchPaginated
from forze_fastapi.transport.http.router import HttpMethod

# ----------------------- #

RawPaginated = ProjectedSearchPaginated
RawCursorPaginated = ProjectedCursorPaginated


@dataclass(frozen=True, slots=True)
class SearchHttpBinding:
    method: HttpMethod
    default_path: str
    response_factory: Callable[[SearchDTOs[Any]], type[Any]]


SEARCH_HTTP_BINDINGS: dict[str, SearchHttpBinding] = {
    "search": SearchHttpBinding(
        "POST",
        "/search",
        lambda dtos: Paginated[dtos.read],  # type: ignore[name-defined]
    ),
    "raw_search": SearchHttpBinding("POST", "/raw-search", lambda _dtos: RawPaginated),
    "search_cursor": SearchHttpBinding(
        "POST",
        "/search-cursor",
        lambda dtos: CursorPaginated[dtos.read],  # type: ignore[name-defined]
    ),
    "raw_search_cursor": SearchHttpBinding(
        "POST",
        "/raw-search-cursor",
        lambda _dtos: RawCursorPaginated,
    ),
}


def search_binding_for(entry: SearchOperationEntry) -> SearchHttpBinding:
    return SEARCH_HTTP_BINDINGS[entry.enable_name]


def make_search_endpoint[M: BaseModel](
    body_type: type[Any],
    facade_attr: str,
    facade_dep: Callable[..., SearchFacade[M]],
) -> Callable[..., Any]:
    async def _endpoint(
        body: body_type = Body(),  # type: ignore[valid-type,assignment]
        search: SearchFacade[M] = Depends(facade_dep),
    ) -> Any:
        return await getattr(search, facade_attr)(body)

    return _endpoint  # type: ignore[return-value]
