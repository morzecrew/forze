"""In-memory stub for SearchReadPort."""

from typing import Any, Optional, Sequence, final

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchOptions
from forze.base.primitives import JsonDict
from pydantic import BaseModel

# ----------------------- #


@final
class InMemorySearchReadPort:
    """In-memory search port for unit tests. Implements :class:`SearchReadPort`.

    Stores mock hits keyed by query; returns empty results when no match.
    Call :meth:`add_hits` to seed data for tests.
    """

    def __init__(self) -> None:
        self._hits: dict[str, list[JsonDict | BaseModel]] = {}
        self._default_hits: list[JsonDict | BaseModel] = []

    def add_hits(self, query: str, hits: list[JsonDict | BaseModel]) -> None:
        """Seed hits for a query. Used by tests to simulate search results."""
        self._hits[query] = hits

    def set_default_hits(self, hits: list[JsonDict | BaseModel]) -> None:
        """Set default hits returned when query has no seeded data."""
        self._default_hits = hits

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        options: Optional[SearchOptions] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[Any], int]:
        hits = list(self._hits.get(query, self._default_hits))
        count = len(hits)

        if offset is not None:
            hits = hits[offset:]
        if limit is not None:
            hits = hits[:limit]

        if return_fields:
            out: list[JsonDict] = []
            for h in hits:
                if isinstance(h, dict):
                    out.append({k: h[k] for k in return_fields if k in h})
                else:
                    d = h.model_dump() if hasattr(h, "model_dump") else {}
                    out.append({k: d[k] for k in return_fields if k in d})
            return out, count
        return hits, count
