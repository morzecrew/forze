"""Search total-count policy helpers for Postgres ranked search."""

from __future__ import annotations

from typing import Literal

from forze.application.contracts.search import SearchOptions

SearchCountPolicy = Literal["exact", "approximate", "none"]

# ----------------------- #


def effective_search_count(options: SearchOptions | None) -> SearchCountPolicy:
    """Resolve how ranked search should populate page totals."""

    raw = (options or {}).get("search_count", "exact")

    if raw in ("exact", "approximate", "none"):
        return raw

    return "exact"
