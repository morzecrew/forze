"""In-memory facet & highlight computation for the mock search adapter.

This is the **reference oracle**: the simplest correct semantics every other backend
(Postgres, Meilisearch, OpenSearch) is held to by the cross-backend parity harness.

- *Facets* — term (value) distributions over the **full matching set**, independent of
  the page window; buckets ordered count-descending then value-ascending, capped.
- *Highlights* — per returned hit, the searchable field text with each matched query
  token wrapped in the requested ``pre_tag`` / ``post_tag`` markers.
"""

from __future__ import annotations

from typing import Any, Sequence

from forze.application.contracts.base import FacetBucket, FacetResults, HitHighlights
from forze.application.contracts.search import (
    SearchOptions,
    facet_size_of,
    highlight_tokens,
    mark_highlight,
)
from forze.base.primitives import JsonDict
from forze_mock.query.matching import (
    _MISSING,  # type: ignore[reportPrivateUsage]
    _path_get,  # type: ignore[reportPrivateUsage]
    _path_text,  # type: ignore[reportPrivateUsage]
)

# ----------------------- #


def compute_facets(
    rows: Sequence[JsonDict],
    fields: Sequence[str],
    *,
    options: SearchOptions | None,
) -> FacetResults:
    """Term distributions over *rows* (the full matching set) for each facet field.

    Null/missing values produce no bucket. A list-valued field contributes one count per
    element (multi-valued faceting). Buckets are ordered count-desc, value-asc, then capped.
    """

    size = facet_size_of(options)
    results: dict[str, tuple[FacetBucket, ...]] = {}

    for field in fields:
        counts: dict[Any, int] = {}
        for row in rows:
            value = _path_get(row, field)
            if value is _MISSING or value is None:
                continue

            for atom in _facet_atoms(value):
                counts[atom] = counts.get(atom, 0) + 1

        # Tie-break by the real value (so 2 sorts before 10), grouping by type name first
        # to keep a total order across the mixed value types a facet field may hold.
        ordered = sorted(
            counts.items(), key=lambda kv: (-kv[1], type(kv[0]).__name__, kv[0])
        )
        results[field] = tuple(
            FacetBucket(value=value, count=count) for value, count in ordered[:size]
        )

    return results


# ....................... #


def _facet_atoms(value: Any) -> list[str | int | float | bool]:
    """Hashable facet atoms for a stored value (scalar → itself; list → each element)."""

    if isinstance(value, (str, int, float, bool)):
        return [value]

    if isinstance(value, (list, tuple)):
        items: list[Any] = list(value)  # pyright: ignore[reportUnknownArgumentType]
        return [v for v in items if isinstance(v, (str, int, float, bool))]

    return []


# ....................... #


def compute_highlights(
    rows: Sequence[JsonDict],
    terms: Sequence[str],
    fields: Sequence[str],
    *,
    pre_tag: str,
    post_tag: str,
) -> list[HitHighlights]:
    """Per-row highlighted fragments (index-aligned with *rows*).

    Each field's text gets every matched query token wrapped in the markers; a field with
    no match is omitted, a row with no matches maps to ``{}`` — so the list stays
    index-aligned and non-sparse.
    """

    tokens = highlight_tokens(terms)

    out: list[HitHighlights] = []

    for row in rows:
        marked: dict[str, tuple[str, ...]] = {}

        if tokens:
            for field in fields:
                text = _path_text(row, field)

                if not text:
                    continue
                fragment = mark_highlight(
                    text, tokens, pre_tag=pre_tag, post_tag=post_tag
                )

                if fragment is not None:
                    marked[field] = (fragment,)

        out.append(marked)

    return out
