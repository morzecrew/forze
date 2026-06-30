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

from forze.application.contracts.search import (
    FacetBucket,
    FacetResults,
    HitHighlights,
    SearchOptions,
    facet_size_of,
)
from forze.application.contracts.search import (
    compute_highlights as shared_compute_highlights,
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

    Delegates to the shared :func:`~forze.application.contracts.search.compute_highlights`
    with the mock's nested-path text accessor, so the oracle and the relational backends wrap
    identically. A field with no match is omitted; a row with none maps to ``{}``.
    """

    return shared_compute_highlights(
        rows,
        terms,
        fields,
        pre_tag=pre_tag,
        post_tag=post_tag,
        get_text=_path_text,
    )
