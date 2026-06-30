"""Threading per-leg highlights through the federated RRF merge.

Each federated leg is run as a full ``SearchQueryPort`` (so it computes its own
``page.highlights``); the coordinator merges/dedupes hits with
:meth:`SearchResultSnapshot.weighted_rrf_merge_rows`, keyed by
:meth:`SearchResultSnapshot.federated_record_key_string`. These helpers re-associate each
surviving merged hit with its originating leg's highlight by that same key.
"""

from __future__ import annotations

from typing import Any, Sequence

from forze.application.contracts.base import HitHighlights
from forze.application.contracts.search import FederatedSearchReadModel

from .snapshot import SearchResultSnapshot

# ----------------------- #


def build_federated_highlight_index(
    leg_pages: Sequence[tuple[str, Any]],
) -> dict[str, HitHighlights]:
    """Index ``{federated_record_key: HitHighlights}`` over every leg's per-hit highlights.

    *leg_pages* is ``(member_name, leg_page)``; a leg whose page has no highlights contributes
    nothing. The key matches the dedup key the RRF merge uses, so lookups line up exactly.
    """

    index: dict[str, HitHighlights] = {}

    for member, page in leg_pages:
        highlights = getattr(page, "highlights", None)
        if not highlights:
            continue

        for hit, hl in zip(page.hits, highlights, strict=True):
            key = SearchResultSnapshot.federated_record_key_string(member, hit)
            index[key] = hl

    return index


def federated_highlights_for_hits(
    final_hits: Sequence[FederatedSearchReadModel[Any]],
    index: dict[str, HitHighlights],
) -> list[HitHighlights] | None:
    """Per-hit highlights aligned with the merged+windowed federated hits, or ``None``.

    ``None`` when no leg produced highlights; otherwise index-aligned with *final_hits*
    (a hit whose leg had no highlight maps to ``{}`` so the list stays non-sparse).
    """

    if not index:
        return None

    return [
        index.get(
            SearchResultSnapshot.federated_record_key_string(item.member, item.hit),
            {},
        )
        for item in final_hits
    ]
