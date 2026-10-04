"""Threading per-leg highlights through the federated RRF merge.

Each federated leg is run as a full ``SearchQueryPort`` (so it computes its own
``page.highlights``); the coordinator merges/dedupes hits with
:meth:`SearchResultSnapshot.weighted_rrf_merge_rows`, keyed by
:meth:`SearchResultSnapshot.federated_merge_keys`. These helpers re-associate each
surviving merged hit with its originating leg's highlight by that same key.
"""

from __future__ import annotations

from collections.abc import Hashable, Sequence
from typing import Any

from forze.application.contracts.search import (
    FederatedSearchReadModel,
    HitHighlights,
)

from .snapshot import SearchResultSnapshot

# ----------------------- #


def build_federated_highlight_index(
    leg_pages: Sequence[tuple[str, Any]],
) -> dict[Hashable, HitHighlights]:
    """Index ``{federated_merge_keys: HitHighlights}`` over every leg's per-hit highlights.

    *leg_pages* is ``(member_name, leg_page)``; a leg whose page has no highlights contributes
    nothing. The key matches the dedup key the RRF merge uses, so lookups line up exactly;
    a record repeated within a leg keeps its first highlight, as the merge keeps its first hit.
    """

    index: dict[Hashable, HitHighlights] = {}

    for member, page in leg_pages:
        highlights = getattr(page, "highlights", None)
        if not highlights:
            continue

        keys = SearchResultSnapshot.federated_merge_keys(member, page.hits)

        for key, hl in zip(keys, highlights, strict=True):
            index.setdefault(key, hl)

    return index


def federated_highlights_for_hits(
    final_hits: Sequence[FederatedSearchReadModel[Any]],
    index: dict[Hashable, HitHighlights],
) -> list[HitHighlights] | None:
    """Per-hit highlights aligned with the merged+windowed federated hits, or ``None``.

    ``None`` when no leg produced highlights; otherwise index-aligned with *final_hits*
    (a hit whose leg had no highlight maps to ``{}`` so the list stays non-sparse).
    """

    if not index:
        return None

    found = SearchResultSnapshot.federated_merge_key_lookup

    return [found(index, item.member, item.hit) or {} for item in final_hits]
