"""Pure-Python hub leg score merge (parallel execution path)."""

from __future__ import annotations

from typing import Any, Literal, Sequence

from forze.domain.constants import ID_FIELD

# ----------------------- #

HubCombine = Literal["or", "and"]
HubScoreMerge = Literal["max", "sum"]


def _hub_row_key(row: dict[str, Any], *, read_fields: frozenset[str]) -> tuple[Any, ...]:
    if ID_FIELD in read_fields:
        return (row[ID_FIELD],)

    return tuple(row[f] for f in sorted(read_fields))


def merge_hub_leg_rows(
    *,
    leg_rows: Sequence[Sequence[dict[str, Any]]],
    weights: Sequence[float],
    score_merge: HubScoreMerge,
    combine: HubCombine,
    read_fields: frozenset[str],
    rank_field: str = "_hub_rank",
) -> list[dict[str, Any]]:
    """Merge per-leg hub row lists into one ranked list (same semantics as SQL ``combo``)."""

    if not leg_rows:
        return []

    n_legs = len(leg_rows)
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    leg_scores: dict[tuple[Any, ...], list[float | None]] = {}
    leg_matched: dict[tuple[Any, ...], list[bool]] = {}

    for leg_idx, rows in enumerate(leg_rows):
        weight = float(weights[leg_idx]) if leg_idx < len(weights) else 0.0

        for row in rows:
            key = _hub_row_key(row, read_fields=read_fields)
            merged.setdefault(key, dict(row))
            scores = leg_scores.setdefault(key, [None] * n_legs)
            matched = leg_matched.setdefault(key, [False] * n_legs)
            raw = row.get(rank_field)

            if raw is None:
                continue

            scores[leg_idx] = float(raw) * weight
            matched[leg_idx] = True

    out: list[dict[str, Any]] = []

    for key, base in merged.items():
        scores = leg_scores[key]
        matched = leg_matched[key]

        if combine == "or" and not any(matched):
            continue

        if combine == "and" and not all(matched):
            continue

        vals = [float(s) if s is not None else 0.0 for s in scores]

        if score_merge == "max":
            rank = max(vals) if vals else 0.0
        else:
            rank = sum(vals)

        row = dict(base)
        row[rank_field] = rank
        out.append(row)

    out.sort(key=lambda r: float(r.get(rank_field) or 0.0), reverse=True)

    return out
