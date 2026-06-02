"""Python hub combo merge for parallel execution (SQL ``combo`` parity)."""

from typing import Any, Sequence

from forze.application.contracts.querying import QuerySortExpression
from forze.domain.constants import ID_FIELD

from .constants import HUB_RANK
from .merge import HubCombine, HubScoreMerge, merge_hub_leg_rows
from .runtime import HubLegRuntime

# ----------------------- #


def _leg_score_for_hub_row(
    leg: HubLegRuntime,
    hub_row: dict[str, Any],
    leg_by_eid: dict[Any, float],
) -> tuple[float, bool]:
    """Per-leg score and matched flag (``merge_coalesce`` / ``merge_matched`` semantics)."""

    if len(leg.hub_fk_columns) == 1:
        col = leg.hub_fk_columns[0]
        eid = hub_row.get(col)

        if eid is None:
            return 0.0, False

        raw = leg_by_eid.get(eid)

        if raw is None:
            return 0.0, False

        return float(raw), True

    branch_scores: list[float] = []
    any_match = False

    for col in leg.hub_fk_columns:
        eid = hub_row.get(col)

        if eid is None:
            branch_scores.append(0.0)
            continue

        raw = leg_by_eid.get(eid)

        if raw is None:
            branch_scores.append(0.0)

        else:
            branch_scores.append(float(raw))
            any_match = True

    if not branch_scores:
        return 0.0, False

    return max(branch_scores), any_match


# ....................... #


def merge_hub_combo_rows(
    *,
    hub_rows: Sequence[dict[str, Any]],
    leg_ranked: Sequence[tuple[HubLegRuntime, dict[Any, float]]],
    weights: Sequence[float],
    score_merge: HubScoreMerge,
    combine: HubCombine,
    read_fields: frozenset[str],
    rank_field: str = HUB_RANK,
) -> list[dict[str, Any]]:
    """Merge hub rows with per-leg ranked ``eid -> score`` maps (multi-FK safe)."""

    out: list[dict[str, Any]] = []

    for hub_row in hub_rows:
        per_leg_scores: list[float] = []
        per_leg_matched: list[bool] = []

        for leg_idx, (leg, by_eid) in enumerate(leg_ranked):
            weight = float(weights[leg_idx]) if leg_idx < len(weights) else 0.0
            raw, matched = _leg_score_for_hub_row(leg, hub_row, by_eid)
            per_leg_scores.append(raw * weight)
            per_leg_matched.append(matched)

        if combine == "or" and not any(per_leg_matched):
            continue

        if combine == "and" and not all(per_leg_matched):
            continue

        if score_merge == "max":
            rank = max(per_leg_scores) if per_leg_scores else 0.0
        else:
            rank = sum(per_leg_scores)

        row = dict(hub_row)
        row[rank_field] = rank
        out.append(row)

    return out


# ....................... #


def sort_merged_hub_rows(
    rows: list[dict[str, Any]],
    *,
    do_legs: bool,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    read_fields: frozenset[str],
    column_types: Any,
    model_type: type,
    nested_field_hints: Any,
    rank_field: str = HUB_RANK,
) -> None:
    """In-place sort aligned with :meth:`HubSearchSqlMixin._hub_order_sql_for_search`."""

    if not rows:
        return

    def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        parts: list[Any] = []

        if do_legs:
            parts.append(-float(row.get(rank_field) or 0.0))

        if sorts:
            for field, order in sorts.items():
                if field == rank_field and do_legs:
                    continue
                val = row.get(field)
                asc = (order or "").lower() == "asc"
                parts.append((0 if asc else 1, val))

        if not do_legs:
            if ID_FIELD in read_fields:
                parts.append(row.get(ID_FIELD))

            else:
                parts.append(tuple(row.get(f) for f in sorted(read_fields)))

        return tuple(parts)

    rows.sort(key=sort_key)


# ....................... #


def merge_hub_leg_row_lists(
    *,
    leg_rows: Sequence[Sequence[dict[str, Any]]],
    legs: Sequence[HubLegRuntime],
    weights: Sequence[float],
    score_merge: HubScoreMerge,
    combine: HubCombine,
    read_fields: frozenset[str],
    rank_field: str = HUB_RANK,
) -> list[dict[str, Any]]:
    """Fast path when each leg query returned hub columns (single-FK INNER JOIN)."""

    return merge_hub_leg_rows(
        leg_rows=leg_rows,
        weights=weights,
        score_merge=score_merge,
        combine=combine,
        read_fields=read_fields,
        rank_field=rank_field,
    )
