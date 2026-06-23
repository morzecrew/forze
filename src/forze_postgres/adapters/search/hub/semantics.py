"""Canonical hub combo merge, match, order, and SQL expression semantics."""

from __future__ import annotations

from functools import cmp_to_key
from typing import TYPE_CHECKING, Any, Literal, Sequence

from forze_postgres._compat import require_psycopg

require_psycopg()

from psycopg import sql

from forze.application.contracts.querying import (
    QuerySortExpression,
    normalize_sorts_for_keyset,
    resolve_effective_sorts,
)
from forze.application.contracts.querying.pagination.cursor_token import (
    compare_keyset_sort_values,
    row_value_for_sort_key,
)
from forze.application.contracts.search import ranked_search_cursor_key_spec
from forze.domain.constants import ID_FIELD

from .constants import HUB_RANK, LEG_EID, LEG_SCORE

if TYPE_CHECKING:
    from pydantic import BaseModel

    from .runtime import HubLegRuntime

# ----------------------- #

HubCombine = Literal["or", "and"]
HubScoreMerge = Literal["max", "sum"]

# ....................... #


def leg_contribution(
    leg: HubLegRuntime,
    hub_row: dict[str, Any],
    leg_by_eid: dict[Any, float],
) -> tuple[float, bool]:
    """Per-leg raw score and matched flag (before member weight)."""

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


def aggregate_rank(
    per_leg_weighted_scores: Sequence[float],
    score_merge: HubScoreMerge,
) -> float:
    if score_merge == "max":
        return max(per_leg_weighted_scores) if per_leg_weighted_scores else 0.0

    return sum(per_leg_weighted_scores)


# ....................... #


def passes_combine(per_leg_matched: Sequence[bool], combine: HubCombine) -> bool:
    if combine == "or":
        return any(per_leg_matched)

    return all(per_leg_matched)


# ....................... #


def hub_order_key_spec(
    *,
    do_legs: bool,
    sorts: QuerySortExpression | None,  # type: ignore[valid-type]
    default_sort: QuerySortExpression | None,  # type: ignore[valid-type]
    read_fields: frozenset[str],
    spec_name: str,
    rank_field: str,
    model: type[BaseModel] | None = None,
) -> list[tuple[str, str]]:
    if not do_legs:
        effective = resolve_effective_sorts(
            sorts=sorts,
            default_sort=default_sort,
            read_fields=read_fields,
            spec_name=spec_name,
            model=model,
        )
        return [
            (k, d)
            for k, d, _ in normalize_sorts_for_keyset(
                effective,
                read_fields=read_fields,
                model=model,
            )
        ]

    user_sorts = sorts if sorts else default_sort

    return ranked_search_cursor_key_spec(
        rank_field=rank_field,
        sorts=user_sorts,
        read_fields=read_fields,
    )


# ....................... #


def compare_hub_rows(
    a: dict[str, Any],
    b: dict[str, Any],
    key_spec: Sequence[tuple[str, str]],
) -> int:
    for key, direction in key_spec:
        cmp = compare_keyset_sort_values(
            row_value_for_sort_key(a, key),
            row_value_for_sort_key(b, key),
        )

        if cmp == 0:
            continue

        return cmp if direction == "asc" else -cmp

    return 0


# ....................... #


def sort_hub_rows(
    rows: list[dict[str, Any]],
    *,
    key_spec: Sequence[tuple[str, str]],
) -> None:
    if not rows:
        return

    rows.sort(key=cmp_to_key(lambda a, b: compare_hub_rows(a, b, key_spec)))


# ....................... #


def _hub_row_key(
    row: dict[str, Any], *, read_fields: frozenset[str]
) -> tuple[Any, ...]:
    if ID_FIELD in read_fields:
        return (row[ID_FIELD],)

    return tuple(row[f] for f in sorted(read_fields))


# ....................... #


def merge_hub_leg_row_lists(
    *,
    leg_rows: Sequence[Sequence[dict[str, Any]]],
    weights: Sequence[float],
    score_merge: HubScoreMerge,
    combine: HubCombine,
    read_fields: frozenset[str],
    rank_field: str = HUB_RANK,
) -> list[dict[str, Any]]:
    """Merge per-leg hub row lists (single-FK INNER JOIN fast path)."""

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

        if not passes_combine(matched, combine):
            continue

        vals = [float(s) if s is not None else 0.0 for s in scores]
        row = dict(base)
        row[rank_field] = aggregate_rank(vals, score_merge)
        out.append(row)

    out.sort(key=lambda r: float(r.get(rank_field) or 0.0), reverse=True)
    return out


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
            raw, matched = leg_contribution(leg, hub_row, by_eid)
            per_leg_scores.append(raw * weight)
            per_leg_matched.append(matched)

        if not passes_combine(per_leg_matched, combine):
            continue

        row = dict(hub_row)
        row[rank_field] = aggregate_rank(per_leg_scores, score_merge)
        out.append(row)

    return out


# ....................... #
# SQL emitters (combo CTE)


def sql_leg_coalesce(leg: HubLegRuntime, leg_index: int) -> sql.Composable:
    if len(leg.hub_fk_columns) == 1:
        return sql.SQL("COALESCE({}.{}, 0)").format(
            sql.Identifier(f"lp{leg_index}"),
            sql.Identifier(LEG_SCORE),
        )

    br = [
        sql.SQL("COALESCE({}.{}, 0)").format(
            sql.Identifier(f"lp{leg_index}_{j}"),
            sql.Identifier(LEG_SCORE),
        )
        for j in range(len(leg.hub_fk_columns))
    ]

    return sql.SQL("GREATEST({})").format(sql.SQL(", ").join(br))


# ....................... #


def sql_leg_matched(leg: HubLegRuntime, leg_index: int) -> sql.Composable:
    if len(leg.hub_fk_columns) == 1:
        return sql.SQL("{} IS NOT NULL").format(
            sql.SQL("{}.{}").format(
                sql.Identifier(f"lp{leg_index}"),
                sql.Identifier(LEG_EID),
            ),
        )

    eid_null = [
        sql.SQL("{} IS NOT NULL").format(
            sql.SQL("{}.{}").format(
                sql.Identifier(f"lp{leg_index}_{j}"),
                sql.Identifier(LEG_EID),
            ),
        )
        for j in range(len(leg.hub_fk_columns))
    ]
    return sql.SQL("({})").format(sql.SQL(" OR ").join(eid_null))


# ....................... #


def sql_merge_expr(
    active: Sequence[tuple[int, HubLegRuntime, float]],
    score_merge: HubScoreMerge,
) -> sql.Composable:
    score_terms = [
        sql.SQL("({}) * {}").format(
            sql_leg_coalesce(leg, i),
            sql.Literal(float(w)),
        )
        for i, leg, w in active
    ]

    if score_merge == "max":
        return sql.SQL("GREATEST({})").format(sql.SQL(", ").join(score_terms))

    return sql.SQL("({})").format(sql.SQL(" + ").join(score_terms))


# ....................... #


def sql_combine_where(
    active: Sequence[tuple[int, HubLegRuntime, float]],
    combine: HubCombine,
) -> sql.Composable:
    leg_null_checks = [sql_leg_matched(leg, i) for i, leg, _ in active]

    if combine == "or":
        return sql.SQL(" OR ").join(leg_null_checks)

    return sql.SQL(" AND ").join(leg_null_checks)
