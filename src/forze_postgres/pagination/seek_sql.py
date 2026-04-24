"""PostgreSQL keyset seek fragments (psycopg :mod:`sql`)."""

from __future__ import annotations

from typing import Any, Literal

from psycopg import sql

from forze.base.errors import CoreError

Nav = Literal["after", "before"]


def build_seek_condition(
    exprs: list[sql.Composable],
    directions: list[str],
    values: list[Any],
    nav: Nav,
) -> tuple[sql.Composable, list[Any]]:
    """``after``: rows strictly after the cursor; ``before``: rows strictly before (reverse)."""
    n = len(exprs)
    if n != len(values) or n != len(directions) or n < 1:
        raise CoreError("Invalid keyset shape")

    after = nav == "after"
    parts: list[sql.Composable] = []
    out_params: list[Any] = []

    for i in range(n):
        prefix: list[sql.Composable] = []
        for j in range(i):
            prefix.append(
                sql.SQL("{} = {}").format(
                    exprs[j],
                    sql.Placeholder(),
                )
            )
            out_params.append(values[j])
        d = directions[i]
        is_asc = d == "asc"
        if after:
            want_gt = is_asc
        else:
            want_gt = not is_asc
        cmp_ = (
            sql.SQL("{} > {}").format(exprs[i], sql.Placeholder())
            if want_gt
            else sql.SQL("{} < {}").format(exprs[i], sql.Placeholder())
        )
        out_params.append(values[i])
        if prefix:
            anded = prefix[0]
            for p2 in prefix[1:]:
                anded = sql.SQL("({} AND {})").format(anded, p2)
            parts.append(sql.SQL("({} AND {})").format(anded, cmp_))
        else:
            parts.append(cmp_)

    if len(parts) == 1:
        return parts[0], out_params
    ored = parts[0]
    for p2 in parts[1:]:
        ored = sql.SQL("({} OR {})").format(ored, p2)
    return ored, out_params


def build_order_by_sql(
    exprs: list[sql.Composable],
    directions: list[str],
    *,
    flip: bool = False,
) -> sql.Composable:
    """Build ``ORDER BY`` from per-key expressions; *flip* reverses each direction."""
    parts: list[sql.Composable] = []
    for ex, d in zip(exprs, directions, strict=True):
        d_out: str = ("desc" if d == "asc" else "asc") if flip else d
        dir_st = "ASC" if d_out == "asc" else "DESC"
        parts.append(sql.SQL("{} {}").format(ex, sql.SQL(dir_st)))
    return sql.SQL(", ").join(parts)
