"""PostgreSQL keyset seek fragments (psycopg :mod:`sql`)."""

from typing import Any, Literal

from psycopg import sql

from forze.base.exceptions import exc

# ----------------------- #

Nav = Literal["after", "before"]

# ....................... #


def _eq_term(col: sql.Composable, value: Any) -> tuple[sql.Composable, list[Any]]:
    """Null-safe prefix equality for a fixed boundary *value* (known at build time)."""

    if value is None:
        return sql.SQL("{} IS NULL").format(col), []

    return sql.SQL("{} = {}").format(col, sql.Placeholder()), [value]


def _strict_seek_term(
    col: sql.Composable,
    direction: str,
    nulls: str,
    value: Any,
    *,
    after: bool,
) -> tuple[sql.Composable, list[Any]]:
    """Strict per-key seek term honoring an explicit null placement.

    Null placement is absolute (``NULLS FIRST``/``LAST``, independent of direction); only
    the non-null comparison flips with direction. Because the boundary *value* is known at
    build time we branch on it directly, and we account for a ``NULL`` *column* so that
    null-keyed rows page correctly (a plain ``col > ?`` is ``NULL`` for a null column and
    would silently drop those rows).
    """

    asc = direction == "asc"

    if value is None:
        # Boundary is a null. A non-null column is strictly past it only on the side the
        # nulls are NOT on: after → past nulls-first nulls; before → past nulls-last nulls.
        non_null_wins = (nulls == "first") if after else (nulls == "last")

        if non_null_wins:
            return sql.SQL("{} IS NOT NULL").format(col), []

        return sql.SQL("FALSE"), []

    if after:
        op = sql.SQL(">") if asc else sql.SQL("<")
        include_null = nulls == "last"  # a null column comes after a non-null value

    else:
        op = sql.SQL("<") if asc else sql.SQL(">")
        include_null = nulls == "first"  # a null column comes before a non-null value

    cmp_ = sql.SQL("{} {} {}").format(col, op, sql.Placeholder())

    if include_null:
        return sql.SQL("({} OR {} IS NULL)").format(cmp_, col), [value]

    return cmp_, [value]


def _default_nulls(directions: list[str], nulls: list[str] | None) -> list[str]:
    """Explicit per-key null placement, or the canonical default per direction."""

    if nulls is None:
        return ["first" if d == "asc" else "last" for d in directions]

    return nulls


def build_seek_condition(
    exprs: list[sql.Composable],
    directions: list[str],
    values: list[Any],
    nav: Nav,
    *,
    nulls: list[str] | None = None,
) -> tuple[sql.Composable, list[Any]]:
    """``after``: rows strictly after the cursor; ``before``: rows strictly before.

    A composite (lexicographic) keyset seek: an OR of branches, each requiring equality
    on the prefix keys and a strict comparison on the next. Per-key direction is honored
    (mixed ``asc``/``desc`` is fine) and each key's null placement (explicit or the
    canonical default) is applied to both the boundary value and the row column.
    """

    null_order = _default_nulls(directions, nulls)
    n = len(exprs)

    if n != len(values) or n != len(directions) or n != len(null_order) or n < 1:
        raise exc.precondition("Invalid keyset shape")

    after = nav == "after"
    parts: list[sql.Composable] = []
    out_params: list[Any] = []

    for i in range(n):
        and_terms: list[sql.Composable] = []

        for j in range(i):
            eq_sql, eq_params = _eq_term(exprs[j], values[j])
            and_terms.append(eq_sql)
            out_params.extend(eq_params)

        strict_sql, strict_params = _strict_seek_term(
            exprs[i],
            directions[i],
            null_order[i],
            values[i],
            after=after,
        )
        and_terms.append(strict_sql)
        out_params.extend(strict_params)

        branch = and_terms[0]

        for term in and_terms[1:]:
            branch = sql.SQL("({} AND {})").format(branch, term)

        parts.append(branch)

    ored = parts[0]

    for p2 in parts[1:]:
        ored = sql.SQL("({} OR {})").format(ored, p2)

    return ored, out_params


def build_order_by_sql(
    exprs: list[sql.Composable],
    directions: list[str],
    *,
    nulls: list[str] | None = None,
    flip: bool = False,
) -> sql.Composable:
    """Build ``ORDER BY`` from per-key expressions; *flip* reverses traversal.

    Emits explicit ``NULLS FIRST``/``LAST`` from each key's placement (explicit or the
    canonical default) so Postgres conforms to the order the keyset seek and the in-memory
    oracle use (its own default — nulls last on asc — would otherwise disagree). *flip*
    reverses the traversal for a ``before`` page, inverting both direction **and** null
    placement.
    """

    parts: list[sql.Composable] = []

    for ex, d, np in zip(exprs, directions, _default_nulls(directions, nulls), strict=True):
        if flip:
            d_out = "desc" if d == "asc" else "asc"
            n_out = "last" if np == "first" else "first"

        else:
            d_out, n_out = d, np

        dir_st = "ASC" if d_out == "asc" else "DESC"
        null_st = "NULLS FIRST" if n_out == "first" else "NULLS LAST"
        parts.append(sql.SQL("{} {} {}").format(ex, sql.SQL(dir_st), sql.SQL(null_st)))

    return sql.SQL(", ").join(parts)


def build_ranked_cursor_order_by_sql(
    exprs: list[sql.Composable],
    sort_keys: list[str],
    directions: list[str],
    *,
    rank_key: str,
    flip: bool = False,
) -> sql.Composable:
    """Like :func:`build_order_by_sql` but applies ``NULLS LAST`` / ``NULLS FIRST`` on *rank_key*."""
    parts: list[sql.Composable] = []

    for ex, d_raw, sk in zip(exprs, directions, sort_keys, strict=True):
        d = ("desc" if d_raw == "asc" else "asc") if flip else d_raw

        if sk == rank_key:
            if d == "desc":
                parts.append(sql.SQL("{} DESC NULLS LAST").format(ex))

            else:
                parts.append(sql.SQL("{} ASC NULLS FIRST").format(ex))

        else:
            suf = "ASC" if d == "asc" else "DESC"
            parts.append(sql.SQL("{} {}").format(ex, sql.SQL(suf)))

    return sql.SQL(", ").join(parts)
