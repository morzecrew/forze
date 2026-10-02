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
    never_null: bool = False,
) -> tuple[sql.Composable, list[Any]]:
    """Strict per-key seek term honoring an explicit null placement.

    A *never_null* key (the record id, a ``NOT NULL`` column) takes the plain comparison: it
    has no null rows to account for, and the bare range is what an index serves.

    Null placement is absolute (``NULLS FIRST``/``LAST``, independent of direction); only
    the non-null comparison flips with direction. Because the boundary *value* is known at
    build time we branch on it directly, and we account for a ``NULL`` *column* so that
    null-keyed rows page correctly (a plain ``col > ?`` is ``NULL`` for a null column and
    would silently drop those rows).
    """

    asc = direction == "asc"

    if never_null and value is not None:
        op = sql.SQL(">") if asc == after else sql.SQL("<")

        return sql.SQL("{} {} {}").format(col, op, sql.Placeholder()), [value]

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
    not_null: list[bool] | None = None,
) -> tuple[sql.Composable, list[Any]]:
    """``after``: rows strictly after the cursor; ``before``: rows strictly before.

    A composite (lexicographic) keyset seek: an OR of branches, each requiring equality
    on the prefix keys and a strict comparison on the next. Per-key direction is honored
    (mixed ``asc``/``desc`` is fine) and each key's null placement (explicit or the
    canonical default) is applied to both the boundary value and the row column.
    """

    null_order = _default_nulls(directions, nulls)
    n = len(exprs)
    never_null = not_null if not_null is not None else [False] * n

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
            never_null=never_null[i],
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
    not_null: list[bool] | None = None,
    flip: bool = False,
) -> sql.Composable:
    """Build ``ORDER BY`` from per-key expressions; *flip* reverses traversal.

    Emits explicit ``NULLS FIRST``/``LAST`` from each key's placement (explicit or the
    canonical default) so Postgres conforms to the order the keyset seek and the in-memory
    oracle use (its own default — nulls last on asc — would otherwise disagree). *flip*
    reverses the traversal for a ``before`` page, inverting both direction **and** null
    placement. A key *not_null* marks as never ``NULL`` gets no placement: it orders the same
    without one, and only without one can a plain btree index serve it.
    """

    parts: list[sql.Composable] = []
    never_null = not_null if not_null is not None else [False] * len(exprs)

    for ex, d, np, nn in zip(
        exprs, directions, _default_nulls(directions, nulls), never_null, strict=True
    ):
        if flip:
            d_out = "desc" if d == "asc" else "asc"
            n_out = "last" if np == "first" else "first"

        else:
            d_out, n_out = d, np

        dir_st = "ASC" if d_out == "asc" else "DESC"
        part = sql.SQL("{} {}").format(ex, sql.SQL(dir_st))

        if not nn:
            null_st = "NULLS FIRST" if n_out == "first" else "NULLS LAST"
            part = sql.SQL("{} {}").format(part, sql.SQL(null_st))

        parts.append(part)

    return sql.SQL(", ").join(parts)


def build_ranked_cursor_order_by_sql(
    exprs: list[sql.Composable],
    sort_keys: list[str],
    directions: list[str],
    *,
    rank_key: str,
    not_null: list[bool] | None = None,
    flip: bool = False,
) -> sql.Composable:
    """:func:`build_order_by_sql` for a ranked cursor, whose keys carry no explicit placement.

    Every key, the rank included, takes the canonical placement for its direction, which is
    what the seek assumes: a null sorts as the smallest value. Postgres's own default puts
    nulls last ascending, so a walk ordered that way would seek past them. A key *not_null*
    marks as never null, the rank aside, takes none, as on every other order.
    """

    never_null = [
        nn and key != rank_key
        for nn, key in zip(not_null or [False] * len(exprs), sort_keys, strict=True)
    ]

    return build_order_by_sql(exprs, directions, not_null=never_null, flip=flip)
