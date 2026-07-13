"""Keyset cursor sort specs for ranked search (score + user sorts + id tie-break)."""

from collections.abc import Sequence

from forze.application.contracts.querying import QuerySortExpression, parse_sort_value
from forze.domain.constants import ID_FIELD

# ----------------------- #


def cursor_return_fields_for_select(
    *,
    sort_keys: Sequence[str],
    rank_field: str | None,
    return_fields: Sequence[str],
) -> tuple[str, ...]:
    """Build the column list for ``SELECT`` on cursor queries.

    Merges keyset columns (*sort_keys*) with caller *return_fields* (order preserved,
    duplicates dropped). When *rank_field* is set (synthetic score alias), it is omitted
    here because the engine adapter adds it separately in ``SELECT``.

    A nested/dotted sort key contributes its **root** column (``address.city`` →
    ``address``): the projection selects the whole JSON column and the cursor token reads
    the nested value out of it via ``row_value_for_sort_key``.
    """

    if rank_field is not None:
        sk_proj = [k.split(".", 1)[0] for k in sort_keys if k != rank_field]

    else:
        sk_proj = [k.split(".", 1)[0] for k in sort_keys]

    merged = tuple(dict.fromkeys([*sk_proj, *return_fields]))

    if rank_field is None:
        return merged
    return tuple(f for f in merged if f != rank_field)


# ....................... #


def ranked_search_cursor_key_spec(
    *,
    rank_field: str,
    sorts: QuerySortExpression | None,
    read_fields: frozenset[str],
    tiebreaker: str = ID_FIELD,
) -> list[tuple[str, str]]:
    """``rank_field`` DESC, optional caller ``sorts``, then optional tie-breaker."""

    spec: list[tuple[str, str]] = [(rank_field, "desc")]

    if sorts:
        for field, direction in sorts.items():
            # Route the caller-supplied direction through the canonical sort parser:
            # it accepts both the ``"asc"``/``"desc"`` shorthand and the ``{"dir","nulls"}``
            # form, and rejects a bad value with a clean precondition (``invalid_sort_value``)
            # rather than an opaque internal.
            d, _ = parse_sort_value(direction, field=str(field))
            spec.append((str(field), d))

    have = {k for k, _ in spec}

    if tiebreaker not in have and tiebreaker in read_fields:
        id_dir = "asc"

        if sorts:
            dirs = {str(v).lower() for v in sorts.values()}

            if len(dirs) == 1:
                id_dir = next(iter(dirs))

        spec.append((tiebreaker, id_dir))

    return spec
