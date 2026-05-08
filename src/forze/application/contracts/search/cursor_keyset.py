"""Keyset cursor sort specs for ranked search (score + user sorts + id tie-break)."""

from typing import Sequence

from forze.application.contracts.query import QuerySortExpression
from forze.base.errors import CoreError
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
    """

    if rank_field is not None:
        sk_proj = [k for k in sort_keys if k != rank_field]

    else:
        sk_proj = list(sort_keys)

    merged = tuple(dict.fromkeys([*sk_proj, *return_fields]))

    if rank_field is None:
        return merged
    return tuple(f for f in merged if f != rank_field)


# ....................... #


def ranked_search_cursor_key_spec(
    *,
    rank_field: str,
    sorts: QuerySortExpression | None,
) -> list[tuple[str, str]]:
    """``rank_field`` DESC, optional caller ``sorts``, then ``id`` tie-breaker."""

    spec: list[tuple[str, str]] = [(rank_field, "desc")]

    if sorts:
        for field, direction in sorts.items():
            d = str(direction).lower()

            if d not in ("asc", "desc"):
                raise CoreError(
                    f"Invalid sort direction in search cursor: {direction!r}"
                )

            spec.append((str(field), d))

    have = {k for k, _ in spec}

    if ID_FIELD not in have:
        id_dir = "asc"

        if sorts:
            dirs = {str(v).lower() for v in sorts.values()}

            if len(dirs) == 1:
                id_dir = next(iter(dirs))

        spec.append((ID_FIELD, id_dir))

    return spec
