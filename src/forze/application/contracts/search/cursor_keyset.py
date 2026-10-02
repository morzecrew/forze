"""Sort specs for search pages: the order after relevance, and the keyset cursor's keys."""

from collections.abc import Sequence

from pydantic import BaseModel

from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QuerySortExpression,
    default_nulls,
    parse_sort_value,
    resolve_sort_keys,
    validate_sort_fields,
)
from forze.base.exceptions import exc
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


def resolve_search_sorts(
    sorts: QuerySortExpression | None,
    *,
    default_sort: QuerySortExpression | None,
    read_fields: frozenset[str],
    tiebreaker: str | None = ID_FIELD,
    model: type[BaseModel] | None = None,
    spec_name: str = "<search>",
) -> QuerySortExpression:
    """The order a search page takes after relevance.

    Caller *sorts* win, else the spec's *default_sort*. The *tiebreaker* then closes the order
    when the read model has it, so rows that tie on every other key still have one place and
    an offset window neither repeats nor skips them. Appended, it takes the sort's direction
    when that is uniform, else ``asc``. Named in the sort, it ends it there: it is unique, so
    a key after it orders nothing, and a cursor seeks by the same keys an offset page sorts by.

    Empty when there is nothing to order by; relevance alone then decides, or the engine.
    ``None`` as *tiebreaker* leaves the order as given.

    Given the read *model*, every key the caller names is checked against *read_fields*
    first, a key after the tiebreaker included, so a field the model lacks is the caller's
    error wherever it sits.
    """

    if sorts and model is not None:
        validate_sort_fields(sorts, read_fields=read_fields, spec_name=spec_name, model=model)

    out = dict(sorts or default_sort or {})

    if tiebreaker is None:
        return out

    if tiebreaker in out:
        keys = list(out)

        return {key: out[key] for key in keys[: keys.index(tiebreaker) + 1]}

    if tiebreaker not in read_fields:
        return out

    # Through the canonical parser: it reads both the ``"asc"`` shorthand and the
    # ``{"dir", "nulls"}`` form, and refuses a bad value as a clean precondition.
    directions = {parse_sort_value(value, field=str(field))[0] for field, value in out.items()}
    out[tiebreaker] = "desc" if directions == {"desc"} else "asc"

    return out


# ....................... #


def refuse_cursor_null_placement(
    sorts: QuerySortExpression | None,
    *,
    default_sort: QuerySortExpression | None,
    read_fields: frozenset[str],
    backend: str,
    sealed: frozenset[str] = frozenset(),
) -> None:
    """Refuse a null placement a keyset cursor cannot keep, on the keys it keeps.

    The cursor seeks with a null as the smallest value. Only the order it walks is checked,
    ended at the id as :func:`resolve_search_sorts` ends it, so a key after the id is dropped
    rather than refused. A placement the request names is the caller's error; one the spec's
    ``default_sort`` carries, on a request that named none, is the spec author's.
    """

    kept = resolve_search_sorts(sorts or default_sort, default_sort=None, read_fields=read_fields)

    for field, direction, nulls in resolve_sort_keys(kept, sealed=sealed):
        if nulls == default_nulls(direction):
            continue

        cannot = (
            f"A {backend} seeks with a null as the smallest value and cannot keep "
            f"NULLS {nulls.upper()} on {field!r}"
        )

        if sorts:
            raise exc.precondition(
                f"{cannot}; omit the per-key 'nulls' placement.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        raise exc.configuration(f"{cannot}, which the search spec's default_sort asks for.")


# ....................... #


def ranked_search_cursor_key_spec(
    *,
    rank_field: str,
    sorts: QuerySortExpression | None,
    read_fields: frozenset[str],
    tiebreaker: str = ID_FIELD,
    model: type[BaseModel] | None = None,
) -> list[tuple[str, str]]:
    """``rank_field`` DESC, optional caller ``sorts``, then optional tie-breaker.

    Given the read *model*, the caller's keys are checked as :func:`resolve_search_sorts`
    checks them.
    """

    ordered = resolve_search_sorts(
        sorts, default_sort=None, read_fields=read_fields, tiebreaker=tiebreaker, model=model
    )

    return [
        (rank_field, "desc"),
        *(
            (str(field), parse_sort_value(value, field=str(field))[0])
            for field, value in ordered.items()
        ),
    ]
