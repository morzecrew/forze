"""Resolve a sort map into ordered ``(field, direction, nulls)`` keys for pagination."""

from typing import Any, Mapping

from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD

from ..expressions import QuerySortExpression
from .field_path import _sort_field_resolves
from .validation import validate_sort_fields
from .value import (
    _raise_invalid_sort,
    _tiebreaker_direction,
    default_nulls,
    parse_sort_value,
)

# ----------------------- #


def resolve_sort_keys(
    sorts: QuerySortExpression | None,
    *,
    read_fields: frozenset[str] | None = None,
    spec_name: str = "<sort>",
    model: type[BaseModel] | None = None,
    client_facing: bool = True,
) -> list[tuple[str, str, str]]:
    """Resolve a sort map to ``(field, direction, nulls)`` triples (no tie-breaker).

    For offset ORDER BY and in-memory sort, where a total order isn't required. Validates
    field membership when *read_fields* is given; pass *model* to allow nested/dotted sort
    paths (validated the same way filters are — see :func:`field_path_resolves`). An unknown
    field raises a precondition (caller-supplied sort, HTTP 400) by default.
    """

    if not sorts:
        return []

    out: list[tuple[str, str, str]] = []

    for field, value in sorts.items():
        if read_fields is not None and not _sort_field_resolves(
            field, read_fields=read_fields, model=model
        ):
            _raise_invalid_sort(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
                client_facing=client_facing,
                code="field_not_on_read_model",
            )

        direction, nulls = parse_sort_value(
            value, field=field, spec_name=spec_name, client_facing=client_facing
        )
        out.append((field, direction, nulls))

    return out


# ....................... #


def resolve_effective_sorts(
    *,
    sorts: QuerySortExpression | None,
    default_sort: QuerySortExpression | None,
    read_fields: frozenset[str],
    spec_name: str,
    model: type[BaseModel] | None = None,
) -> QuerySortExpression:
    """Pick the sort map used for queries when the caller omits ``sorts``.

    Caller ``sorts`` win when non-empty. Otherwise ``default_sort``, then ``id``
    when the read model has an ``id`` field. Otherwise raise precondition. Pass *model*
    to permit nested/dotted sort paths.
    """

    if sorts:
        validate_sort_fields(
            sorts,
            read_fields=read_fields,
            spec_name=spec_name,
            model=model,
            client_facing=True,
        )
        return sorts

    if default_sort:
        # An invalid ``default_sort`` is the spec author's configuration error, not the
        # caller's — keep it a 500, unlike the caller-supplied ``sorts`` above.
        validate_sort_fields(
            default_sort,
            read_fields=read_fields,
            spec_name=spec_name,
            model=model,
            client_facing=False,
        )
        return default_sort

    if ID_FIELD in read_fields:
        return {ID_FIELD: "asc"}

    raise exc.precondition(
        f"Spec {spec_name!r}: read model has no {ID_FIELD!r} field; pass ``sorts`` "
        f"or set ``default_sort`` on the document/search spec.",
    )


# ....................... #


def _with_tiebreaker(
    s: Mapping[str, Any],
    *,
    tiebreaker: str,
    append_tiebreaker: bool,
    spec_name: str,
) -> list[tuple[str, str, str]]:
    """Build ``(field, direction, nulls)`` triples, appending a tie-breaker key.

    The tie-breaker inherits the shared direction when the sort is uniform, else ``asc``,
    with the canonical null placement for that direction.
    """

    parsed = {
        k: parse_sort_value(v, field=k, spec_name=spec_name) for k, v in s.items()
    }
    order_keys = [k for k in s if k != tiebreaker]

    if tiebreaker in s or append_tiebreaker:
        order_keys.append(tiebreaker)

    tb_dir = _tiebreaker_direction([d for d, _ in parsed.values()])

    out: list[tuple[str, str, str]] = []

    for k in order_keys:
        if k in parsed:
            direction, nulls = parsed[k]
        else:
            direction, nulls = tb_dir, default_nulls(tb_dir)

        out.append((k, direction, nulls))

    return out


# ....................... #


def normalize_sorts_for_keyset(
    sorts: QuerySortExpression | None,
    *,
    read_fields: frozenset[str],
    tiebreaker: str = ID_FIELD,
    model: type[BaseModel] | None = None,
) -> list[tuple[str, str, str]]:
    """Resolve sorts into ``(field, direction, nulls)`` keys with a final tie-breaker.

    Directions may be **mixed** (some ``asc``, some ``desc``): the composite keyset seek
    compares each key in its own direction, so a mixed order is stable and paginable. The
    auto-appended tie-breaker (``id``) inherits the sort's direction when uniform, else
    ``asc``. Each key carries its null placement (the canonical default, or an explicit
    ``nulls`` override): ``asc`` → nulls first, ``desc`` → nulls last unless overridden,
    so a null sorts as the smallest value, matching the in-memory oracle. Backends emit
    explicit ``NULLS FIRST``/``LAST`` from this to conform.
    """

    s = dict(sorts) if sorts else {}

    if not s:
        raise exc.internal(
            "Keyset pagination requires non-empty sorts; resolve effective sorts first.",
        )

    validate_sort_fields(s, read_fields=read_fields, spec_name="<keyset>", model=model)

    return _with_tiebreaker(
        s,
        tiebreaker=tiebreaker,
        append_tiebreaker=tiebreaker in read_fields,
        spec_name="<keyset>",
    )


# ....................... #


def normalize_sorts_with_id(
    sorts: QuerySortExpression | None,
) -> list[tuple[str, str, str]]:
    """Sorts with *id* as the final tie-breaker (legacy callers); directions may mix."""

    s = dict(sorts) if sorts else {}

    if not s:
        return [(ID_FIELD, "asc", default_nulls("asc"))]

    return _with_tiebreaker(
        s,
        tiebreaker=ID_FIELD,
        append_tiebreaker=True,
        spec_name="<keyset>",
    )
