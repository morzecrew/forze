"""Resolve and normalize sort expressions for stable pagination."""

from pydantic import BaseModel

from forze.application.contracts.codecs import stored_field_names_for
from forze.application.contracts.querying.expressions import QuerySortExpression
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD

# ----------------------- #

_DIRECTIONS = ("asc", "desc")

# ....................... #


def read_fields_for_model(model: type[BaseModel]) -> frozenset[str]:
    """Pydantic field names on *model* (excludes computed fields)."""

    return stored_field_names_for(model, include_computed=False)


# ....................... #


def validate_sort_fields(
    sorts: QuerySortExpression,
    *,
    read_fields: frozenset[str],
    spec_name: str,
) -> None:
    """Raise :class:`~forze.base.exceptions.exc.configuration` when sorts are invalid."""

    for field, direction in sorts.items():
        if field not in read_fields:
            raise exc.configuration(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
            )

        d = str(direction).lower()

        if d not in _DIRECTIONS:
            raise exc.configuration(
                f"Invalid sort direction {direction!r} for field {field!r} "
                f"on spec {spec_name!r}.",
            )


# ....................... #


def resolve_effective_sorts(
    *,
    sorts: QuerySortExpression | None,
    default_sort: QuerySortExpression | None,
    read_fields: frozenset[str],
    spec_name: str,
) -> QuerySortExpression:
    """Pick the sort map used for queries when the caller omits ``sorts``.

    Caller ``sorts`` win when non-empty. Otherwise ``default_sort``, then ``id``
    when the read model has an ``id`` field. Otherwise raise precondition.
    """

    if sorts:
        validate_sort_fields(sorts, read_fields=read_fields, spec_name=spec_name)
        return sorts

    if default_sort:
        validate_sort_fields(default_sort, read_fields=read_fields, spec_name=spec_name)
        return default_sort

    if ID_FIELD in read_fields:
        return {ID_FIELD: "asc"}

    raise exc.precondition(
        f"Spec {spec_name!r}: read model has no {ID_FIELD!r} field; pass ``sorts`` "
        f"or set ``default_sort`` on the document/search spec.",
    )


# ....................... #


def normalize_sorts_for_keyset(
    sorts: QuerySortExpression | None,
    *,
    read_fields: frozenset[str],
    tiebreaker: str = ID_FIELD,
) -> list[tuple[str, str]]:
    """Uniform-direction sorts with an optional final tie-breaker field."""

    s = dict(sorts) if sorts else {}

    if not s:
        raise exc.internal(
            "Keyset pagination requires non-empty sorts; resolve effective sorts first.",
        )

    validate_sort_fields(s, read_fields=read_fields, spec_name="<keyset>")

    dirs: set[str] = {str(s[k]).lower() for k in s}  # type: ignore[assignment, operator]

    if len(dirs) != 1:
        raise exc.internal(
            "Keyset (cursor) pagination requires all sort directions to match "
            "(all ``asc`` or all ``desc``).",
        )
    direction = next(iter(dirs))

    if direction not in _DIRECTIONS:
        raise exc.internal("Invalid sort direction in sorts expression")

    order_keys: list[str] = [k for k in s if k != tiebreaker]

    if tiebreaker in s:
        order_keys.append(tiebreaker)

    elif tiebreaker in read_fields:
        order_keys.append(tiebreaker)

    return [
        (k, s[k] if k in s else direction)  # type: ignore[dict-item, misc]
        for k in order_keys
    ]


# ....................... #


def normalize_sorts_with_id(
    sorts: QuerySortExpression | None,
) -> list[tuple[str, str]]:
    """Uniform-direction sorts with *id* as final tie-breaker (legacy callers)."""

    s = dict(sorts) if sorts else {}
    if not s:
        return [(ID_FIELD, "asc")]

    dirs: set[str] = {str(s[k]).lower() for k in s}  # type: ignore[assignment, operator]

    if len(dirs) != 1:
        raise exc.internal(
            "Keyset (cursor) pagination requires all sort directions to match "
            "(all ``asc`` or all ``desc``).",
        )
    direction = next(iter(dirs))

    if direction not in _DIRECTIONS:
        raise exc.internal("Invalid sort direction in sorts expression")

    order_keys: list[str] = [k for k in s if k != ID_FIELD]

    if ID_FIELD in s:
        order_keys.append(ID_FIELD)
    else:
        order_keys.append(ID_FIELD)

    return [
        (k, s[k] if k in s else direction)  # type: ignore[dict-item, misc]
        for k in order_keys
    ]
