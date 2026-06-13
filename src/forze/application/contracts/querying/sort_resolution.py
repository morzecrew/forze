"""Resolve and normalize sort expressions for stable pagination."""

from typing import Any, Mapping, cast

from pydantic import BaseModel

from forze.application.contracts.codecs import stored_field_names_for
from forze.application.contracts.querying.expressions import QuerySortExpression
from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD

# ----------------------- #

_DIRECTIONS = ("asc", "desc")
_NULLS = ("first", "last")

# ....................... #


def default_nulls(direction: str) -> str:
    """Canonical null placement for *direction*: ``first`` for asc, ``last`` for desc.

    A null then sorts as the smallest value, matching the in-memory keyset oracle and the
    native default of Mongo/Firestore.
    """

    return "first" if direction == "asc" else "last"


# ....................... #


def parse_sort_value(
    value: Any,
    *,
    field: str | None = None,
    spec_name: str | None = None,
) -> tuple[str, str]:
    """Resolve a sort value (string shorthand or ``{"dir","nulls"}``) to ``(dir, nulls)``.

    Applies the canonical null default when *nulls* is omitted. Raises
    :func:`~forze.base.exceptions.exc.configuration` for an invalid direction or
    placement.
    """

    where = ""

    if field is not None:
        where = f" for field {field!r}"

    if spec_name is not None:
        where += f" on spec {spec_name!r}"

    nulls: str | None = None

    if isinstance(value, Mapping):
        value = cast(Mapping[str, Any], value)
        direction = str(value.get("dir", "")).lower()
        raw_nulls = value.get("nulls")
        nulls = str(raw_nulls).lower() if raw_nulls is not None else None

    else:
        direction = str(value).lower()

    if direction not in _DIRECTIONS:
        raise exc.configuration(f"Invalid sort direction {value!r}{where}.")

    if nulls is not None and nulls not in _NULLS:
        raise exc.configuration(f"Invalid null placement {nulls!r}{where}.")

    return direction, nulls if nulls is not None else default_nulls(direction)


# ....................... #


def assert_default_null_ordering(
    resolved: list[tuple[str, str, str]],
    *,
    backend: str,
) -> None:
    """Reject an explicit null placement a backend can't express (clean precondition).

    Backends that always order nulls as the smallest value (Mongo, Firestore) support the
    canonical default but not a per-key ``NULLS FIRST``/``LAST`` override; surface that as
    a ``query_feature_unsupported`` precondition rather than silently mis-ordering.
    """

    for field, direction, nulls in resolved:
        if nulls != default_nulls(direction):
            raise exc.precondition(
                f"The {backend!r} backend orders nulls as the smallest value and does "
                f"not support an explicit NULLS {nulls.upper()} override on field "
                f"{field!r}; omit the per-key 'nulls' placement.",
                code="query_feature_unsupported",
            )


# ....................... #


def _tiebreaker_direction(explicit: list[str]) -> str:
    """Direction for an auto-appended tie-breaker: the shared one, else ``asc``.

    A single-direction sort keeps its direction on the tie-breaker (so ``m desc`` stays
    ``m desc, id desc``); a mixed-direction sort has no single direction to inherit, so
    the tie-breaker is a deterministic ``asc``.
    """

    return explicit[0] if explicit and len(set(explicit)) == 1 else "asc"


# ....................... #


def resolve_sort_keys(
    sorts: QuerySortExpression | None,
    *,
    read_fields: frozenset[str] | None = None,
    spec_name: str = "<sort>",
) -> list[tuple[str, str, str]]:
    """Resolve a sort map to ``(field, direction, nulls)`` triples (no tie-breaker).

    For offset ORDER BY and in-memory sort, where a total order isn't required. Validates
    field membership when *read_fields* is given.
    """

    if not sorts:
        return []

    out: list[tuple[str, str, str]] = []

    for field, value in sorts.items():
        if read_fields is not None and field not in read_fields:
            raise exc.configuration(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
            )

        direction, nulls = parse_sort_value(value, field=field, spec_name=spec_name)
        out.append((field, direction, nulls))

    return out


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

    for field, value in sorts.items():
        if field not in read_fields:
            raise exc.configuration(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
            )

        parse_sort_value(value, field=field, spec_name=spec_name)


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

    validate_sort_fields(s, read_fields=read_fields, spec_name="<keyset>")

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
