"""Sort-value parsing and canonical null placement (the per-key atom layer)."""

from typing import Any, Mapping, NoReturn, cast

from forze.base.exceptions import exc

# ----------------------- #

_DIRECTIONS = ("asc", "desc")
_NULLS = ("first", "last")


# ....................... #


def _raise_invalid_sort(message: str, *, client_facing: bool, code: str) -> NoReturn:
    """Raise the right kind for a bad sort field/value.

    A malformed sort coming from a request is the *caller's* fault — a clean precondition
    (HTTP 400). The same defect in a spec's ``default_sort`` is the *author's* fault, a
    configuration error (HTTP 500). Callers thread ``client_facing`` so a runtime read path
    surfaces 400 while spec validation stays 500.
    """

    if client_facing:
        raise exc.precondition(message, code=code)

    raise exc.configuration(message)


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
    client_facing: bool = True,
) -> tuple[str, str]:
    """Resolve a sort value (string shorthand or ``{"dir","nulls"}``) to ``(dir, nulls)``.

    Applies the canonical null default when *nulls* is omitted. An invalid direction or
    placement raises a precondition (a caller-supplied value, HTTP 400) by default; pass
    ``client_facing=False`` for spec ``default_sort`` validation, where it is the author's
    configuration error.
    """

    where = f" for field {field!r}" if field is not None else ""

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
        _raise_invalid_sort(
            f"Invalid sort direction {value!r}{where}.",
            client_facing=client_facing,
            code="invalid_sort_value",
        )

    if nulls is not None and nulls not in _NULLS:
        _raise_invalid_sort(
            f"Invalid null placement {nulls!r}{where}.",
            client_facing=client_facing,
            code="invalid_sort_value",
        )

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
