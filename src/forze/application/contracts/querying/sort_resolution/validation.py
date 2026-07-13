"""Validate sort fields against a read model (flat or path-aware)."""

from pydantic import BaseModel

from forze.base.exceptions import exc

from ..expressions import QuerySortExpression
from .field_path import (
    _sort_field_resolves,  # pyright: ignore[reportPrivateUsage]
    field_path_resolves,
)
from .value import (
    _raise_invalid_sort,  # pyright: ignore[reportPrivateUsage]
    parse_sort_value,
)

# ----------------------- #


def validate_sort_fields(
    sorts: QuerySortExpression,
    *,
    read_fields: frozenset[str],
    spec_name: str,
    model: type[BaseModel] | None = None,
    client_facing: bool = True,
) -> None:
    """Raise when sorts are invalid.

    Pass *model* to permit nested/dotted sort paths, validated the same way filters are
    (via :func:`field_path_resolves`); without it, only flat *read_fields* membership is
    accepted (legacy behavior). An invalid sort raises a precondition (caller-supplied,
    HTTP 400) by default; pass ``client_facing=False`` to validate a spec's ``default_sort``,
    where it is the author's configuration error.
    """

    for field, value in sorts.items():
        if not _sort_field_resolves(field, read_fields=read_fields, model=model):
            _raise_invalid_sort(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
                client_facing=client_facing,
                code="field_not_on_read_model",
            )

        parse_sort_value(value, field=field, spec_name=spec_name, client_facing=client_facing)


# ....................... #


def validate_runtime_sort_fields(
    sorts: QuerySortExpression | None,
    *,
    model: type[BaseModel],
    backend: str,
    materialized: frozenset[str] = frozenset(),
    lenient: frozenset[str] = frozenset(),
) -> None:
    """Raise when a runtime sort references a field absent from the read *model*.

    Backends that hand sort fields straight to the driver (Mongo, Firestore)
    otherwise silently mis-sort (or drop rows) on an unknown field; this gives
    them the same fail-loud, path-aware validation Postgres gets from SQL.

    *materialized* names computed fields persisted for this spec, so they are
    sortable despite living in ``model_computed_fields``. *lenient* names fields
    declared on the model but **not** stored (see ``DocumentSpec.lenient_read_fields``);
    they have no column/key and are rejected before reaching the backend.
    """

    if not sorts:
        return

    for field in sorts:
        if field.split(".", 1)[0] in lenient:
            raise exc.precondition(
                f"Sort field {field!r} is a lenient (non-stored) field on the "
                f"{backend} read model ({model.__name__}); it cannot be sorted on.",
                code="field_not_on_read_model",
            )

        if not field_path_resolves(model, field, materialized=materialized):
            raise exc.precondition(
                f"Sort field {field!r} is not on the {backend} read model ({model.__name__}).",
                code="field_not_on_read_model",
            )
