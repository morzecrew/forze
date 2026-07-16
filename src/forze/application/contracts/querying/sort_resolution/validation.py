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
    sealed: frozenset[str] = frozenset(),
) -> None:
    """Raise when sorts are invalid.

    Pass *model* to permit nested/dotted sort paths, validated the same way filters are
    (via :func:`field_path_resolves`); without it, only flat *read_fields* membership is
    accepted (legacy behavior). An invalid sort raises a precondition (caller-supplied,
    HTTP 400) by default; pass ``client_facing=False`` to validate a spec's ``default_sort``,
    where it is the author's configuration error.

    *sealed* names fields whose stored value is ciphertext (``FieldEncryption.encrypted |
    .searchable`` — use :meth:`FieldEncryption.forbidden_sort_fields`). They have no usable
    order at rest, so sorting on one is a silent no-op ordering, and a keyset cursor would
    carry the last row's raw sort value in its token. The search plane already refuses this
    (``core.search.encrypted_sort_field``); this is the same rule for every other plane, at
    the seam every backend's sort passes through.
    """

    for field, value in sorts.items():
        if not _sort_field_resolves(field, read_fields=read_fields, model=model):
            _raise_invalid_sort(
                f"Sort field {field!r} is not on read model for spec {spec_name!r}.",
                client_facing=client_facing,
                code="field_not_on_read_model",
            )

        # Root-aware: ``contract.ssn`` is sealed when ``contract`` is (the value lives inside
        # the sealed ciphertext), matching FieldEncryption.sealed_fields_in.
        if field.split(".", 1)[0] in sealed:
            _raise_invalid_sort(
                f"Sorting on field-encrypted field {field!r} is not allowed for "
                f"{spec_name!r}: encrypted (randomized) and searchable (deterministic) "
                "fields have no order at rest and cannot be used as sort keys.",
                client_facing=client_facing,
                code="core.crypto.encrypted_sort_field",
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
    sealed: frozenset[str] = frozenset(),
) -> None:
    """Raise when a runtime sort references a field absent from the read *model*.

    Backends that hand sort fields straight to the driver (Mongo, Firestore)
    otherwise silently mis-sort (or drop rows) on an unknown field; this gives
    them the same fail-loud, path-aware validation Postgres gets from SQL.

    *materialized* names computed fields persisted for this spec, so they are
    sortable despite living in ``model_computed_fields``. *lenient* names fields
    declared on the model but **not** stored (see ``DocumentSpec.lenient_read_fields``);
    they have no column/key and are rejected before reaching the backend.

    *sealed* names fields stored as ciphertext (see :func:`validate_sort_fields`) — no order
    at rest, so they are refused as sort keys. Passing it is what makes a backend that stores
    plaintext (the mock) answer as one that stores ciphertext, instead of returning a
    correctly-ordered page for a query that mis-orders in production.
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

        if field.split(".", 1)[0] in sealed:
            raise exc.precondition(
                f"Sorting on field-encrypted field {field!r} is not allowed on the "
                f"{backend} read model ({model.__name__}): encrypted (randomized) and "
                "searchable (deterministic) fields have no order at rest and cannot be "
                "used as sort keys.",
                code="core.crypto.encrypted_sort_field",
            )

        if not field_path_resolves(model, field, materialized=materialized):
            raise exc.precondition(
                f"Sort field {field!r} is not on the {backend} read model ({model.__name__}).",
                code="field_not_on_read_model",
            )
