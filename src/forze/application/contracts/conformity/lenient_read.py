"""Shared validation for *lenient read fields* (storage-conformity leniency).

A lenient read field is declared on a read model but has no backing column on the
relation: it is dropped from the read projection and hydrated from its model default
on read. This module holds the rules common to every spec that exposes the knob
(:class:`~forze.application.contracts.document.DocumentSpec`,
:class:`~forze.application.contracts.search.SearchSpec`), so they cannot drift.
"""

from typing import Final, Literal

from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.domain.constants import ID_FIELD, LAST_UPDATE_AT_FIELD, REV_FIELD

from ..._logger import logger
from ..querying.sort_resolution import read_fields_for_model

# ----------------------- #

ReadConformity = Literal["strict", "lenient"]
"""Storage-conformity level for a read model.

``strict`` (default): every read field must map to a stored column. ``lenient``:
auto-derive :func:`derive_lenient_read_fields` (every defaulted, non-identity,
non-operative read field may be absent from storage and hydrates from its default)."""

IDENTITY_READ_FIELDS: Final = frozenset({ID_FIELD, REV_FIELD, "created_at", LAST_UPDATE_AT_FIELD})
"""Identity/audit fields that must always be read from storage (never lenient)."""

# ....................... #


def validate_lenient_read_fields(
    *,
    model_type: type[BaseModel],
    lenient: frozenset[str],
    spec_name: object,
) -> None:
    """Validate that *lenient* read fields are absent-tolerant and non-operative.

    Each name must be a non-computed field on *model_type* that carries a default
    (is non-required) and is not an identity/audit field. A field whose default is
    a ``default_factory`` is allowed but warned: every read of a row missing the
    column produces a fresh value, not stored data.

    Caller-specific overlaps (a document's ``materialized`` set, a search index's
    ``fields``) are checked by the caller — they are operative and carry tailored
    messages.

    :raises exc.configuration: when a lenient field is unknown, required, or an
        identity/audit field.
    """

    if not lenient:
        return

    if identity := lenient & IDENTITY_READ_FIELDS:
        raise exc.configuration(
            f"Field(s) {sorted(identity)} are identity/audit fields and cannot be "
            f"lenient; they must always be read from storage (spec {spec_name!r}).",
        )

    read_fields = read_fields_for_model(model_type)

    if missing := lenient - read_fields:
        raise exc.configuration(
            f"Lenient read field(s) {sorted(missing)} are not non-computed fields "
            f"on the read model {model_type.__name__} (spec {spec_name!r}).",
        )

    fields = model_type.model_fields

    for name in sorted(lenient):
        field = fields[name]

        if field.is_required():
            raise exc.configuration(
                f"Lenient read field {name!r} has no default (spec {spec_name!r}); a "
                "field absent from storage must be constructible from a default.",
            )

        if field.default_factory is not None:
            logger.warning(
                "Spec %r: lenient read field %r uses a default_factory; every read of a "
                "row missing this column produces a fresh value, not stored data.",
                str(spec_name),
                name,
            )


# ....................... #


def derive_lenient_read_fields(
    model_type: type[BaseModel],
    *,
    exclude: frozenset[str] = frozenset(),
) -> frozenset[str]:
    """Auto-derive the lenient read set for ``read_conformity="lenient"``.

    Every non-computed read field that carries a **static** default and is neither an
    identity/audit field nor in *exclude* (a document's ``materialized`` set, or a
    search index's ``fields``). Fields with a ``default_factory`` are **not** derived —
    they would yield a fresh value per row; declare those explicitly to accept that.
    """

    fields = model_type.model_fields
    derived = {
        name
        for name in read_fields_for_model(model_type)
        if name not in IDENTITY_READ_FIELDS
        and name not in exclude
        and not fields[name].is_required()
        and fields[name].default_factory is None
    }

    return frozenset(derived)
