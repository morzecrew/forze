"""Shared validation for *derived read fields* (values produced by the backend).

A derived read field is declared on a read model and produced by the relation the
backend reads — a view's joined column — rather than by any write this application
performs. It has no write column, cannot be sealed at rest, and is not queryable.

This is the sibling of :mod:`~forze.application.contracts.conformity.lenient_read`
and deliberately not the same knob. Leniency says *may be absent from storage, and
reconstructs from the model default*, so it needs a default and refuses a required
field. Derivation says *comes from that relation*, so it needs no default and a
required field is the ordinary case — a joined display name is rarely optional.
"""

import types
from collections.abc import Mapping
from enum import StrEnum
from typing import Union, get_args, get_origin

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from ..querying.sort_resolution import read_fields_for_model
from .lenient_read import IDENTITY_READ_FIELDS

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DerivedReadField:
    """One read field the backend derives from another relation.

    The declaration is what a backend already does, written down: a Postgres view
    joins the source relation and projects *field* into this aggregate's read shape.
    Real backends therefore read it from storage as they always have. It is an
    instruction only to an adapter that has no view — see the mock's document
    adapter, which holds every row and so can perform the join itself.
    """

    source: str | StrEnum
    """Name of the spec holding the value — the ``name`` of another ``DocumentSpec``."""

    via: str
    """Field on *this* aggregate's read model carrying the source row's primary key."""

    field: str
    """Field on the source's read model whose value lands here."""

    optional: bool = False
    """Whether a missing source row yields ``None`` instead of a refusal.

    Set it when *via* is nullable: a parent with no supplier yet has no row to join,
    and that is data rather than corruption. Left off, a key that resolves to nothing
    is a refusal, because in a store that holds every row it is a seeding bug.
    """


# ....................... #


def validate_derived_read_fields(
    *,
    model_type: type[BaseModel],
    derived: Mapping[str, DerivedReadField],
    spec_name: object,
) -> None:
    """Validate that *derived* names real, non-operative read fields with usable sources.

    Each key must be a non-computed field on *model_type* that is not an identity/audit
    field, and each declaration's ``via`` must be a non-computed field on the same model.
    A derived field **may be required** — that is the whole distinction from leniency.

    Overlaps with the caller's other conformity sets (``lenient_read_fields``,
    ``materialized``, ``write_omit_fields``) are checked by the caller, which knows them
    and can name each collision precisely.

    :raises exc.configuration: when a name is unknown, is an identity/audit field, names
        itself as its own key, or carries a blank ``source``/``field``.
    """

    if not derived:
        return

    names = frozenset(derived)

    if identity := names & IDENTITY_READ_FIELDS:
        raise exc.configuration(
            f"Field(s) {sorted(identity)} are identity/audit fields and cannot be "
            f"derived; they are written by this aggregate (spec {spec_name!r}).",
        )

    read_fields = read_fields_for_model(model_type)
    fields = model_type.model_fields

    if missing := names - read_fields:
        raise exc.configuration(
            f"Derived read field(s) {sorted(missing)} are not non-computed fields "
            f"on the read model {model_type.__name__} (spec {spec_name!r}).",
        )

    for name in sorted(derived):
        spec = derived[name]

        if not str(spec.source).strip():
            raise exc.configuration(
                f"Derived read field {name!r} has a blank source (spec {spec_name!r}); "
                "name the spec the value is joined from.",
            )

        if not spec.field.strip():
            raise exc.configuration(
                f"Derived read field {name!r} has a blank source field "
                f"(spec {spec_name!r}); name the field on {str(spec.source)!r} to read.",
            )

        if spec.via not in read_fields:
            raise exc.configuration(
                f"Derived read field {name!r} joins on {spec.via!r}, which is not a "
                f"non-computed field on the read model {model_type.__name__} "
                f"(spec {spec_name!r}).",
            )

        if not spec.optional and _admits_none(fields[spec.via].annotation):
            # A nullable key cannot produce a non-optional value: the first row with it
            # unset refuses at read time, which is a worse place to learn this than here.
            raise exc.configuration(
                f"Derived read field {name!r} joins on {spec.via!r}, which is nullable, "
                f"but is not declared optional (spec {spec_name!r}); a key that may be "
                f"unset cannot yield a required value.",
            )

        if spec.via in names:
            # A key that is itself derived cannot be read before the join it depends on,
            # and a declaration order that decides it would be a resolution order nobody
            # declared. One hop means the key is stored.
            raise exc.configuration(
                f"Derived read field {name!r} joins on {spec.via!r}, which is itself "
                f"derived (spec {spec_name!r}); the join key must be a stored field.",
            )


# ....................... #


def _admits_none(annotation: object) -> bool:
    """Whether *annotation* accepts ``None``.

    Deliberately not ``FieldInfo.is_required()``: a field with a default is not the
    same as a field that may hold ``None``. ``note: str = ""`` is a perfectly good join
    key, and treating it as nullable refused a legitimate shape — caught by the
    itself-derived guardrail's own test, which uses exactly that field.
    """

    if annotation is type(None):
        return True

    if get_origin(annotation) in (Union, types.UnionType):
        return type(None) in get_args(annotation)

    return False
