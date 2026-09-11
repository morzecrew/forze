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
    """One read field the backend produces, optionally with the join that produces it.

    Two modes, and the first is the floor:

    - **Marked** (no *source*/*via*/*field*). The field is declared as produced by the
      relation and nothing more. Real backends read the view's column as they always
      have; every guard that follows from "not stored here" applies; and the mock takes
      the value from the stored row, which is where a seed or a test puts it. This mode
      is shape-independent — a nested reference object, a ``COALESCE`` over sibling
      rows, a ``CASE`` expression and a plain joined column are all the same
      declaration, because none of them is computed here.
    - **Resolved** (all three given). The mock performs the join itself, one hop by
      primary key. Available where the value is one field of one row reached by one key,
      which is the narrow case; everything else is marked.

      The join reads the source's **stored** row, not its read model, so it does not
      decrypt: a source field the source spec seals at rest arrives as ciphertext. Mark
      such a field instead of resolving it. The mock cannot refuse this at wiring time —
      the deps factory sees route configs, never the source spec — so it is a limit to
      know rather than one the framework enforces.

    The split exists because marking and resolving answer different questions, and fusing
    them meant a field could not be declared derived without also declaring a join it may
    not have.
    """

    source: str | StrEnum | None = None
    """Name of the spec holding the value — the ``name`` of another ``DocumentSpec``."""

    via: str | None = None
    """Field on *this* aggregate's read model carrying the source row's primary key."""

    field: str | None = None
    """Field on the source's read model whose value lands here."""

    optional: bool = False
    """Whether a missing source row yields ``None`` instead of a refusal.

    Resolved fields only. Set it when *via* is nullable: a parent with no supplier yet
    has no row to join, and that is data rather than corruption. Left off, a key that
    resolves to nothing is a refusal, because in a store that holds every row it is a
    seeding bug.
    """

    # ....................... #

    @property
    def resolved(self) -> bool:
        """Whether this declaration carries a join the mock can perform."""

        return self.source is not None

    # ....................... #

    def __attrs_post_init__(self) -> None:
        given = {
            "source": self.source is not None,
            "via": self.via is not None,
            "field": self.field is not None,
        }

        if any(given.values()) and not all(given.values()):
            missing = sorted(name for name, present in given.items() if not present)
            raise exc.configuration(
                f"DerivedReadField is partly resolved: {missing} missing. Give all of "
                "source, via and field to declare a join, or none of them to mark the "
                "field derived and leave its value to the relation.",
            )

        if self.optional and not any(given.values()):
            # `optional` describes what a *join* does when it finds no row. On a marked
            # field there is no join, so the flag would read as a promise nothing keeps.
            raise exc.configuration(
                "DerivedReadField sets optional without a join; optional describes what "
                "a resolved field does when the source row is missing.",
            )


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

        source, via, field = spec.source, spec.via, spec.field

        if source is None or via is None or field is None:
            # A marked field declares only that the relation produces it. There is no
            # source to reach, no key to join on and no nullability to reconcile, so
            # every check below is about a join this declaration does not make. The
            # three are all-or-nothing (`__attrs_post_init__`), so this narrows them
            # together rather than asserting the invariant three times.
            continue

        if not str(source).strip():
            raise exc.configuration(
                f"Derived read field {name!r} has a blank source (spec {spec_name!r}); "
                "name the spec the value is joined from.",
            )

        if not field.strip():
            raise exc.configuration(
                f"Derived read field {name!r} has a blank source field "
                f"(spec {spec_name!r}); name the field on {str(source)!r} to read.",
            )

        if via not in read_fields:
            raise exc.configuration(
                f"Derived read field {name!r} joins on {via!r}, which is not a "
                f"non-computed field on the read model {model_type.__name__} "
                f"(spec {spec_name!r}).",
            )

        if not spec.optional and _admits_none(fields[via].annotation):
            # A nullable key cannot produce a non-optional value: the first row with it
            # unset refuses at read time, which is a worse place to learn this than here.
            raise exc.configuration(
                f"Derived read field {name!r} joins on {via!r}, which is nullable, "
                f"but is not declared optional (spec {spec_name!r}); a key that may be "
                f"unset cannot yield a required value.",
            )

        if via in names:
            # A key that is itself derived cannot be read before the join it depends on,
            # and a declaration order that decides it would be a resolution order nobody
            # declared. One hop means the key is stored.
            raise exc.configuration(
                f"Derived read field {name!r} joins on {via!r}, which is itself "
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
