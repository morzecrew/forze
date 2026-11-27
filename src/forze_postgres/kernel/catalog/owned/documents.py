"""The table of a framework-owned document, derived from its spec (RFC 0080 §5.7).

A store table has a hand-written shape beside its adapter. A document table cannot:
``forze_postgres`` may import neither ``forze_identity`` nor ``forze_kits``, which ship the
specs. So the table is derived from the
:class:`~forze_postgres.kernel.catalog.validation.validate_schema.PostgresDocumentSchemaSpec`
the startup validator reads, built from the spec and the route's config, and a render cannot
ask for a column the validator would not accept.

The derivation:

* the base columns of every document, ``id``, ``rev``, ``created_at`` and
  ``last_update_at``, keyed on ``id``, then ``tenant_id`` on a tenant-aware route;
* each field the validator requires on the write relation, in model order, typed by
  :func:`column_type`, nullable when it is ``T | None``, and with a ``DEFAULT`` for a literal
  default or an empty collection factory;
* a required unique index per ``UniqueTogether`` guarantee, per unique
  :class:`~forze.application.contracts.document.DocumentIndex`, and for a configured conflict
  target; a required plain index per other ``DocumentIndex``. A tenant-aware route leads each
  with ``tenant_id``.

What it cannot print is refused, naming the spec: a guarantee Postgres keeps with something
other than a plain unique index, a history table, and an annotation outside the type table.
"""

import json
from collections.abc import Iterator, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Annotated, Any, Final, Literal, TypeAliasType, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel, EmailStr
from pydantic.fields import ComputedFieldInfo, FieldInfo
from pydantic_core import PydanticUndefined

from forze.application.contracts.guarantees import UniqueTogether
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import exc
from forze.base.primitives import int_width
from forze.base.serialization import stored_field_names_for

from ..validation.validate_schema import PostgresDocumentSchemaSpec
from .quoting import quote_ident
from .shapes import OwnedColumn, OwnedIndex, OwnedRelationShape, is_constant_default

# ----------------------- #

DOCUMENT_TABLES_SINCE: Final = "0.25.0"
"""The release that first declares the document tables. Their columns carry no ``since`` of
their own: each is the spec's, as the spec is in the installed release."""

_BASE_COLUMNS: Final = (
    OwnedColumn(name="id", type="uuid", nullable=False),
    OwnedColumn(name="rev", type="integer", nullable=False),
    OwnedColumn(name="created_at", type="timestamptz", nullable=False),
    OwnedColumn(name="last_update_at", type="timestamptz", nullable=False),
)
"""What every document stores, in the order every published page prints it."""

_BASE_NAMES: Final = frozenset(column.name for column in _BASE_COLUMNS)

_SCALARS: Final[tuple[tuple[type[Any], str], ...]] = (
    # bool before int, datetime before date: each is a subclass of the other.
    (UUID, "uuid"),
    (str, "text"),
    (bool, "boolean"),
    (int, "integer"),
    (float, "double precision"),
    (Decimal, "numeric"),
    (datetime, "timestamptz"),
    (date, "date"),
    (time, "time"),
    (bytes, "bytea"),
)
"""The column type of each scalar annotation, a subclass (an enum) taking its base's."""

_REFUSED: Final = "postgres_document_render_refused"

_DATABASE_BOOKKEEPING: Final = (
    'bookkeeping_strategy="database": the trigger that keeps rev and last_update_at is yours; '
    "startup refuses the table without it"
)
"""The note above a table whose route leaves bookkeeping to a trigger (RFC 0080 row 35)."""


def _refused(spec: PostgresDocumentSchemaSpec, reason: str) -> exc:
    return exc.configuration(
        f"Cannot print the table of document {spec.name!r}: {reason}",
        code=_REFUSED,
        details={"document": spec.name},
    )


# ....................... #


def _strip(annotation: Any) -> tuple[Any, bool, tuple[Any, ...]]:
    """*annotation* without ``None``, ``Annotated`` and type aliases: the core type, whether
    ``None`` was in it, and the ``Annotated`` metadata met on the way."""

    nullable = False
    metadata: list[Any] = []

    while True:
        origin = get_origin(annotation)

        if origin is Annotated:
            core, *extra = get_args(annotation)
            metadata.extend(extra)
            annotation = core
            continue

        if isinstance(annotation, TypeAliasType):
            annotation = annotation.__value__
            continue

        if origin is Union or origin is UnionType:
            args = [arg for arg in get_args(annotation) if arg is not type(None)]
            nullable = nullable or len(args) < len(get_args(annotation))

            if len(args) == 1:
                annotation = args[0]
                continue

        return annotation, nullable, tuple(metadata)


def _scalar(annotation: Any, metadata: Sequence[Any]) -> str | None:
    if annotation is EmailStr:
        return "text"

    if get_origin(annotation) is Literal:
        values = get_args(annotation)

        return "text" if values and all(isinstance(value, str) for value in values) else None

    if not isinstance(annotation, type):
        return None

    for base, column in _SCALARS:
        if issubclass(annotation, base):
            if column == "integer" and int_width(annotation, metadata) == 64:
                return "bigint"

            return column

    return None


def _is_json(annotation: Any) -> bool:
    origin = get_origin(annotation)

    if annotation is Any or annotation is dict or origin is dict:
        return True

    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _element(annotation: Any) -> Any | None:
    """The element of a ``list[X]`` or ``tuple[X, ...]``, or ``None`` for anything else."""

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is list and len(args) == 1:
        return args[0]

    if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
        return args[0]

    return None


def column_type(annotation: Any, metadata: Sequence[Any] = ()) -> tuple[str, bool] | None:
    """The column type of a field annotated *annotation*, and whether it is nullable, or
    ``None`` when the type table (RFC 0080 §5.7) has no row for it.

    *metadata* is the field's pydantic metadata, where an outer ``Annotated``'s items land,
    such as the :data:`~forze.base.primitives.Int64` width.
    """

    core, nullable, extra = _strip(annotation)
    widths = (*metadata, *extra)

    if (scalar := _scalar(core, widths)) is not None:
        return scalar, nullable

    if (element := _element(core)) is not None:
        inner, _, inner_extra = _strip(element)
        scalar = _scalar(inner, inner_extra)

        return (f"{scalar}[]" if scalar is not None else "jsonb"), nullable

    if _is_json(core) or get_origin(core) in (list, tuple):
        return "jsonb", nullable

    return None


# ....................... #


def _literal(value: Any) -> str | None:
    """*value* as a SQL literal, or ``None`` when it is not a boolean, number or string."""

    if isinstance(value, Enum):
        value = value.value

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, int | float | Decimal):
        text = str(value)

        return text if is_constant_default(text) else None

    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"

    return None


def _default(info: FieldInfo, column: str) -> str | None:
    """The ``DEFAULT`` for a field stored in a *column*-typed column: its literal default, or
    an empty collection for an empty ``list``, ``tuple`` or ``dict``. Anything else, a call to
    ``uuid7`` or ``utcnow`` say, has no literal and renders no default."""

    factory = info.default_factory
    value: Any = PydanticUndefined

    if factory in (list, tuple, dict):
        value = factory()  # pyright: ignore[reportCallIssue, reportUnknownVariableType]

    elif info.default is not PydanticUndefined:
        value = info.default

    if isinstance(value, list | tuple | dict) and not value:
        if column.endswith("[]"):
            return "'{}'"

        if column == "jsonb":
            return "'{}'" if isinstance(value, dict) else "'[]'"

        return None

    if value is PydanticUndefined or value is None:
        return None

    if column == "jsonb":
        return _json_literal(value)

    return _literal(value)


def _json_literal(value: Any) -> str | None:
    """*value* as a ``jsonb`` literal: a ``jsonb`` column parses its default as JSON, so a
    bare ``'pending'`` or ``0`` fails the ``CREATE TABLE``."""

    if isinstance(value, Enum):
        value = value.value

    if not isinstance(value, bool | int | float | str):
        return None

    try:
        return _literal(json.dumps(value, allow_nan=False))

    except ValueError:
        return None


# ....................... #


def _write_fields(spec: PostgresDocumentSchemaSpec) -> Iterator[tuple[str, FieldInfo]]:
    """Each field the validator requires on the write relation, once, in model order: the
    domain model's, then the create and update commands' own."""

    models = [spec.write_domain_model, spec.write_create_model, spec.write_update_model]
    seen = set(_BASE_NAMES)

    if spec.tenant_aware:
        seen.add(TENANT_ID_FIELD)

    for model in models:
        if model is None:
            continue

        stored = stored_field_names_for(model, include_computed=False)

        for name, info in model.model_fields.items():
            if name in stored and name not in spec.write_omit_fields and name not in seen:
                seen.add(name)
                yield name, info


def _materialized(spec: PostgresDocumentSchemaSpec) -> Iterator[tuple[str, ComputedFieldInfo]]:
    domain = spec.write_domain_model
    computed = domain.model_computed_fields if domain is not None else {}

    for name in sorted(spec.materialized - spec.write_omit_fields):
        if name in computed:
            yield name, computed[name]


def _columns(spec: PostgresDocumentSchemaSpec) -> list[OwnedColumn]:
    columns = list(_BASE_COLUMNS)

    if spec.tenant_aware:
        columns.append(OwnedColumn(name=TENANT_ID_FIELD, type="uuid", nullable=False))

    for name, info in _write_fields(spec):
        # Ciphertext is stored as base64 text, whatever the plaintext's type, and a plaintext
        # default would read back as a value that never decrypts.
        if name in spec.sealed_fields:
            _, nullable, _ = _strip(info.annotation)
            columns.append(OwnedColumn(name=name, type="text", nullable=nullable))
            continue

        typed = column_type(info.annotation, info.metadata)

        if typed is None:
            raise _refused(
                spec,
                f"field {name!r} is annotated {info.annotation!r}, which has no column type: "
                "a union of several types, a type variable, or a type outside the mapping.",
            )

        column, nullable = typed
        columns.append(
            OwnedColumn(name=name, type=column, nullable=nullable, default=_default(info, column))
        )

    for name, computed in _materialized(spec):
        typed = column_type(computed.return_type)

        if typed is None:
            raise _refused(
                spec,
                f"materialized field {name!r} returns {computed.return_type!r}, which has no "
                "column type.",
            )

        columns.append(OwnedColumn(name=name, type=typed[0], nullable=typed[1]))

    return columns


# ....................... #


def _scoped(spec: PostgresDocumentSchemaSpec, fields: Sequence[str]) -> tuple[str, ...]:
    """*fields* as quoted index keys, led by ``tenant_id`` on a tenant-aware route: every read
    and write there is scoped by tenant, so uniqueness holds per tenant. A field named for a
    keyword, such as ``order``, is quoted, as its column is."""

    if spec.tenant_aware and TENANT_ID_FIELD not in fields:
        fields = (TENANT_ID_FIELD, *fields)

    return tuple(quote_ident(field) for field in fields)


def _indexes(spec: PostgresDocumentSchemaSpec, columns: Sequence[OwnedColumn]) -> list[OwnedIndex]:
    nullable = {column.name for column in columns if column.nullable}
    indexes: list[OwnedIndex] = []

    def counts_nulls(fields: Sequence[str], skip_null: bool) -> bool:
        return not skip_null and any(field in nullable for field in fields)

    for guarantee in spec.guarantees:
        if not isinstance(guarantee, UniqueTogether):
            raise _refused(
                spec,
                f"it declares a {guarantee.kind!r} guarantee; P2c prints only uniqueness.",
            )

        if guarantee.where is not None or guarantee.holds != "always":
            raise _refused(
                spec,
                f"its uniqueness over {guarantee.fields!r} is filtered or holds at commit, which "
                "a plain unique index cannot keep.",
            )

        indexes.append(
            OwnedIndex(
                keys=_scoped(spec, guarantee.fields),
                unique=True,
                nulls_not_distinct=counts_nulls(guarantee.fields, guarantee.skip_null),
                required=True,
                comment=f"keeps the uniqueness {spec.name} declares over "
                f"({', '.join(guarantee.fields)})",
            )
        )

    for declared in spec.required_indexes:
        indexes.append(
            OwnedIndex(
                keys=_scoped(spec, declared.fields),
                unique=declared.unique,
                nulls_not_distinct=declared.unique
                and counts_nulls(declared.fields, declared.skip_null),
                # Without a unique index two writes can each insert a row; without a plain
                # one, reads are only slower.
                required=declared.unique,
                comment=f"{spec.name} relies on it over ({', '.join(declared.fields)})",
            )
        )

    target = spec.conflict_target

    if target is not None and frozenset(target) != {"id"}:
        indexes.append(
            OwnedIndex(
                keys=tuple(quote_ident(column) for column in target),
                unique=True,
                required=True,
                comment=f"the conflict target {spec.name}'s route configures",
            )
        )

    # One index per shape: a guarantee and a required index over the same keys ask for one.
    unique: dict[tuple[object, ...], OwnedIndex] = {}

    for index in indexes:
        unique.setdefault((index.keys, index.unique, index.nulls_not_distinct), index)

    return list(unique.values())


# ....................... #


def document_shape(spec: PostgresDocumentSchemaSpec) -> OwnedRelationShape:
    """The table *spec*'s write relation needs, as forze declares an owned table.

    :raises CoreException: ``configuration``, code ``postgres_document_render_refused``, for a
        spec with no write side or with history enabled, a guarantee other than an unfiltered
        uniqueness that holds after every write, or a field whose annotation the type table
        has no row for.
    """

    if spec.write_relation is None or spec.write_domain_model is None:
        raise _refused(spec, "it is bound read-only, so forze writes no table for it.")

    if spec.history_enabled:
        raise _refused(spec, "it enables history, and P2c prints no history table.")

    columns = _columns(spec)
    notes: tuple[str, ...] = ()

    if spec.bookkeeping_strategy == "database":
        notes = (_DATABASE_BOOKKEEPING,)

    return OwnedRelationShape(
        owner=f"PostgresDocumentAdapter {spec.name}",
        since=DOCUMENT_TABLES_SINCE,
        columns=tuple(columns),
        primary_key=("id",),
        indexes=tuple(_indexes(spec, columns)),
        notes=notes,
    )
