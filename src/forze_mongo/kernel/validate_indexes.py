"""Validate Mongo write-collection indexes for document ensure/upsert."""

import json
import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from math import isinf, isnan
from uuid import UUID

import attrs
from bson import Decimal128

from forze.application.contracts.guarantees import StorageGuarantees, UniqueTogether
from forze.application.contracts.querying import (
    QueryFilterExpression,
    collect_filter_field_roots,
)
from forze.application.contracts.querying.internal.nodes import QueryAnd, QueryField
from forze.application.contracts.querying.internal.parse import (
    QueryFilterExpressionParser,
)
from forze.application.contracts.resolution import (
    RelationSpec,
    is_static_relation,
    require_static_relation,
)
from forze.base.exceptions import exc

from ._logger import logger
from .introspect import MongoIndexInfo, MongoIntrospector

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDocumentIndexSpec:
    """Index validation input for one writable document route."""

    name: str
    """Document route name (for log messages)."""

    write_relation: RelationSpec
    """Write collection ``(database, collection)``."""

    guarantees: StorageGuarantees = ()
    """What the spec requires the store to enforce (see :attr:`DocumentSpec.guarantees`).

    Reconciliation at wiring said Mongo can keep these; this says whether the deployment
    created the index."""


# ....................... #


def _format_index_keys(keys: tuple[tuple[str, int | str], ...]) -> str:
    inner = ", ".join(f"{k}: {d}" for k, d in keys)
    return f"{{{inner}}}"


def _is_id_unique_index(index: MongoIndexInfo) -> bool:
    return index.unique and index.keys == (("_id", 1),)


# ....................... #


_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
"""A field name ``mongosh`` takes as a bare object key; anything else is quoted."""

_EXACT_IN_A_DOUBLE = 2**53
"""Above this an integer is not exactly representable as a double, so a bare numeric literal in
``mongosh`` would round it to a different value."""


def _bson_key(value: object) -> object:
    """A comparison key that tells BSON types apart the way the server does.

    Python compares ``True == 1`` and Mongo does not: a boolean and a number are different BSON
    types, so an index filtered ``{is_current: 1}`` indexes none of the documents a guarantee
    filtered ``{is_current: true}`` selects. Comparing the raw values reads those two filters as
    the same one, which is the false acceptance this exists to stop.

    Numeric *widths* go the other way — Mongo compares an int, a long and a double by value — so
    those share a tag and are left to Python's own numeric comparison, which is exact. Casting
    them to ``float`` to make them comparable would undo the point: ``9007199254740993`` and
    ``9007199254740992.0`` are different values that one double cannot tell apart.
    """

    if isinstance(value, bool):
        return ("bool", value)

    # What this adapter *stores*, not what BSON could hold: a UUID is written as its canonical
    # string and a Decimal as a Decimal128 (see the write gateway's storage mapping), so an
    # index filter read back from the server carries the stored spelling while a guarantee
    # carries the domain one. Keying them apart would refuse a correct index.
    if isinstance(value, UUID):
        return ("str", str(value))

    if isinstance(value, Decimal128):
        return ("number", value.to_decimal())

    # Both spellings of an instant reduce to the one the server hands back: BSON has no
    # date-only type and no naive datetime, the client reads with ``tz_aware=True``, so a date
    # is stored as UTC midnight and a naive datetime as UTC. Keying a `date` apart from the
    # `datetime` it becomes would refuse the very index the printed migration creates.
    if isinstance(value, datetime):
        return ("datetime", value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC))

    if isinstance(value, date):
        return ("datetime", datetime(value.year, value.month, value.day, tzinfo=UTC))

    if isinstance(value, int | float | Decimal):
        return ("number", value)

    if value is None:
        return ("null", None)

    if isinstance(value, list | tuple):
        return ("array", tuple(_bson_key(item) for item in value))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

    return (type(value).__name__, value)


# ....................... #


def _mongosh_field(name: str) -> str:
    """Render a field name as a ``mongosh`` object key.

    Bare where it can be, because that is how the statement is written by hand and an operator
    reads it more easily, and quoted where it must be: a field named ``effective-date`` is legal
    in Mongo and not a legal JavaScript identifier, so printing it bare hands over a statement
    that does not parse.
    """

    return name if _IDENTIFIER.fullmatch(name) else json.dumps(name)


# ....................... #


def _mongosh(key: object) -> str:
    """Render a reduced constraint key as the literal ``mongosh`` would take.

    Rendered from the *reduced* key rather than the raw value, so the migration the refusal
    prints is written in the same terms the comparison uses — a message that says one thing and
    a check that wants another is worse than no message. ``repr`` is not usable here: it spells
    a boolean ``True``, which ``mongosh`` does not accept.
    """

    match key:
        case ("bool", value):
            return "true" if value else "false"

        case ("number", int() as value):
            # `mongosh` reads a bare number as a double, which silently rounds anything a
            # double cannot hold — and the index the operator then creates filters on a
            # different value than the one printed, so validation refuses it again.
            return str(value) if abs(value) < _EXACT_IN_A_DOUBLE else f'Long("{value}")'

        case ("number", Decimal() as value):
            # Stored as a Decimal128, so the statement has to create one; a bare literal would
            # store a double and read back as a different value.
            return f'Decimal128("{value}")'

        case ("number", float() as value):
            if isnan(value):
                return "NaN"

            if isinf(value):
                return "Infinity" if value > 0 else "-Infinity"

            return str(int(value)) if value.is_integer() else str(value)

        case ("null", _):
            return "null"

        case ("str", value):
            return json.dumps(value)

        case ("array", tuple() as items):
            return "[" + ", ".join(_mongosh(item) for item in items) + "]"  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

        case ("datetime", datetime() as value):
            # The instant as it will be stored, so the statement creates the value the
            # comparison then reads back — a quoted string would create an index over strings.
            return f'ISODate("{value.isoformat()}")'

        case _:  # pragma: no cover - every scalar a filter admits is handled above
            return json.dumps(str(key))


# ....................... #


def _merge(found: dict[str, object], name: str, key: object) -> bool:
    """Record the reduced constraint *key* for *name*, refusing a second, different one.

    A filter can name one field twice — ``{"$and": [{"f": false}, {"f": true}]}`` is a legal
    ``partialFilterExpression`` — and it then selects **no** documents at all, so the index
    enforces nothing. Letting the later constraint overwrite the earlier one reduces that filter
    to a plausible-looking single equality, which is how an index that indexes nothing comes to
    satisfy a guarantee.

    :returns: ``False`` when the field already carries a different value, which every caller
        turns into a refusal.
    """

    if name in found and found[name] != key:
        return False

    found[name] = key

    return True


# ....................... #


def _equalities(expression: object) -> dict[str, object] | None:
    """The equality constraints a ``partialFilterExpression`` imposes, or ``None``.

    ``None`` means "this is not a plain conjunction of equalities", which is the honest answer
    for a range, an ``$exists``, an ``$or`` or anything else: whether such an index covers the
    documents a guarantee selects is an implication between two predicates, and nothing here
    decides that. The caller refuses what it cannot prove, so ``None`` is a refusal rather than
    a shrug — and a filter naming one field twice with different values is refused the same way,
    since it selects nothing.

    ``{"f": v}`` and ``{"f": {"$eq": v}}`` are the same constraint and reduce to the same pair;
    ``$and`` is flattened at any depth, since it is how a filter with one field per clause is
    usually written. Values are reduced to :func:`_bson_key`, not kept raw.
    """

    if not isinstance(expression, dict):
        return None

    found: dict[str, object] = {}

    for key, value in expression.items():  # pyright: ignore[reportUnknownVariableType]
        name = str(key)

        if name == "$and":
            if not isinstance(value, list | tuple):
                return None

            for branch in value:  # pyright: ignore[reportUnknownVariableType]
                nested = _equalities(branch)

                if nested is None:
                    return None

                for nested_name, nested_key in nested.items():
                    if not _merge(found, nested_name, nested_key):
                        return None

            continue

        if name.startswith("$"):
            return None

        if isinstance(value, dict):
            keys = list(value)  # pyright: ignore[reportUnknownArgumentType]

            if keys != ["$eq"]:
                return None

            if not _merge(found, name, _bson_key(value["$eq"])):  # pyright: ignore[reportUnknownArgumentType]
                return None

            continue

        if not _merge(found, name, _bson_key(value)):
            return None

    return found


# ....................... #


def _guarantee_equalities(where: QueryFilterExpression) -> dict[str, object] | None:  # type: ignore[valid-type]
    """The same reduction for a guarantee's own filter, read off the parsed expression.

    Parsed rather than pattern-matched on the raw mapping, so the two sides of the comparison
    are reduced from the same kind of structure and a shorthand spelling on either side cannot
    make them differ.

    Conjunctions nest — an explicit ``$and`` wraps one ``QueryAnd`` per branch — so they are
    flattened rather than read one level deep, which would refuse a filter the author is
    entitled to write. A null test reduces to an equality against ``None``: the parser spells it
    ``$null`` and Mongo spells it ``{f: null}``, and they select the same documents. The
    negative form does not reduce, since a ``partialFilterExpression`` cannot say "not null".
    """

    parsed = QueryFilterExpressionParser.parse(where)
    found: dict[str, object] = {}

    def collect(node: object) -> bool:
        match node:
            case QueryAnd(items):
                return all(collect(item) for item in items)

            case QueryField(name, op, value) if op == "$eq":
                return _merge(found, name, _bson_key(value))

            case QueryField(name, op, value) if op == "$null" and value is True:
                return _merge(found, name, _bson_key(None))

            case _:
                return False

    # Anything else at the root — an `$or`, a negation, a range — is a shape whose document set
    # this does not decide, and the caller refuses what it cannot prove.
    if not collect(parsed):
        return None

    return found


# ....................... #


def _require_guarantee_indexes(
    spec: MongoDocumentIndexSpec,
    indexes: Sequence[MongoIndexInfo],
    *,
    database: str,
    collection: str,
) -> None:
    """Refuse a declared guarantee with no index behind it, naming the index that would serve.

    The Mongo half of the same rule Postgres follows: declared, validated, never created. An
    adapter that built the index would hold a write lock on a collection nobody asked it to
    touch, and would hide the missing migration.

    Fields are matched as a *set*: a guarantee is a property of a tuple of values, and
    ``{tenant_id: 1, email: 1}`` keeps it exactly as ``{email: 1, tenant_id: 1}`` does. Only
    ordinary ascending/descending keys count — a ``text`` or ``hashed`` index is a different
    structure over a different value.

    A filtered guarantee needs a ``partialFilterExpression`` selecting the same documents. The
    index's filter and the guarantee's are each reduced to their equality constraints and
    compared, so an index over the right field and the wrong value — ``{is_current: false}``
    for a guarantee about current documents — is refused rather than counted.

    Anything that does not reduce that way is refused too, and deliberately: whether an index
    restricted by a range, an ``$exists`` or an ``$or`` covers the documents a guarantee selects
    is an implication between two predicates, which nothing here decides. A refusal names the
    condition the index needs, and a migration written from that message satisfies it. Note that
    an index covering *more* documents is refused as well, for the reason a plain index is: the
    extra documents are ones the guarantee meant to leave alone, and constraining them breaks
    the shape the spec declared.

    Postgres is checked more loosely, and the asymmetry is in the data rather than the effort:
    ``pg_get_expr`` hands back deparsed SQL, so comparing values there means parsing a
    dialect, while Mongo stores the filter as a document that can simply be read.

    A ``sparse`` index does **not** serve as a filter, however close it reads: it skips a
    document only when every indexed field is missing, still indexes an explicit null, and says
    nothing about the guarantee's condition.
    """

    for guarantee in spec.guarantees:
        if not isinstance(guarantee, UniqueTogether):
            continue

        fields = tuple(guarantee.fields)
        wanted = frozenset(fields)
        predicate_fields = (
            collect_filter_field_roots(guarantee.where) if guarantee.where else frozenset()
        )
        wanted_filter = (
            _guarantee_equalities(guarantee.where) if guarantee.where is not None else None
        )

        for index in indexes:
            if not index.unique:
                continue

            if any(not isinstance(direction, int) for _, direction in index.keys):
                continue

            if frozenset(key for key, _ in index.keys) != wanted:
                continue

            if guarantee.where is not None:
                if index.partial_filter is None:
                    continue

                if wanted_filter is None or _equalities(index.partial_filter) != wanted_filter:
                    continue

            elif index.partial_filter is not None or index.sparse:
                continue

            break

        else:
            field_list = ", ".join(fields)
            keys = ", ".join(f"{_mongosh_field(field)}: 1" for field in fields)
            # What the *validation* asks for, not what the filter happens to mention: a filter
            # naming no field at all still needs a partialFilterExpression, and a message that
            # suggested a plain index for one would send an operator to a migration that leaves
            # startup failing.
            filtered = guarantee.where is not None
            condition = ", ".join(sorted(predicate_fields)) or "its filter"
            wanted_json = (
                "{"
                + ", ".join(
                    f"{_mongosh_field(name)}: {_mongosh(key)}"
                    for name, key in sorted(wanted_filter.items())
                )
                + "}"
                if wanted_filter
                else f"<a condition selecting the same documents as {condition}>"
            )
            options = (
                f"{{unique: true, partialFilterExpression: {wanted_json}}}"
                if filtered
                else "{unique: true}"
            )

            raise exc.configuration(
                f"Document {spec.name!r} guarantees at most one document per ({field_list})"
                + (" among the documents its filter selects" if filtered else "")
                + f", and {database}.{collection} has no unique index on those fields"
                + (
                    f" whose partialFilterExpression selects the same documents as {condition}"
                    if filtered
                    else " covering every document"
                )
                + ". The migration is what satisfies a guarantee — nothing here creates one. "
                f"This would:\n"
                f"  db.{collection}.createIndex({{{keys}}}, {options})",
                details={
                    "document": spec.name,
                    "collection": f"{database}.{collection}",
                    "fields": list(fields),
                },
            )


# ....................... #


async def validate_mongo_document_indexes(
    introspector: MongoIntrospector,
    specs: Sequence[MongoDocumentIndexSpec],
) -> None:
    """List indexes on write collections and warn about secondary unique indexes.

    ``ensure`` / ``upsert`` are idempotent by document ``id`` (stored as ``_id``).
    Secondary unique indexes are allowed; inserts with a new ``id`` that violate
    them raise duplicate-key conflicts.
    """

    for spec in specs:
        if not is_static_relation(spec.write_relation):
            logger.trace(
                "Mongo index validation for document %r: skipping dynamic write relation.",
                spec.name,
            )
            continue

        database, collection = require_static_relation(
            spec.write_relation,
            route_name=spec.name,
            field="write",
            integration="Mongo",
            omit_hint="Omit mongo_document_index_validation_lifecycle_step for this route.",
        )
        indexes = await introspector.list_indexes(
            database=database,
            collection=collection,
        )

        _require_guarantee_indexes(
            spec,
            indexes,
            database=database,
            collection=collection,
        )

        has_id_unique = any(_is_id_unique_index(idx) for idx in indexes)

        if not has_id_unique:
            logger.trace(
                "Mongo index validation for document %r (%s.%s): "
                "no explicit unique index on _id (MongoDB always indexes _id).",
                spec.name,
                database,
                collection,
            )

        for idx in indexes:
            if idx.name == "_id_":
                continue

            if not idx.unique:
                continue

            if _is_id_unique_index(idx):
                continue

            logger.warning(
                "Mongo index validation for document %r write collection %s.%s: "
                "secondary unique index %r on %s — ensure/upsert are PK-only; "
                "new ids that collide on this index will raise duplicate-key conflicts.",
                spec.name,
                database,
                collection,
                idx.name,
                _format_index_keys(idx.keys),
            )
