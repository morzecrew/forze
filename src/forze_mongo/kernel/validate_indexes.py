"""Validate Mongo write-collection indexes for document ensure/upsert."""

from collections.abc import Sequence

import attrs

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


def _equalities(expression: object) -> dict[str, object] | None:
    """The equality constraints a ``partialFilterExpression`` imposes, or ``None``.

    ``None`` means "this is not a plain conjunction of equalities", which is the honest answer
    for a range, an ``$exists``, an ``$or`` or anything else: whether such an index covers the
    documents a guarantee selects is an implication between two predicates, and nothing here
    decides that. The caller refuses what it cannot prove, so ``None`` is a refusal rather than
    a shrug.

    ``{"f": v}`` and ``{"f": {"$eq": v}}`` are the same constraint and reduce to the same pair;
    a top-level ``$and`` is flattened, since it is how a filter with one field per clause is
    usually written. Everything else gives up.
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

                found.update(nested)

            continue

        if name.startswith("$"):
            return None

        if isinstance(value, dict):
            keys = list(value)  # pyright: ignore[reportUnknownArgumentType]

            if keys != ["$eq"]:
                return None

            found[name] = value["$eq"]  # pyright: ignore[reportUnknownArgumentType]

            continue

        found[name] = value

    return found


# ....................... #


def _guarantee_equalities(where: QueryFilterExpression) -> dict[str, object] | None:  # type: ignore[valid-type]
    """The same reduction for a guarantee's own filter, read off the parsed expression.

    Parsed rather than pattern-matched on the raw mapping, so the two sides of the comparison
    are reduced from the same kind of structure and a shorthand spelling on either side cannot
    make them differ.
    """

    parsed = QueryFilterExpressionParser.parse(where)
    found: dict[str, object] = {}

    # The parser wraps a filter's fields in a conjunction, so anything else at the root — an
    # `$or`, a negation — is a shape whose document set this does not decide, and the caller
    # refuses what it cannot prove.
    if not isinstance(parsed, QueryAnd):
        return None

    for node in parsed.items:
        if not isinstance(node, QueryField) or node.op != "$eq":
            return None

        found[node.name] = node.value

    return found


# ....................... #


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
            keys = ", ".join(f"{field}: 1" for field in fields)
            filtered = bool(predicate_fields)
            condition = ", ".join(sorted(predicate_fields))
            wanted_json = (
                "{"
                + ", ".join(f"{name}: {value!r}" for name, value in sorted(wanted_filter.items()))
                + "}"
                if wanted_filter
                else f"<a condition over {condition}>"
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
