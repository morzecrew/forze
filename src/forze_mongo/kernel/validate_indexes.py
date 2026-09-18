"""Validate Mongo write-collection indexes for document ensure/upsert."""

from collections.abc import Sequence

import attrs

from forze.application.contracts.guarantees import StorageGuarantees, UniqueTogether
from forze.application.contracts.querying import collect_filter_field_roots
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

    A filtered guarantee needs a ``partialFilterExpression`` naming the fields the filter
    selects on. A ``sparse`` index does **not** serve there, however close it reads: it skips a
    document only when every indexed field is missing, still indexes an explicit null, and says
    nothing about the guarantee's condition. What the filter *means* is still not compared — an
    expression equivalence is the server's judgement, not a startup check's — so this catches a
    predicate over the wrong fields and not one that is merely wrong.
    """

    for guarantee in spec.guarantees:
        if not isinstance(guarantee, UniqueTogether):
            continue

        fields = tuple(guarantee.fields)
        wanted = frozenset(fields)
        predicate_fields = (
            collect_filter_field_roots(guarantee.where) if guarantee.where else frozenset()
        )

        for index in indexes:
            if not index.unique:
                continue

            if any(not isinstance(direction, int) for _, direction in index.keys):
                continue

            if frozenset(key for key, _ in index.keys) != wanted:
                continue

            if predicate_fields:
                if index.partial_filter is None:
                    continue

                rendered = repr(index.partial_filter)

                if any(field not in rendered for field in predicate_fields):
                    continue

            elif index.partial_filter is not None or index.sparse:
                continue

            break

        else:
            field_list = ", ".join(fields)
            keys = ", ".join(f"{field}: 1" for field in fields)
            filtered = bool(predicate_fields)
            condition = ", ".join(sorted(predicate_fields))
            options = (
                f"{{unique: true, partialFilterExpression: {{<a condition over {condition}>}}}}"
                if filtered
                else "{unique: true}"
            )

            raise exc.configuration(
                f"Document {spec.name!r} guarantees at most one document per ({field_list})"
                + (" among the documents its filter selects" if filtered else "")
                + f", and {database}.{collection} has no unique index on those fields"
                + (
                    f" with a partialFilterExpression over {condition}"
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
