"""Validate Mongo write-collection indexes for document ensure/upsert."""

from typing import Sequence

import attrs

from forze.application.contracts.resolution import (
    RelationSpec,
    is_static_relation,
    require_static_relation,
)

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


# ....................... #


def _format_index_keys(keys: tuple[tuple[str, int], ...]) -> str:
    inner = ", ".join(f"{k}: {d}" for k, d in keys)
    return f"{{{inner}}}"


def _is_id_unique_index(index: MongoIndexInfo) -> bool:
    return index.unique and index.keys == (("_id", 1),)


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
