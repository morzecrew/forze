"""Mongo collection index introspection with in-memory caching."""

from __future__ import annotations

from typing import cast, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ..client import MongoClientPort
from .types import MongoIndexInfo

# ----------------------- #

MongoIndexCache = dict[tuple[str, str], tuple[MongoIndexInfo, ...]]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class MongoIntrospector:
    """List and cache index metadata for Mongo write collections."""

    client: MongoClientPort
    """Mongo client used for catalog queries."""

    __index_cache: MongoIndexCache = attrs.field(factory=dict, init=False)

    # ....................... #

    def invalidate_collection(self, *, database: str, collection: str) -> None:
        """Evict cached index metadata for one collection."""

        self.__index_cache.pop((database, collection), None)

    def clear(self) -> None:
        """Clear all cached index metadata."""

        self.__index_cache.clear()

    # ....................... #

    async def list_indexes(
        self,
        *,
        database: str,
        collection: str,
    ) -> tuple[MongoIndexInfo, ...]:
        """Return index metadata for a collection, using the cache when available."""

        key = (database, collection)

        if key in self.__index_cache:
            return self.__index_cache[key]

        raw = await self.client.list_indexes(
            database=database,
            collection=collection,
        )
        parsed = tuple(_parse_index_info(doc) for doc in raw)
        self.__index_cache[key] = parsed

        return parsed


# ....................... #


def _parse_index_info(doc: dict[str, object]) -> MongoIndexInfo:
    name = str(doc.get("name") or "")

    if not name:
        raise exc.internal("Mongo index document missing name.")

    key_doc = doc.get("key")

    if not isinstance(key_doc, dict):
        raise exc.internal(f"Mongo index {name!r} has invalid key document.")

    key_doc = cast(JsonDict, key_doc)

    # Direction is ``1``/``-1`` for btree indexes but a string for special
    # index types (``"text"``, ``"2dsphere"``, ``"2d"``, ``"hashed"``,
    # ``"vector"``); ``int(v)`` would crash on those, so keep non-int verbatim.
    keys = tuple(
        (str(k), v if isinstance(v, int) else str(v)) for k, v in key_doc.items()
    )
    unique = bool(doc.get("unique", False))

    return MongoIndexInfo(name=name, keys=keys, unique=unique)
