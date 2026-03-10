"""Base Mongo gateway with shared collection access, query rendering, and document mapping."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from functools import cached_property
from typing import Any, Optional, Sequence
from uuid import UUID

import attrs
from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.query import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
)
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD

from ..platform import MongoClient
from ..query import MongoQueryRenderer

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoGateway[M: BaseModel]:
    """Base gateway providing collection access, query rendering, and document mapping.

    Subclasses (e.g. :class:`MongoReadGateway`, :class:`MongoWriteGateway`)
    inherit shared helpers for translating between domain models and Mongo
    storage documents.  All documents are stored with ``_id`` equal to the
    domain :data:`~forze.domain.constants.ID_FIELD` as a string.
    """

    source: str
    """Mongo collection name."""

    client: MongoClient
    """Shared :class:`MongoClient` instance."""

    model: type[M]
    """Pydantic model used for deserialization."""

    db_name: Optional[str] = None
    """Override database name; ``None`` uses the client default."""

    renderer: MongoQueryRenderer = attrs.field(factory=MongoQueryRenderer)
    """Query expression renderer."""

    # ....................... #

    @cached_property
    def read_fields(self) -> set[str]:
        """Field names exposed by the model, cached for repeated access."""

        return pydantic_field_names(self.model)

    # ....................... #

    def coll(self) -> AsyncCollection[JsonDict]:
        """Return the async Mongo collection handle for this gateway's source."""

        return self.client.collection(self.source, db_name=self.db_name)

    # ....................... #

    def _storage_pk(self, pk: UUID) -> str:
        """Convert a domain primary key to its Mongo string representation."""

        return str(pk)

    # ....................... #

    def _storage_doc(self, data: JsonDict) -> JsonDict:
        """Map a domain dict to a Mongo document with ``_id`` set."""

        out = dict(data)
        out[ID_FIELD] = str(out[ID_FIELD])
        out["_id"] = out[ID_FIELD]
        return out

    # ....................... #

    def _from_storage_doc(self, raw: JsonDict) -> JsonDict:
        """Map a Mongo document back to a domain dict, restoring the ID field."""

        out = dict(raw)
        storage_id = out.pop("_id", None)

        if ID_FIELD not in out and storage_id is not None:
            out[ID_FIELD] = storage_id

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return out

    # ....................... #

    def _coerce_query_value(self, value: Any) -> Any:
        """Recursively coerce domain values (e.g. UUIDs) to Mongo-safe types."""

        if isinstance(value, UUID):
            return str(value)

        if isinstance(value, list):
            return [
                self._coerce_query_value(x)
                for x in value  # pyright: ignore[reportUnknownVariableType]
            ]

        if isinstance(value, dict):
            return {
                k: self._coerce_query_value(v)
                for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]
            }

        return value

    # ....................... #

    def _render_filters(self, filters: Optional[QueryFilterExpression]) -> JsonDict:  # type: ignore[valid-type]
        """Parse and render a filter expression into a Mongo query dict."""

        if not filters:
            return {}

        parsed = QueryFilterExpressionParser.parse(filters)
        rendered = self.renderer.render(parsed)

        return self._coerce_query_value(rendered)

    # ....................... #

    def _sorts(self, sorts: Optional[QuerySortExpression]) -> list[tuple[str, int]]:
        """Convert a sort expression to Mongo ``(field, direction)`` pairs.

        Defaults to descending by ID when no sorts are provided.
        """

        if not sorts:
            sorts = {ID_FIELD: "desc"}

        out: list[tuple[str, int]] = []

        for field, direction in sorts.items():
            target = "_id" if field == ID_FIELD else field
            out.append((target, 1 if direction == "asc" else -1))

        return out

    # ....................... #

    def _projection(self, return_fields: Optional[Sequence[str]]) -> Optional[JsonDict]:
        """Build a Mongo projection dict, excluding ``_id``."""

        if return_fields is None:
            return None

        return {**{field: 1 for field in return_fields}, "_id": 0}

    # ....................... #

    def _return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        """Extract only the requested fields from a document dict."""

        return {k: raw.get(k, None) for k in return_fields}
