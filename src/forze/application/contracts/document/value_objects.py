"""Value objects and shared types for document contracts.

The keyed-write value objects: ``ensure``/``upsert`` insert at a caller-chosen primary key,
and ``update`` patches a known key at an expected revision, so the id is an explicit part of
each item. For bulk variants these value objects bundle the id with its payload(s) and revision
instead of relying on positional tuples (clearer than ``(id, create, update)`` or
``(id, rev, dto)`` triples and extensible — e.g. an import payload can carry its own
``created_at``/``last_update_at``). Also holds the read-side row-lock mode vocabulary.
"""

from collections.abc import Awaitable, Sequence
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO

from ..querying import QueryFilterExpression

if TYPE_CHECKING:
    from .specs import DocumentSpec

# ----------------------- #

RowLockMode = Literal[False, True, "nowait", "skip_locked"]
"""Row lock mode for pessimistic reads.

* ``False`` — no lock.
* ``True`` — lock when the backend supports it (Postgres: ``FOR UPDATE``).
* ``"nowait"`` / ``"skip_locked"`` — Postgres ``FOR UPDATE NOWAIT`` / ``SKIP LOCKED``;
  other backends degrade to ``True`` with a debug log.
"""


def row_lock_requires_transaction(mode: RowLockMode) -> bool:
    """Return whether *mode* implies a transactional read on non-Postgres backends."""

    return mode is not False


# ....................... #

_BATCH_NOT_FOUND = "Some records not found"


@attrs.define(slots=True, frozen=True)
class OwnedBy:
    """Ownership predicate for a read by primary key: the row's ``field`` must equal ``value``.

    A row owned by someone else is **not found** — the same error, raised by the same read, as a
    row that does not exist — so a handler cannot serve a foreign row by forgetting a check.
    ``value`` is a UUID (a principal id) and ``field`` a UUID field of the read model.
    """

    field: str
    """Name of the read-model field holding the owner."""

    value: UUID
    """The owner the row must belong to."""

    # ....................... #

    def filter(self, pk: UUID) -> QueryFilterExpression:  # type: ignore[valid-type]
        """The query filter selecting *pk* only when it belongs to :attr:`value`."""

        return {"$values": {ID_FIELD: pk, self.field: self.value}}

    # ....................... #

    def check(self, spec: "DocumentSpec[Any, Any, Any, Any]") -> None:
        """Refuse a :attr:`field` that one of the two read paths could not evaluate.

        An owned read puts the owner in the database predicate, or checks a row read through
        the cache. A field only one of them can use — missing from the read model, sealed with
        randomized encryption, or declared but not stored — would make the answer depend on
        whether the row was cached, so it is refused before either runs. Refused rather than
        answered "not owned": a misspelled field would otherwise turn every read into a
        not-found, which looks exactly like correct enforcement.
        """

        if self.field not in spec.read.model_fields:
            raise exc.configuration(
                f"owned_by names {self.field!r}, which {spec.read.__name__} does not have.",
                code="owned_by_unknown_field",
            )

        sealed = spec.encryption.encrypted if spec.encryption else frozenset()

        if self.field in sealed or self.field in spec.lenient_read_fields:
            raise exc.configuration(
                f"owned_by names {self.field!r}, which {str(spec.name)!r} cannot filter on "
                "(randomized-encrypted or not stored).",
                code="owned_by_unfilterable_field",
            )

    # ....................... #

    def owns(self, row: BaseModel) -> bool:
        """Whether *row* (a read model) belongs to :attr:`value`; call :meth:`check` first."""

        return bool(getattr(row, self.field, None) == self.value)

    # ....................... #

    async def read_batch[T: BaseModel](self, read: Awaitable[Sequence[T]]) -> Sequence[T]:
        """Await a batch read by pk and fail it whole if any id is missing or foreign.

        The one summary names no id: a batch that listed only its missing ids would tell the
        caller that every other id it sent — a foreign one included — exists.
        """

        try:
            rows = await read

        except CoreException as error:
            if error.kind is not ExceptionKind.NOT_FOUND:
                raise

            raise exc.not_found(_BATCH_NOT_FOUND) from error

        if not all(self.owns(row) for row in rows):
            raise exc.not_found(_BATCH_NOT_FOUND)

        return rows


# ....................... #


@attrs.define(slots=True, frozen=True)
class KeyedCreate[C: BaseDTO]:
    """A create payload paired with the primary key to insert it at."""

    id: UUID
    """Primary key to insert the payload at."""

    payload: C
    """Create payload (domain fields only)."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class UpsertItem[C: BaseDTO, U: BaseDTO]:
    """A create payload + update payload for one primary key, for bulk upsert."""

    id: UUID
    """Primary key to upsert at."""

    create: C
    """Payload inserted when the key is absent."""

    update: U
    """Patch applied when the key already exists."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class KeyedUpdate[U: BaseDTO]:
    """A patch + its expected revision for one primary key, for bulk update."""

    id: UUID
    """Primary key of the document to update."""

    rev: int
    """Expected revision (optimistic-concurrency token), as in :meth:`update`."""

    dto: U
    """Patch applied to the document."""
