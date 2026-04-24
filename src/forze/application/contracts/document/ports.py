"""Ports for document storage and retrieval.

**Cursor pagination (``find_many_with_cursor``):** Production adapters need a
defined total order (sort keys + disambiguator, usually primary key) and
encoding rules for opaque cursors. Typical additions: optional
``cursor_sort_keys`` / ``cursor_tiebreak`` on :class:`.DocumentSpec` or
per-relation config; read gateways then emit ``WHERE (k1, k2) > (:v1, :v2)``
for forward pages. SQL search adapters need the same relative to the ranked
``ORDER BY``. Until configured, RDBMS / Mongo adapters may raise
``NotImplementedError``; the in-memory mock implements index-based cursors for
tests.
"""

from typing import (
    Any,
    Awaitable,
    Literal,
    Protocol,
    Sequence,
    TypeVar,
    overload,
    runtime_checkable,
)
from uuid import UUID

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..base import CountlessPage, CursorPage, Page
from ..query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


class BaseDocumentPort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](Protocol):
    """Base port for document storage and retrieval."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""


# ....................... #


@runtime_checkable
class DocumentQueryPort[R: ReadDocument](BaseDocumentPort[R, Any, Any, Any], Protocol):
    """Query operations for document aggregates."""

    @overload
    def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[JsonDict]:
        """Fetch a document and return selected fields as a JSON mapping."""
        ...  # pragma: no cover

    @overload
    def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Awaitable[R]:
        """Fetch a document and return the typed read model."""
        ...  # pragma: no cover

    def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[R | JsonDict]:
        """Fetch a single document by primary key.

        :param pk: Document identifier.
        :param for_update: When ``True``, lock the row for update when possible.
        :param return_fields: Optional subset of fields to project.
        :returns: Either the typed read model or a JSON mapping.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Awaitable[Sequence[JsonDict]]:
        """Fetch multiple documents and project selected fields as JSON."""
        ...  # pragma: no cover

    @overload
    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Awaitable[Sequence[R]]:
        """Fetch multiple documents and return typed read models."""
        ...  # pragma: no cover

    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[Sequence[R] | Sequence[JsonDict]]:
        """Fetch multiple documents by primary key."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[JsonDict | None]:
        """Find a single document by filters and project selected fields."""
        ...  # pragma: no cover

    @overload
    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Awaitable[R | None]:
        """Find a single document by filters and return the typed read model."""
        ...  # pragma: no cover

    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[R | JsonDict | None]:
        """Find a single document by filters or return ``None`` when missing."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
        return_count: Literal[False] = False,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """Find many documents and project selected fields as JSON (no count query)."""
        ...  # pragma: no cover

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
        return_count: Literal[False] = False,
    ) -> Awaitable[CountlessPage[R]]:
        """Find many documents and return typed read models (no count query)."""
        ...  # pragma: no cover

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
        return_count: Literal[True],
    ) -> Awaitable[Page[JsonDict]]:
        """Find many documents, project as JSON, and return the total count."""
        ...  # pragma: no cover

    @overload
    def find_many(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
        return_count: Literal[True],
    ) -> Awaitable[Page[R]]:
        """Find many documents and return typed read models and total count."""
        ...  # pragma: no cover

    def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
        return_count: bool = False,
    ) -> Awaitable[
        Page[R] | CountlessPage[R] | Page[JsonDict] | CountlessPage[JsonDict]
    ]:
        """Find many documents, optionally paginated and sorted.

        When ``return_count`` is ``True``, runs a count query and returns
        ``(results, total)``. Otherwise returns only ``results`` (default).
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: Sequence[str],
    ) -> Awaitable[CursorPage[JsonDict]]:
        """Keyset / cursor page with field projection (opaque ``prev`` / ``next`` cursors)."""
        ...  # pragma: no cover

    @overload
    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = ...,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = ...,
        sorts: QuerySortExpression | None = ...,
        *,
        return_fields: None = ...,
    ) -> Awaitable[CursorPage[R]]:
        """Keyset / cursor page with typed read models.

        **Adapter note:** Opaque cursors require a stable sort order and encoded
        key columns; production backends need spec/config for cursor keys
        (see port module docstring on this file for outline).
        """
        ...  # pragma: no cover

    def find_many_with_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        return_fields: Sequence[str] | None = None,
    ) -> Awaitable[CursorPage[R] | CursorPage[JsonDict]]: ...  # pragma: no cover

    # ....................... #

    def count(self, filters: QueryFilterExpression | None = None) -> Awaitable[int]:  # type: ignore[valid-type]
        """Count documents by filters."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DocumentCommandPort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](BaseDocumentPort[R, D, C, U], Protocol):
    """Command operations for document aggregates."""

    @overload
    def create(self, dto: C, *, return_new: Literal[True] = True) -> Awaitable[R]:
        """Create a new document from the given command DTO."""
        ...  # pragma: no cover

    @overload
    def create(self, dto: C, *, return_new: Literal[False]) -> Awaitable[None]:
        """Create a new document from the given command DTO."""
        ...  # pragma: no cover

    def create(self, dto: C, *, return_new: bool = True) -> Awaitable[R | None]:
        """Create a new document from the given command DTO."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    @overload
    def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    def create_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def ensure(self, dto: C, *, return_new: Literal[True] = True) -> Awaitable[R]:
        """Insert when missing; if a row with the same id exists, return it unchanged."""
        ...  # pragma: no cover

    @overload
    def ensure(self, dto: C, *, return_new: Literal[False]) -> Awaitable[None]:
        """Insert when missing; no read when ``return_new`` is false."""
        ...  # pragma: no cover

    def ensure(self, dto: C, *, return_new: bool = True) -> Awaitable[R | None]:
        """Insert when missing; if a row with the same primary key exists, return it unchanged.

        Requires :attr:`~CreateDocumentCmd.id` to be set on ``dto`` so the
        operation is idempotent by primary key (insert-only; no updates to
        existing rows).
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bulk insert-when-missing; existing primary keys are left unchanged."""
        ...  # pragma: no cover

    @overload
    def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bulk insert-when-missing without re-reads."""
        ...  # pragma: no cover

    def ensure_many(
        self,
        dtos: Sequence[C],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bulk insert-when-missing; existing primary keys are left unchanged.

        Requires each DTO to set :attr:`~CreateDocumentCmd.id` and ids must be
        unique within ``dtos``. Order of the returned read models matches ``dtos``.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Insert from ``create_dto`` or, if a row with that id exists, apply ``update_dto``."""
        ...  # pragma: no cover

    @overload
    def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Insert or update without a follow-up read when ``return_new`` is false."""
        ...  # pragma: no cover

    def upsert(
        self,
        create_dto: C,
        update_dto: U,
        *,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Insert when missing; on primary-key conflict, apply ``update_dto`` like :meth:`update`.

        Requires :attr:`~CreateDocumentCmd.id` on ``create_dto``. The update branch
        uses the current stored revision (same optimistic rules as :meth:`update`).
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bulk upsert: each pair is ``(create_cmd, update_cmd)`` for the same id."""
        ...  # pragma: no cover

    @overload
    def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bulk upsert without re-reads when ``return_new`` is false."""
        ...  # pragma: no cover

    def upsert_many(
        self,
        pairs: Sequence[tuple[C, U]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bulk insert-or-update. Create commands must set ``id`` and ids must be unique.

        Result order matches ``pairs``.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Awaitable[R]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Awaitable[tuple[R, JsonDict]]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> Awaitable[None]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    @overload
    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Awaitable[JsonDict]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    def update(
        self,
        pk: UUID,
        rev: int,
        dto: U,
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Awaitable[R | JsonDict | None | tuple[R, JsonDict]]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[False] = False,
    ) -> Awaitable[Sequence[R]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[True] = True,
        return_diff: Literal[True],
    ) -> Awaitable[Sequence[tuple[R, JsonDict]]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[False] = False,
    ) -> Awaitable[None]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    @overload
    def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: Literal[False],
        return_diff: Literal[True],
    ) -> Awaitable[Sequence[JsonDict]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    def update_many(
        self,
        updates: Sequence[tuple[UUID, int, U]],
        *,
        return_new: bool = True,
        return_diff: bool = False,
    ) -> Awaitable[
        Sequence[R] | Sequence[JsonDict] | Sequence[tuple[R, JsonDict]] | None
    ]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def touch(self, pk: UUID, *, return_new: Literal[True] = True) -> Awaitable[R]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    @overload
    def touch(self, pk: UUID, *, return_new: Literal[False]) -> Awaitable[None]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    def touch(self, pk: UUID, *, return_new: bool = True) -> Awaitable[R | None]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    @overload
    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    def touch_many(
        self,
        pks: Sequence[UUID],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    def kill(self, pk: UUID) -> Awaitable[None]:
        """Hard-delete a single document without soft-delete semantics."""
        ...  # pragma: no cover

    def kill_many(self, pks: Sequence[UUID]) -> Awaitable[None]:
        """Hard-delete multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Soft-delete a document if the model supports it."""
        ...  # pragma: no cover

    @overload
    def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Soft-delete a document if the model supports it."""
        ...  # pragma: no cover

    def delete(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Soft-delete a document if the model supports it."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Soft-delete multiple documents."""
        ...  # pragma: no cover

    @overload
    def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Soft-delete multiple documents."""
        ...  # pragma: no cover

    def delete_many(
        self,
        deletes: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Soft-delete multiple documents."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[R]:
        """Restore a previously soft-deleted document."""
        ...  # pragma: no cover

    @overload
    def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Restore a previously soft-deleted document."""
        ...  # pragma: no cover

    def restore(
        self,
        pk: UUID,
        rev: int,
        *,
        return_new: bool = True,
    ) -> Awaitable[R | None]:
        """Restore a previously soft-deleted document."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Restore multiple previously soft-deleted documents."""
        ...  # pragma: no cover

    @overload
    def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: Literal[False],
    ) -> Awaitable[None]:
        """Restore multiple previously soft-deleted documents."""
        ...  # pragma: no cover

    def restore_many(
        self,
        restores: Sequence[tuple[UUID, int]],
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | None]:
        """Restore multiple previously soft-deleted documents."""
        ...  # pragma: no cover
