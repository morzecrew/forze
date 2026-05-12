"""Ports for document storage and retrieval."""

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

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..base import CountlessPage, CursorPage, Page
from ..query import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .specs import DocumentSpec

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

T = TypeVar("T", bound=BaseModel)

# ....................... #


class BaseDocumentPort(Protocol[R, D, C, U]):
    """Base port for document storage and retrieval."""

    spec: DocumentSpec[R, D, C, U]
    """Document specification."""


# ....................... #


@runtime_checkable
class DocumentQueryPort(BaseDocumentPort[R, Any, Any, Any], Protocol[R]):
    """Query operations for document aggregates.

    Result shape is encoded in the method name: ``get*`` / ``find*`` return the
    read model ``R``; ``project*`` returns ``JsonDict`` rows; ``select*`` uses an
    explicit ``return_type``; ``*_many`` is countless offset pagination;
    ``*_page`` includes a total count; ``*_cursor`` is keyset pagination;
    ``aggregate_*`` returns aggregate rows as JSON; ``select_*_aggregated``
    validates aggregate rows against ``return_type``.
    """

    def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        skip_cache: bool = False,
    ) -> Awaitable[R]:
        """Fetch a single document by primary key as the typed read model."""
        ...  # pragma: no cover

    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        skip_cache: bool = False,
    ) -> Awaitable[Sequence[R]]:
        """Fetch multiple documents by primary key as typed read models."""
        ...  # pragma: no cover

    def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: bool = False,
    ) -> Awaitable[R | None]:
        """Find a single document by filters or return ``None`` when missing."""
        ...  # pragma: no cover

    def project(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        fields: Sequence[str],
        *,
        for_update: bool = False,
    ) -> Awaitable[JsonDict | None]:
        """Find a single document by filters and project ``fields`` to a JSON mapping."""
        ...  # pragma: no cover

    def select(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        return_type: type[T],
        *,
        for_update: bool = False,
    ) -> Awaitable[T | None]:
        """Find a single document by filters and validate as ``return_type``."""
        ...  # pragma: no cover

    def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[R]]:
        """List documents (offset pagination) without a total count query."""
        ...  # pragma: no cover

    def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """List documents with field projection (no total count query)."""
        ...  # pragma: no cover

    def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """List documents validating each row as ``return_type`` (no total count)."""
        ...  # pragma: no cover

    def find_page(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[R]]:
        """List documents with offset pagination and total matching row count."""
        ...  # pragma: no cover

    def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """List documents with projection and total matching row count."""
        ...  # pragma: no cover

    def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[T]]:
        """List documents as ``return_type`` with total matching row count."""
        ...  # pragma: no cover

    def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CursorPage[R]]:
        """Keyset / cursor page of typed read models (opaque ``prev`` / ``next`` cursors)."""
        ...  # pragma: no cover

    def project_cursor(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CursorPage[JsonDict]]:
        """Keyset / cursor page with field projection."""
        ...  # pragma: no cover

    def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """Aggregate query returning JSON rows (no total count query)."""
        ...  # pragma: no cover

    def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """Aggregate query returning JSON rows and total group count."""
        ...  # pragma: no cover

    def select_many_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """Aggregate query validating each row as ``return_type`` (no total count)."""
        ...  # pragma: no cover

    def select_page_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Awaitable[Page[T]]:
        """Aggregate query with typed rows and total group count."""
        ...  # pragma: no cover

    def count(self, filters: QueryFilterExpression | None = None) -> Awaitable[int]:  # type: ignore[valid-type]
        """Count documents by filters."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DocumentCommandPort(BaseDocumentPort[R, D, C, U], Protocol[R, D, C, U]):
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
    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
    ) -> Awaitable[Sequence[R]]:
        """Apply the same partial update to all documents matching *filters* in one store operation."""
        ...  # pragma: no cover

    @overload
    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
    ) -> Awaitable[int]:
        """Apply the same partial update to all documents matching *filters* in one store operation."""
        ...  # pragma: no cover

    def update_matching(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
    ) -> Awaitable[Sequence[R] | int]:
        """Apply the same partial update to every document matching *filters* using a fast store path.

        Does not take per-row expected revisions. Revision bumps are handled in the
        database (Postgres: single ``UPDATE … WHERE``; Mongo: batched ``update_many`` on
        ``_id`` sets). Semantics may differ from :meth:`update` when domain models derive
        fields during apply.

        :param filters: Required filter expression (same shape as :meth:`DocumentQueryPort.find_many` / :meth:`DocumentQueryPort.find_page`).
        :param dto: Patch applied uniformly to each matching row.
        :param return_new: When ``True``, reload and return read models for updated rows
            (Postgres: ``RETURNING``; Mongo: ``get_many`` per chunk). When ``False``, return
            the number of rows the store reported as updated.
        """
        ...  # pragma: no cover

    # ....................... #

    @overload
    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[True] = True,
        chunk_size: int | None = ...,
    ) -> Awaitable[Sequence[R]]:
        """Match :meth:`update_many` semantics via chunked :meth:`DocumentQueryPort.project_many` + :meth:`update_many`."""
        ...  # pragma: no cover

    @overload
    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: Literal[False],
        chunk_size: int | None = ...,
    ) -> Awaitable[int]:
        """Match :meth:`update_many` semantics via chunked :meth:`DocumentQueryPort.project_many` + :meth:`update_many`."""
        ...  # pragma: no cover

    def update_matching_strict(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        dto: U,
        *,
        return_new: bool = True,
        chunk_size: int | None = None,
    ) -> Awaitable[Sequence[R] | int]:
        """Apply the same partial update to every matching document using optimistic revisions.

        Loads documents in chunks (keyset by primary key), then calls :meth:`update_many`
        so each row uses its current ``rev`` like :meth:`update`.

        :param filters: Required filter expression.
        :param dto: Patch applied uniformly to each row in a chunk.
        :param chunk_size: Maximum rows per chunk; defaults to the adapter batch size when omitted.
        :param return_new: When ``True``, return all updated read models; when ``False``, return the count updated.
        """
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
