"""Ports for document storage and retrieval"""

from typing import (
    Awaitable,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    overload,
    runtime_checkable,
)
from uuid import UUID

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..query import QueryFilterExpression, QuerySortExpression

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=CreateDocumentCmd)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@runtime_checkable
class DocumentReadPort[R](Protocol):
    """Read-only operations for document aggregates."""

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
        return_fields: Optional[Sequence[str]] = None,
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
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[Sequence[R] | Sequence[JsonDict]]:
        """Fetch multiple documents by primary key."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Awaitable[Optional[JsonDict]]:
        """Find a single document by filters and project selected fields."""
        ...  # pragma: no cover

    @overload
    def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Awaitable[Optional[R]]:
        """Find a single document by filters and return the typed read model."""
        ...  # pragma: no cover

    def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[Optional[R | JsonDict]]:
        """Find a single document by filters or return ``None`` when missing."""
        ...  # pragma: no cover

    # ....................... #

    @overload
    def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: Sequence[str],
    ) -> Awaitable[tuple[list[JsonDict], int]]:
        """Find many documents and project selected fields as JSON."""
        ...  # pragma: no cover

    @overload
    def find_many(
        self,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        return_fields: None = ...,
    ) -> Awaitable[tuple[list[R], int]]:
        """Find many documents and return typed read models."""
        ...  # pragma: no cover

    def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[tuple[list[R] | list[JsonDict], int]]:
        """Find many documents, optionally paginated and sorted.

        :returns: A tuple of result list and total count.
        """
        ...  # pragma: no cover

    # ....................... #

    def count(self, filters: Optional[QueryFilterExpression] = None) -> Awaitable[int]:
        """Count documents by filters."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DocumentWritePort[R, D, C, U](Protocol):
    """Write operations for document aggregates."""

    def create(self, dto: C) -> Awaitable[R]:
        """Create a new document from the given command DTO."""
        ...  # pragma: no cover

    def create_many(self, dtos: Sequence[C]) -> Awaitable[Sequence[R]]:
        """Create multiple documents in a batch."""
        ...  # pragma: no cover

    def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Apply a partial update to a document identified by ``pk``."""
        ...  # pragma: no cover

    def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Apply partial updates to multiple documents."""
        ...  # pragma: no cover

    def touch(self, pk: UUID) -> Awaitable[R]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...  # pragma: no cover

    def touch_many(self, pks: Sequence[UUID]) -> Awaitable[Sequence[R]]:
        """Bump metadata for multiple documents."""
        ...  # pragma: no cover

    def kill(self, pk: UUID) -> Awaitable[None]:
        """Hard-delete a single document without soft-delete semantics."""
        ...  # pragma: no cover

    def kill_many(self, pks: Sequence[UUID]) -> Awaitable[None]:
        """Hard-delete multiple documents."""
        ...  # pragma: no cover

    def delete(self, pk: UUID, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Soft-delete a document if the model supports it."""
        ...  # pragma: no cover

    def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Soft-delete multiple documents."""
        ...  # pragma: no cover

    def restore(self, pk: UUID, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Restore a previously soft-deleted document."""
        ...  # pragma: no cover

    def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Restore multiple previously soft-deleted documents."""
        ...  # pragma: no cover
