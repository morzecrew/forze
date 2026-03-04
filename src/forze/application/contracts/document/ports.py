"""Ports for document storage and retrieval"""

from typing import Awaitable, Optional, Protocol, Sequence, overload, runtime_checkable
from uuid import UUID

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from ..query import QueryFilterExpression, QuerySortExpression

# ----------------------- #


@runtime_checkable
class DocumentReadPort[R: ReadDocument](Protocol):
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

        ...

    @overload
    def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Awaitable[R]:
        """Fetch a document and return the typed read model."""

        ...

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
        ...

    # ....................... #

    @overload
    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Awaitable[Sequence[JsonDict]]:
        """Fetch multiple documents and project selected fields as JSON."""

        ...

    @overload
    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Awaitable[Sequence[R]]:
        """Fetch multiple documents and return typed read models."""

        ...

    def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[Sequence[R] | Sequence[JsonDict]]:
        """Fetch multiple documents by primary key."""
        ...

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

        ...

    @overload
    def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Awaitable[Optional[R]]:
        """Find a single document by filters and return the typed read model."""

        ...

    def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Awaitable[Optional[R | JsonDict]]:
        """Find a single document by filters or return ``None`` when missing."""
        ...

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

        ...

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

        ...

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
        ...

    # ....................... #

    def count(self, filters: Optional[QueryFilterExpression] = None) -> Awaitable[int]:
        """Count documents by filters."""
        ...


# ....................... #


@runtime_checkable
class DocumentWritePort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](Protocol):
    """Write operations for document aggregates."""

    def create(self, dto: C) -> Awaitable[R]:
        """Create a new document from the given command DTO."""
        ...

    def create_many(self, dtos: Sequence[C]) -> Awaitable[Sequence[R]]:
        """Create multiple documents in a batch."""
        ...

    def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Apply a partial update to a document identified by ``pk``."""
        ...

    def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Apply partial updates to multiple documents."""
        ...

    def touch(self, pk: UUID) -> Awaitable[R]:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...

    def touch_many(self, pks: Sequence[UUID]) -> Awaitable[Sequence[R]]:
        """Bump metadata for multiple documents."""
        ...

    def kill(self, pk: UUID) -> Awaitable[None]:
        """Hard-delete a single document without soft-delete semantics."""
        ...

    def kill_many(self, pks: Sequence[UUID]) -> Awaitable[None]:
        """Hard-delete multiple documents."""
        ...

    def delete(self, pk: UUID, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Soft-delete a document if the model supports it."""
        ...

    def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Soft-delete multiple documents."""
        ...

    def restore(self, pk: UUID, *, rev: Optional[int] = None) -> Awaitable[R]:
        """Restore a previously soft-deleted document."""
        ...

    def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Awaitable[Sequence[R]]:
        """Restore multiple previously soft-deleted documents."""
        ...


# ....................... #


@runtime_checkable
class DocumentPort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](DocumentReadPort[R], DocumentWritePort[R, D, C, U], Protocol):
    """Combined port exposing read, search, and write operations for documents."""
