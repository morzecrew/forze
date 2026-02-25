"""Ports for document storage, retrieval and search."""

from typing import (
    Any,
    Literal,
    Optional,
    Protocol,
    Sequence,
    TypedDict,
    overload,
    runtime_checkable,
)
from uuid import UUID

from forze.base.primitives import JsonDict
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #

DocumentSorts = dict[str, Literal["asc", "desc"]]
"""Sort specification for document queries keyed by field name."""

# Filters = JsonDict  #! TODO: review?

# ....................... #


class DocumentSearchOptions(TypedDict, total=False):
    """Optional tuning parameters for search backends."""

    use_index: str
    """Explicit index identifier to use when backends support multiple indices."""

    use_fuzzy: bool
    """Whether to enable fuzzy matching for the search query."""

    overwrite_weights: Sequence[int]
    """Override default field weights when scoring search results."""

    overwrite_fuzzy_max: float
    """Upper bound for fuzziness distance when fuzzy search is enabled."""


# ....................... #


@runtime_checkable
class DocumentReadPort[R: ReadDocument](Protocol):
    """Read-only operations for document aggregates."""

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict:
        """Fetch a document and return selected fields as a JSON mapping."""

        ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R:
        """Fetch a document and return the typed read model."""

        ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> R | JsonDict:
        """Fetch a single document by primary key.

        :param pk: Document identifier.
        :param for_update: When ``True``, lock the row for update when possible.
        :param return_fields: Optional subset of fields to project.
        :returns: Either the typed read model or a JSON mapping.
        """
        ...

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Sequence[JsonDict]:
        """Fetch multiple documents and project selected fields as JSON."""

        ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Sequence[R]:
        """Fetch multiple documents and return typed read models."""

        ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[R] | Sequence[JsonDict]:
        """Fetch multiple documents by primary key."""
        ...

    # ....................... #

    @overload
    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]:
        """Find a single document by filters and project selected fields."""

        ...

    @overload
    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]:
        """Find a single document by filters and return the typed read model."""

        ...

    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[R | JsonDict]:
        """Find a single document by filters or return ``None`` when missing."""
        ...

    # ....................... #

    @overload
    async def find_many(
        self,
        filters: Optional[JsonDict] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[DocumentSorts] = ...,
        *,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]:
        """Find many documents and project selected fields as JSON."""

        ...

    @overload
    async def find_many(
        self,
        filters: Optional[JsonDict] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[DocumentSorts] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]:
        """Find many documents and return typed read models."""

        ...

    async def find_many(
        self,
        filters: Optional[JsonDict] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[DocumentSorts] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        """Find many documents, optionally paginated and sorted.

        :returns: A tuple of result list and total count.
        """
        ...


# ....................... #


@runtime_checkable
class DocumentSearchPort[R: ReadDocument](Protocol):
    """Full-text or secondary index search over documents."""

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[JsonDict] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[DocumentSorts] = ...,
        *,
        options: Optional[DocumentSearchOptions] = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]:
        """Search documents and project selected fields as JSON."""

        ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[JsonDict] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[DocumentSorts] = ...,
        *,
        options: Optional[DocumentSearchOptions] = ...,
        return_fields: None = ...,
    ) -> tuple[list[R], int]:
        """Search documents and return typed read models."""

        ...

    async def search(
        self,
        query: str,
        filters: Optional[JsonDict] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[DocumentSorts] = None,
        options: Optional[DocumentSearchOptions] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]:
        """Search documents using a query string and optional filters.

        :param query: Query expression interpreted by the backend.
        :param filters: Structured filters applied before scoring.
        :param limit: Maximum number of hits to return.
        :param offset: Offset into the result set.
        :param sorts: Field-level sort specification.
        :param options: Backend-specific tuning options.
        :param return_fields: Optional projection of fields.
        :returns: A tuple of hits and total hit count.
        """
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

    async def create(self, dto: C) -> R:
        """Create a new document from the given command DTO."""
        ...

    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]:
        """Create multiple documents in a batch."""
        ...

    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R:
        """Apply a partial update to a document identified by ``pk``."""
        ...

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Apply partial updates to multiple documents."""
        ...

    async def touch(self, pk: UUID) -> R:
        """Bump metadata (e.g. ``last_update_at``) for a single document."""
        ...

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]:
        """Bump metadata for multiple documents."""
        ...

    async def kill(self, pk: UUID) -> None:
        """Hard-delete a single document without soft-delete semantics."""
        ...

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        """Hard-delete multiple documents."""
        ...

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        """Soft-delete a document if the model supports it."""
        ...

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Soft-delete multiple documents."""
        ...

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R:
        """Restore a previously soft-deleted document."""
        ...

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]:
        """Restore multiple previously soft-deleted documents."""
        ...


# ....................... #


@runtime_checkable
class DocumentPort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](DocumentReadPort[R], DocumentSearchPort[R], DocumentWritePort[R, D, C, U], Protocol):
    """Combined port exposing read, search and write operations for documents."""


# ....................... #


@runtime_checkable
class DocumentCachePort(Protocol):  # pragma: no cover
    """Cache abstraction for document read models."""

    async def get(self, pk: UUID) -> Optional[Any]: ...
    async def set(self, pk: UUID, rev: int, value: Any) -> None: ...
    async def delete(self, pk: UUID, *, hard: bool) -> None: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
    ) -> tuple[dict[UUID, Any], list[UUID]]: ...
    async def set_many(
        self,
        mapping: dict[tuple[UUID, int], Any],
    ) -> None: ...
    async def delete_many(self, pks: Sequence[UUID], *, hard: bool) -> None: ...
