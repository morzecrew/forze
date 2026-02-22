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
# Filters = JsonDict  #! TODO: review?

# ....................... #


class DocumentSearchOptions(TypedDict, total=False):
    use_index: str
    use_fuzzy: bool
    overwrite_weights: Sequence[int]
    overwrite_fuzzy_max: float


# ....................... #


@runtime_checkable
class DocumentReadPort[R: ReadDocument](Protocol):
    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> JsonDict: ...

    @overload
    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> R: ...

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> R | JsonDict: ...

    # ....................... #

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Sequence[str],
    ) -> Sequence[JsonDict]: ...

    @overload
    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: None = ...,
    ) -> Sequence[R]: ...

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[R] | Sequence[JsonDict]: ...

    # ....................... #

    @overload
    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = ...,
        return_fields: Sequence[str],
    ) -> Optional[JsonDict]: ...

    @overload
    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = ...,
        return_fields: None = ...,
    ) -> Optional[R]: ...

    async def find(
        self,
        filters: JsonDict,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[R | JsonDict]: ...

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
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def find_many(
        self,
        filters: Optional[JsonDict] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[DocumentSorts] = ...,
        *,
        return_fields: None = ...,
    ) -> tuple[list[R], int]: ...

    async def find_many(
        self,
        filters: Optional[JsonDict] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[DocumentSorts] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[R] | list[JsonDict], int]: ...

    # ....................... #

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
    ) -> tuple[list[JsonDict], int]: ...

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
    ) -> tuple[list[R], int]: ...

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
    ) -> tuple[list[R] | list[JsonDict], int]: ...


# ....................... #


@runtime_checkable
class DocumentWritePort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](Protocol):
    async def create(self, dto: C) -> R: ...
    async def create_many(self, dtos: Sequence[C]) -> Sequence[R]: ...
    async def update(self, pk: UUID, dto: U, *, rev: Optional[int] = None) -> R: ...
    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[U],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]: ...
    async def touch(self, pk: UUID) -> R: ...
    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[R]: ...
    async def kill(self, pk: UUID) -> None: ...
    async def kill_many(self, pks: Sequence[UUID]) -> None: ...
    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> R: ...
    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]: ...
    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> R: ...
    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[R]: ...


# ....................... #


@runtime_checkable
class DocumentPort[
    R: ReadDocument,
    D: Document,
    C: CreateDocumentCmd,
    U: BaseDTO,
](DocumentReadPort[R], DocumentWritePort[R, D, C, U]): ...


# ....................... #


@runtime_checkable
class DocumentCachePort(Protocol):  # pragma: no cover
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
