from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.application.contracts.execution import Handler
from forze.application.contracts.mapping import Mapper
from forze.application.dto.paginated import (
    CursorPaginated,
    Paginated,
    ProjectedCursorPaginated,
    ProjectedPaginated,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd

from .dto import (
    AggregatedListRequestDTO,
    CursorListRequestDTO,
    DocumentIdDTO,
    DocumentUpdateDTO,
    DocumentUpdateRes,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)

# ----------------------- #

Bm = BaseModel
Bd = BaseDTO
Cd = CreateDocumentCmd
Du = DocumentUpdateDTO
Dur = DocumentUpdateRes
Did = DocumentIdDTO

Lr = ListRequestDTO
Plr = ProjectedListRequestDTO
Clr = CursorListRequestDTO
Pclr = ProjectedCursorListRequestDTO
Alr = AggregatedListRequestDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreateDocument[In: Bd, Cmd: Cd, Out: Bm](Handler[In, Out]):
    """Handler that creates a new document from a mapped command."""

    doc: DocumentCommandPort[Out, Any, Cmd, Any]
    """Document port for create operations."""

    mapper: Mapper[In, Cmd]
    """Mapper that converts input DTO to create command."""

    # ....................... #

    async def __call__(self, args: In) -> Out:
        """Create a document from the mapped command.

        :param args: Input DTO (e.g. request payload).
        :returns: Created read model.
        """

        cmd = await self.mapper(args)

        return await self.doc.create(dto=cmd)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateDocument[In: Bd, Cmd: Bd, Out: Bm](Handler[Du[In], Dur[Out]]):
    """Usecase that updates an existing document from a mapped command and returns a result with diff."""

    doc: DocumentCommandPort[Out, Any, Any, Cmd]
    """Document port for update operations."""

    mapper: Mapper[In, Cmd]
    """Mapper that converts input DTO to update command."""

    # ....................... #

    async def __call__(self, args: Du[In]) -> Dur[Out]:
        """Update a document from the mapped command and return a result with diff.

        :param args: Update arguments (pk, dto, rev).
        :returns: Updated read model and diff.
        """

        cmd = await self.mapper(args.dto)

        res, diff = await self.doc.update(
            pk=args.id,
            rev=args.rev,
            dto=cmd,
            return_diff=True,
        )

        return DocumentUpdateRes(data=res, diff=diff)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class KillDocument(Handler[Did, None]):
    """Handler that permanently deletes a document (hard delete)."""

    doc: DocumentCommandPort[Any, Any, Any, Any]
    """Document port for kill operations."""

    # ....................... #

    async def __call__(self, args: Did) -> None:
        """Permanently delete a document.

        :param args: Document primary key.
        :returns: ``None``.
        """

        return await self.doc.kill(pk=args.id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GetDocument[R: Bm](Handler[Did, R]):
    """Handler that fetches a single document by primary key.

    Delegates to :meth:`DocumentReadPort.get`. Read-only; uses the lighter
    :class:`DocumentReadPort`.
    """

    doc: DocumentQueryPort[R]
    """Document port for get operations."""

    # ....................... #

    async def __call__(self, args: Did) -> R:
        """Fetch a document by primary key.

        :param args: Document primary key.
        :returns: Read model.
        """

        return await self.doc.get(pk=args.id)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ListDocuments[Out: Bm](Handler[Lr, Paginated[Out]]):
    """Handler that fetches multiple documents by filters and sorts."""

    doc: DocumentQueryPort[Out]
    """Document port for list operations."""

    mapper: Mapper[Lr, Lr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Lr) -> Paginated[Out]:
        """Fetch multiple documents by filters and sorts.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of read models.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.find_page(
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return Paginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ProjectedListDocuments(Handler[Plr, ProjectedPaginated]):
    """Handler that fetches multiple documents by filters and sorts with raw results."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[Plr, Plr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Plr) -> ProjectedPaginated:
        """Fetch multiple documents by filters and sorts with raw results.

        :param args: List arguments (body, page, size).
        :returns: Paginated list of raw results.
        """

        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.project_page(
            tuple(body.return_fields),
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return ProjectedPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CursorListDocuments[Out: Bm](Handler[Clr, CursorPaginated[Out]]):
    """Usecase that lists documents with cursor (keyset) pagination."""

    doc: DocumentQueryPort[Out]
    """Document port for list operations."""

    mapper: Mapper[Clr, Clr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Clr) -> CursorPaginated[Out]:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.find_cursor(
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
        )

        return CursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ProjectedCursorListDocuments(Handler[Pclr, ProjectedCursorPaginated]):
    """Usecase that lists documents with raw projection and cursor pagination."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[Pclr, Pclr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Pclr) -> ProjectedCursorPaginated:
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.project_cursor(
            tuple(body.return_fields),
            filters=body.filters,
            cursor=body.to_cursor_expression(),
            sorts=body.sorts,
        )

        return ProjectedCursorPaginated.from_page(res)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AggregatedListDocuments(Handler[Alr, ProjectedPaginated]):
    """Usecase that fetches multiple documents by filters and sorts with aggregates."""

    doc: DocumentQueryPort[Any]
    """Document port for list operations."""

    mapper: Mapper[Alr, Alr] | None = attrs.field(default=None)
    """Optional mapper to transform incoming request DTO"""

    # ....................... #

    async def __call__(self, args: Alr) -> ProjectedPaginated:
        page = args.page
        size = args.size
        limit = size
        offset = (page - 1) * limit
        body = args

        if self.mapper:
            body = await self.mapper(body)

        res = await self.doc.aggregate_page(
            body.aggregates,
            filters=body.filters,
            sorts=body.sorts,
            pagination={
                "limit": limit,
                "offset": offset,
            },
        )

        return ProjectedPaginated.from_page(res)
