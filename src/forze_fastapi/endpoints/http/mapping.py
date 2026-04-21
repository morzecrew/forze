from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperPort
from forze.application.dto import DocumentUpdateRes
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO, ReadDocument

from .contracts import HttpRequestDTO

# ----------------------- #

ReqDTO = HttpRequestDTO[Any, Any, Any, Any, Any]

# ....................... #


@attrs.define(slots=True, frozen=True)
class EmptyMapper(MapperPort[ReqDTO, BaseDTO]):
    """Mapper that maps the request to an empty DTO."""

    # ....................... #

    async def __call__(
        self,
        dto: ReqDTO,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> BaseDTO:
        return BaseDTO()


# ....................... #


@attrs.define(slots=True, frozen=True)
class QueryAsIsMapper[Out: BaseModel](MapperPort[ReqDTO, Out]):
    """Mapper that maps the query parameters to the output model."""

    out: type[Out]
    """The output model type."""

    # ....................... #

    async def __call__(
        self,
        dto: ReqDTO,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Out:
        if dto.query is None:
            raise CoreError("Query is required")

        dump = pydantic_dump(dto.query, exclude={"unset": True})
        result = pydantic_validate(self.out, dump)

        return result


# ....................... #


@attrs.define(slots=True, frozen=True)
class BodyAsIsMapper[Out: BaseModel](MapperPort[ReqDTO, Out]):
    """Mapper that maps the body to the output model."""

    out: type[Out]
    """The output model type."""

    # ....................... #

    async def __call__(
        self,
        dto: ReqDTO,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Out:
        if dto.body is None:
            raise CoreError("Body is required")

        dump = pydantic_dump(dto.body, exclude={"unset": True})
        result = pydantic_validate(self.out, dump)

        return result


# ....................... #


@attrs.define(slots=True, frozen=True)
class QueryAsIsBodyAssignMapper[Out: BaseModel](MapperPort[ReqDTO, Out]):
    """Mapper that maps the query parameters and body to the output model."""

    out: type[Out]
    """The output model type."""

    body_key: str
    """The key to assign the body to in the output model."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.body_key not in self.out.model_fields:
            raise CoreError(f"Body key {self.body_key} not found in output model")

    # ....................... #

    async def __call__(
        self,
        dto: ReqDTO,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Out:
        if dto.query is None or dto.body is None:
            raise CoreError("Query and body are required")

        dump = pydantic_dump(dto.query, exclude={"unset": True})
        body = pydantic_dump(dto.body, exclude={"unset": True})
        result = pydantic_validate(self.out, {**dump, self.body_key: body})

        return result


# ....................... #


@attrs.define(slots=True, frozen=True)
class DocumentUpdateResDataMapper[Out: ReadDocument](MapperPort[DocumentUpdateRes[Out], Out]):
    """Maps :class:`~forze.application.dto.DocumentUpdateRes` to the read model for HTTP."""

    async def __call__(
        self,
        source: DocumentUpdateRes[Out],
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Out:
        return source.data
