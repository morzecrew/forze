from typing import Any

import attrs
from starlette.responses import Response

from forze.application.contracts.storage import DownloadedObject
from forze.application.dto import UploadObjectRequestDTO
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ..http import HttpRequestDTO
from .models import StorageObjectKeyPath, StorageUploadFormBody

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class StorageUploadFormMapper:
    """Maps multipart :class:`StorageUploadFormBody` to :class:`UploadObjectRequestDTO`."""

    async def __call__(
        self,
        dto: HttpRequestDTO[Any, Any, Any, Any, StorageUploadFormBody],
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> UploadObjectRequestDTO:
        if dto.body is None:
            raise CoreError("Body is required")

        body = dto.body
        raw = await body.file.read()
        filename = body.file.filename or "unnamed"
        return UploadObjectRequestDTO(
            filename=filename,
            data=raw,
            description=body.description,
            prefix=body.prefix,
        )


# ....................... #


@attrs.define(slots=True, frozen=True)
class StorageKeyFromPathMapper:
    """Maps :class:`StorageObjectKeyPath` to the string key expected by storage usecases."""

    async def __call__(
        self,
        dto: HttpRequestDTO[Any, StorageObjectKeyPath, Any, Any, Any],
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> str:
        if dto.path is None:
            raise CoreError("Path is required")

        return dto.path.key


# ....................... #


@attrs.define(slots=True, frozen=True)
class DownloadedObjectResponseMapper:
    """Maps :class:`DownloadedObject` to a binary :class:`~starlette.responses.Response`."""

    async def __call__(
        self,
        source: DownloadedObject,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Response:
        filename = source["filename"]
        safe = filename.replace('"', "_")
        return Response(
            content=source["data"],
            media_type=source["content_type"],
            headers={
                "Content-Disposition": f'attachment; filename="{safe}"',
            },
        )
