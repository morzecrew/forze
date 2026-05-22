from io import BytesIO
from typing import Any
from urllib.parse import quote

from fastapi.responses import StreamingResponse
from starlette.responses import Response

from forze.application.contracts.storage import DownloadedObject
from forze.application.handlers.storage.dto import UploadObjectRequestDTO
from forze.base.errors import CoreError

from ..http import HttpRequestDTO
from .models import StorageObjectKeyPath, StorageUploadFormBody

# ----------------------- #


class StorageUploadFormMapper:
    """Maps multipart :class:`StorageUploadFormBody` to :class:`UploadObjectRequestDTO`."""

    async def __call__(
        self,
        source: HttpRequestDTO[Any, Any, Any, Any, StorageUploadFormBody],
    ) -> UploadObjectRequestDTO:
        if source.body is None:
            raise CoreError("Body is required")

        body = source.body
        raw = await body.file.read()
        filename = body.file.filename or "unnamed"

        return UploadObjectRequestDTO(
            filename=filename,
            data=raw,
            description=body.description,
            prefix=body.prefix,
        )


# ....................... #


class StorageKeyFromPathMapper:
    """Maps :class:`StorageObjectKeyPath` to the string key expected by storage usecases."""

    async def __call__(
        self,
        source: HttpRequestDTO[Any, StorageObjectKeyPath, Any, Any, Any],
    ) -> str:
        if source.path is None:
            raise CoreError("Path is required")

        return source.path.key


# ....................... #


class DownloadedObjectResponseMapper:
    """Maps :class:`DownloadedObject` to a binary :class:`~starlette.responses.Response`."""

    async def __call__(self, source: DownloadedObject) -> Response:
        filename = source.filename
        quoted_filename = quote(filename)

        buffer = BytesIO(source.data)

        return StreamingResponse(
            content=buffer,
            media_type=source.content_type,
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quoted_filename}",
            },
        )
