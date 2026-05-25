"""Storage HTTP wire types and response mapping."""

from io import BytesIO
from urllib.parse import quote

from fastapi import UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.responses import Response

from forze.application.contracts.storage import DownloadedObject
from forze.application.handlers.storage.dto import UploadObjectRequestDTO
from forze.domain.models import BaseDTO

# ----------------------- #


class StorageObjectKeyPath(BaseDTO):
    """Path parameters for routes that address one object by storage key."""

    key: str


class StorageUploadFormBody(BaseModel):
    """Multipart form body for HTTP uploads."""

    file: UploadFile
    description: str | None = None
    prefix: str | None = None


async def map_upload_form(body: StorageUploadFormBody) -> UploadObjectRequestDTO:
    raw = await body.file.read()
    filename = body.file.filename or "unnamed"
    return UploadObjectRequestDTO(
        filename=filename,
        data=raw,
        description=body.description,
        prefix=body.prefix,
    )


def map_downloaded_object(source: DownloadedObject) -> Response:
    quoted_filename = quote(source.filename)
    buffer = BytesIO(source.data)
    return StreamingResponse(
        content=buffer,
        media_type=source.content_type,
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quoted_filename}",
        },
    )
